// Copyright 2016 FullStory, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package solrmonitor

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/fullstorydev/zk"
)

const (
	collectionsPath  = "/collections"
	liveNodesPath    = "/live_nodes"
	rolesPath        = "/roles.json"
	clusterPropsPath = "/clusterprops.json"
	SysColPrefix     = ".sys."
)

// Keeps an in-memory copy of the current state of the Solr cluster; automatically updates on ZK changes.
type SolrMonitor struct {
	logger         zk.Logger // where to debug log
	zkCli          ZkCli     // the ZK client
	solrRoot       string    // e.g. "/solr"
	ignorePrefixes []string  // ignore collections with these prefixes, for example system collection prefix .sys.
	ignoreSuffixes []string  // ignore collections with these suffixes
	zkWatcher      *ZkWatcherMan

	mu                sync.RWMutex
	collections       map[string]*collection // map of all currently-known collections
	liveNodes         []string               // current set of live_nodes
	overseerNodes     []string               // current set of overseer nodes (from roles.json)
	clusterProps      map[string]string      // current set of cluster props (from clusterprops.json)
	solrEventListener SolrEventListener      // to listen the solr cluster state
	pathsToWatch      map[string]struct{}    // set of paths to always watch
}

// Minimal interface solrmonitor needs (allows for mock ZK implementations).
type ZkCli interface {
	ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	Get(path string) ([]byte, *zk.Stat, error)
	GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error)
	ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	State() zk.State
	Close()
}

// Create a new solrmonitor.  Solrmonitor takes ownership of the provided zkCli and zkWatcher-- they
// will be closed when Solrmonitor is closed, and should not be used by any other caller.
// The provided zkWatcher must already be wired to the associated zkCli to receive all global events.
func NewSolrMonitor(zkCli ZkCli, ignoreSuffixes []string, zkWatcher *ZkWatcherMan) (*SolrMonitor, error) {
	return NewSolrMonitorWithLogger(zkCli, zkWatcher, zk.DefaultLogger, ignoreSuffixes, nil)
}

// Create a new solrmonitor.  Solrmonitor takes ownership of the provided zkCli and zkWatcher-- they
// will be closed when Solrmonitor is closed, and should not be used by any other caller.
// The provided zkWatcher must already be wired to the associated zkCli to receive all global events.
func NewSolrMonitorWithLogger(zkCli ZkCli, zkWatcher *ZkWatcherMan, logger zk.Logger, ignoreSuffixes []string, solrEventListener SolrEventListener) (*SolrMonitor, error) {
	//ignore system collections by default
	return NewSolrMonitorWithRoot(zkCli, zkWatcher, logger, "/solr", []string{SysColPrefix}, ignoreSuffixes, solrEventListener)
}

// Create a new solrmonitor.  Solrmonitor takes ownership of the provided zkCli and zkWatcher-- they
// will be closed when Solrmonitor is closed, and should not be used by any other caller.
// The provided zkWatcher must already be wired to the associated zkCli to receive all global events.
func NewSolrMonitorWithRoot(zkCli ZkCli, zkWatcher *ZkWatcherMan, logger zk.Logger, solrRoot string, ignorePrefixes []string, ignoreSuffixes []string, solrEventListener SolrEventListener) (*SolrMonitor, error) {
	c := &SolrMonitor{
		logger:            logger,
		zkCli:             zkCli,
		solrRoot:          solrRoot,
		ignorePrefixes:    ignorePrefixes,
		ignoreSuffixes:    ignoreSuffixes,
		zkWatcher:         zkWatcher,
		collections:       make(map[string]*collection),
		solrEventListener: solrEventListener,
		pathsToWatch:      make(map[string]struct{}),
	}
	err := c.start()
	if err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

type callbacks struct {
	*SolrMonitor
}

var _ Callbacks = callbacks{}

func (c callbacks) ChildrenChanged(path string, children []string) error {
	return c.SolrMonitor.childrenChanged(path, children)
}

func (c callbacks) DataChanged(path string, data string, stat *zk.Stat) error {
	version := int32(-1)
	if stat != nil {
		version = stat.Version
	}
	return c.SolrMonitor.dataChanged(path, data, version)
}

func (c callbacks) ShouldWatchChildren(path string) bool {
	return c.SolrMonitor.shouldWatchChildren(path)
}

func (c callbacks) ShouldWatchData(path string) bool {
	// some paths, like "/clusterprops.json" we always want to watch, otherwise, see if we should watch that collection's path
	return c.SolrMonitor.shouldWatchPath(path) || c.SolrMonitor.shouldWatchCollection(path)
}

func (c *SolrMonitor) Close() {
	c.zkCli.Close()
	c.zkWatcher.Close()
}

func (c *SolrMonitor) GetCurrentState() (ClusterState, error) {
	if c.zkCli.State() != zk.StateHasSession {
		return nil, errors.New("not currently connected to zk")
	}
	result := make(ClusterState)

	c.mu.RLock()
	defer c.mu.RUnlock()
	for name := range c.collections {
		collectionState, err := c.doGetCollectionState(name)
		if err != nil {
			c.logger.Printf("collection %s: error fetching state: %s", name, err)
			continue
		}
		if collectionState != nil {
			result[name] = collectionState
		}
	}

	return result, nil
}

func (c *SolrMonitor) GetCollectionState(name string) (*CollectionState, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.doGetCollectionState(name)
}

func (c *SolrMonitor) InvalidateCollectionState(name string) error {
	if coll := c.getCollection(name); coll != nil {
		return coll.start()
	}

	return errors.New(fmt.Sprintf("collection not found %s ", name))
}

func (c *SolrMonitor) getCollection(name string) *collection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if coll, found := c.collections[name]; found {
		return coll
	}

	return nil
}

func (c *SolrMonitor) doGetCollectionState(name string) (*CollectionState, error) {
	coll := c.collections[name]

	if coll == nil {
		return nil, nil
	}
	coll.mu.RLock()
	defer coll.mu.RUnlock()
	return coll.collectionState, nil
}

func (c *SolrMonitor) GetLiveNodes() ([]string, error) {
	if c.zkCli.State() != zk.StateHasSession {
		return nil, errors.New("not currently connected to zk")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string{}, c.liveNodes...), nil
}

func (c *SolrMonitor) GetOverseerNodes() ([]string, error) {
	if c.zkCli.State() != zk.StateHasSession {
		return nil, errors.New("not currently connected to zk")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string{}, c.overseerNodes...), nil
}

func (c *SolrMonitor) GetClusterProps() (map[string]string, error) {
	if c.zkCli.State() != zk.StateHasSession {
		return nil, errors.New("not currently connected to zk")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterProps, nil
}

func (c *SolrMonitor) childrenChanged(path string, children []string) error {
	switch path {
	case c.solrRoot + collectionsPath:
		return c.updateCollections(c.filterCollections(children))
	case c.solrRoot + liveNodesPath:
		return c.updateLiveNodes(children)
	default:
		//collectionsPath + "/" + coll.name + "/state.json": we want to state.json children
		if !strings.HasPrefix(path, c.solrRoot+"/collections/") || !strings.HasSuffix(path, "/state.json") {
			return fmt.Errorf("solrmonitor: unknown childrenChanged: %s", path)
		}
		return c.updateCollection(path, children)
	}
}

func (c *SolrMonitor) filterCollections(collections []string) []string {
	if c.ignorePrefixes == nil && c.ignoreSuffixes == nil {
		return collections
	}
	var filtered []string
	for _, coll := range collections {
		if !c.shouldIgnoreCollection(coll) {
			filtered = append(filtered, coll)
		}
	}
	return filtered
}

func (c *SolrMonitor) shouldIgnoreCollection(coll string) bool {
	for _, ignorePrefix := range c.ignorePrefixes {
		if strings.HasPrefix(coll, ignorePrefix) {
			return true
		}
	}
	for _, ignoreSuffix := range c.ignoreSuffixes {
		if strings.HasSuffix(coll, ignoreSuffix) {
			return true
		}
	}
	return false
}

type zkRoleState struct {
	Overseer []string `json:"overseer"`
}

func (c *SolrMonitor) rolesChanged(data string) error {
	if len(data) == 0 {
		return nil
	}

	var roles *zkRoleState
	err := json.Unmarshal([]byte(data), &roles)
	if err != nil {
		c.logger.Printf("error when parsing JSON for roles: %s", err.Error())
		return err
	}

	err = c.updateOverseerNodes(roles.Overseer)
	if err != nil {
		c.logger.Printf("error when updating overseer nodes: %s", err.Error())
		return err
	}
	return nil
}

func (c *SolrMonitor) clusterPropsChanged(data string) error {
	if len(data) == 0 {
		return nil
	}

	var clusterProps map[string]interface{}
	err := json.Unmarshal([]byte(data), &clusterProps)
	if err != nil {
		c.logger.Printf("error when parsing JSON for cluster props: %s", err.Error())
		return err
	}

	stringOnlyClusterProps := map[string]string{}
	for key, value := range clusterProps {
		if v, ok := value.(string); ok {
			stringOnlyClusterProps[key] = v
		}
	}

	err = c.updateClusterProps(stringOnlyClusterProps)
	if err != nil {
		c.logger.Printf("error when updating cluster props: %s", err.Error())
		return err
	}

	return nil
}

func (c *SolrMonitor) updateCollection(path string, children []string) error {
	rs, err := c.updateCollectionState(path, children)

	if err == nil && rs != nil && len(rs) > 0 {
		coll := c.getCollFromPath(path)
		if coll != nil {
			c.mu.RLock()
			defer c.mu.RUnlock()
			if c.solrEventListener != nil {
				collName := c.getCollNameFromPath(path)
				c.solrEventListener.SolrCollectionReplicaStatesChanged(collName, rs)
			}
		}
	}

	return err
}

func (c *SolrMonitor) updateCollectionState(path string, children []string) (map[string]*PerReplicaState, error) {
	c.logger.Printf("updateCollectionState with PRS: path %s, children %d", path, len(children))
	coll := c.getCollFromPath(path)
	if coll == nil || len(children) == 0 {
		//looks like we have not got the collection event yet; it  should be safe to ignore it
		return nil, nil
	}

	rmap := make(map[string]*PerReplicaState)

	for _, r := range children {
		replicaParts := strings.Split(r, ":")
		if len(replicaParts) < 3 || len(replicaParts) > 4 {
			c.logger.Printf("PRS protocol is wrong %s ", r)
			panic(fmt.Sprintf("PRS protocol is in wrong format %s ", r))
		}
		version, err := strconv.ParseInt(replicaParts[1], 10, 32)
		if err != nil {
			c.logger.Printf("PRS protocol has wrong version %s ", r)
			panic(fmt.Sprintf("PRS protocol has wrong version %s ", r))
		}

		prs := &PerReplicaState{
			Name:    replicaParts[0],
			Version: int32(version),
		}

		switch replicaParts[2] {
		case "A":
			prs.State = "active"
		case "D":
			prs.State = "down"
		case "R":
			prs.State = "recovering"
		case "F":
			prs.State = "RECOVERY_FAILED"
		default:
			// marking inactive - as it should be recoverable error
			c.logger.Printf("ERROR: PRS protocol UNKNOWN state %s ", replicaParts[2])
			prs.State = "inactive"
		}

		prs.Leader = "false"
		if len(replicaParts) == 4 {
			prs.Leader = "true"
		}

		//keep ths latest prs
		if currentPrs, found := rmap[prs.Name]; found {
			if currentPrs.Version >= prs.Version {
				continue
			}
		}

		rmap[prs.Name] = prs
	}
	c.logger.Printf("updateCollectionState on collection %s: updating prs state %d", coll.name, len(rmap))
	coll.mu.Lock()
	defer coll.mu.Unlock()
	//update the collection state based on new PRS (per replica state)
	prsUpdateCount := 0
	for _, shard := range coll.collectionState.Shards {
		for rname, rstate := range shard.Replicas {
			if prs, found := rmap[rname]; found {
				prsUpdateCount++
				if prs.Version < rstate.Version {
					c.logger.Printf("WARNING: PRS update with lower version than received previously. Existing: %v, Update: %v", rstate, prs)
				}
				rstate.Version = prs.Version
				rstate.Leader = prs.Leader
				rstate.State = prs.State
			}
		}
	}

	if prsUpdateCount < len(rmap) {
		c.logger.Printf("All prs entries not updated for collection %s", coll.name)
		replicaVsShard := map[string]string{}
		for shardName, shardState := range coll.collectionState.Shards {
			for replicaName := range shardState.Replicas {
				replicaVsShard[replicaName] = shardName
			}
		}

		for replica := range rmap {
			if _, found := replicaVsShard[replica]; !found {
				c.logger.Printf("For Collection %s, prs update for replica %s, shard is missing", coll.name, replica)
			}
		}
	}

	return rmap, nil
}

func (c *SolrMonitor) shouldWatchChildren(path string) bool {
	switch path {
	case c.solrRoot + collectionsPath:
		return true
	case c.solrRoot + liveNodesPath:
		return true
	default:
		// watch coll/state.json childrens for replica status
		if strings.HasPrefix(path, c.solrRoot+"/collections/") && strings.HasSuffix(path, "/state.json") {
			coll := c.getCollFromPath(path)
			if coll != nil {
				return coll.isPRSEnabled()
			}
		}
		return false
	}
}

func (c *SolrMonitor) dataChanged(path string, data string, version int32) error {
	if strings.HasSuffix(path, rolesPath) {
		return c.rolesChanged(data)
	}

	if strings.HasSuffix(path, clusterPropsPath) {
		return c.clusterPropsChanged(data)
	}

	collectionsPrefix := c.solrRoot + "/collections/"
	if !strings.HasPrefix(path, collectionsPrefix) {
		// Expecting a collection in the /collections/ folder
		return fmt.Errorf("unknown dataChanged: %s", path)
	}

	coll := c.getCollFromPath(path)
	if coll == nil {
		// Always require a collection!
		return fmt.Errorf("unknown dataChanged: %s", path)
	} else if c.shouldIgnoreCollection(coll.name) {
		//then ignore this change, might not be necessary as such collection is already filtered out
		//at childrenChanged on the collections node, but just to play safe here...
		return nil
	}

	if strings.HasSuffix(path, "/state.json") {
		// common state.json change
		state := coll.setStateData(data, version)
		c.callSolrListener(coll.name, state)
		if coll.isPRSEnabled() {
			coll.startMonitoringReplicaStatus()
		}
	} else {
		// less common (usually just initialization) collection change
		state := coll.setCollectionData(data)
		if state != nil {
			// for collection data, we don't hit the callback until the collection state is available as well
			// on delete, the standard state.json flow will handle passing back the nil state once cleared
			c.callSolrListener(coll.name, state)
		}
	}
	return nil
}

func (c *SolrMonitor) callSolrListener(name string, state *CollectionState) {
	if c.solrEventListener != nil {
		c.mu.RLock()
		defer c.mu.RUnlock()
		c.solrEventListener.SolrCollectionStateChanged(name, state)
	}
}

func (c *SolrMonitor) shouldWatchCollection(path string) bool {
	coll := c.getCollFromPath(path)
	return coll != nil
}

func (c *SolrMonitor) shouldWatchPath(path string) bool {
	_, ok := c.pathsToWatch[path]
	return ok
}

func (c *SolrMonitor) getCollFromPath(path string) *collection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name := c.getCollNameFromPath(path)
	return c.collections[name]
}

func (c *SolrMonitor) getCollNameFromPath(path string) string {
	name := strings.TrimPrefix(path, c.solrRoot+"/collections/")
	name = strings.TrimSuffix(name, "/state.json")
	return name
}

func (c *SolrMonitor) start() error {
	// Synchronously check the initial calls, then setup event listening.

	collectionsPath := c.solrRoot + collectionsPath
	liveNodesPath := c.solrRoot + liveNodesPath
	rolesPath := c.solrRoot + rolesPath
	clusterPropsPath := c.solrRoot + clusterPropsPath
	c.pathsToWatch[clusterPropsPath] = struct{}{}
	c.zkWatcher.Start(c.zkCli, callbacks{c})

	if err := c.zkWatcher.MonitorChildren(liveNodesPath); err != nil {
		return err
	}
	if err := c.zkWatcher.MonitorChildren(collectionsPath); err != nil {
		return err
	}
	if err := c.zkWatcher.MonitorData(rolesPath); err != nil {
		return err
	}
	if err := c.zkWatcher.MonitorData(clusterPropsPath); err != nil {
		return err
	}
	return nil
}

// Update the set of active collections.
func (c *SolrMonitor) updateCollections(collections []string) error {
	var logAdded, logRemoved []string
	logOldSize, logNewSize := len(c.collections), len(collections)

	c.collectionsChanged(collections)

	var added []*collection
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		collectionExists := map[string]bool{}

		// First, add any collections that don't already exist
		for _, name := range collections {
			collectionExists[name] = true
			_, found := c.collections[name]
			if !found {
				coll := &collection{
					name:          name,
					parent:        c,
					zkNodeVersion: -1,
				}
				c.collections[name] = coll
				added = append(added, coll)
				logAdded = append(logAdded, name)
			}
		}

		// Now remove any collections that disappeared.
		for name := range c.collections {
			if !collectionExists[name] {
				delete(c.collections, name)
				logRemoved = append(logRemoved, name)
			}
		}
	}()
	// Now start any new collections.
	var errCount int32
	var wg sync.WaitGroup
	for _, c := range added {
		coll := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := coll.start(); err != nil {
				coll.parent.logger.Printf("solrmonitor: error starting coll: %s: %s", coll.name, err)
				atomic.AddInt32(&errCount, 1)
			}
		}()
	}
	wg.Wait()

	if len(logAdded) > 10 {
		logAdded = []string{fmt.Sprintf("(%d) suppressing list", len(logAdded))}
	}
	if len(logRemoved) > 10 {
		logRemoved = []string{fmt.Sprintf("(%d) suppressing list", len(logRemoved))}
	}

	c.logger.Printf("solrmonitor (%d) -> (%d) collections; added=%s, removed=%s, errors=%d",
		logOldSize, logNewSize, logAdded, logRemoved, errCount)

	if int(errCount) > logNewSize/10 {
		return fmt.Errorf("%d/%d orgs failed to start", errCount, logNewSize)
	}

	return nil
}

func (c *SolrMonitor) collectionsChanged(collections []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.solrEventListener != nil {
		c.solrEventListener.SolrCollectionsChanged(collections)
	}
}

func (c *SolrMonitor) updateLiveNodes(liveNodes []string) error {
	c.logger.Printf("%s (%d): %s", liveNodesPath, len(liveNodes), liveNodes)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.liveNodes = liveNodes
	if c.solrEventListener != nil {
		c.solrEventListener.SolrLiveNodesChanged(liveNodes)
	}
	return nil
}

func (c *SolrMonitor) updateOverseerNodes(overseerNodes []string) error {
	c.logger.Printf("%s (%d): %s", rolesPath, len(overseerNodes), overseerNodes)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.overseerNodes = overseerNodes
	return nil
}

func (c *SolrMonitor) updateClusterProps(clusterProps map[string]string) error {
	c.logger.Printf("%s (%d): %v", clusterPropsPath, len(clusterProps), clusterProps)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.clusterProps = clusterProps
	if c.solrEventListener != nil {
		c.solrEventListener.SolrClusterPropsChanged(c.clusterProps)
	}
	return nil
}

// Represents an individual collection.
type collection struct {
	mu            sync.RWMutex
	name          string       // the name of the collection
	zkNodeVersion int32        // the version of the state.json data, or -1 if no state.json node exists
	parent        *SolrMonitor // if nil, this collection object was removed from the ClusterState

	configName      string // held onto separately due to different lifecycle, copied over to collection state as needed
	collectionState *CollectionState
	isWatched       bool
}

func parseCollectionData(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}
	var state zkCollectionState
	if err := json.Unmarshal(data, &state); err != nil {
		return "", err
	}
	return state.ConfigName, nil
}

func parseStateData(name string, data []byte, version int32) (*CollectionState, error) {
	if len(data) == 0 {
		return nil, nil
	}
	// The individual per-collection state.json files are kind of weird; they are a single-element map from collection
	// name to a CollectionState object.  In other words they are a ClusterState, containing only the relevant collection.
	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	var keys []string
	for k := range state {
		keys = append(keys, k)
	}

	if len(keys) != 1 || keys[0] != name {
		err := fmt.Errorf("Expected 1 key, got %s", keys)
		return nil, err
	}

	collState := state[name]
	collState.ZkNodeVersion = version
	return collState, nil
}

func (coll *collection) start() error {
	coll.setWatch(false)
	collPath := coll.parent.solrRoot + "/collections/" + coll.name
	statePath := collPath + "/state.json"
	if err := coll.parent.zkWatcher.MonitorData(collPath); err != nil {
		return err
	}
	if err := coll.parent.zkWatcher.MonitorData(statePath); err != nil {
		return err
	}
	return nil
}

func (coll *collection) setCollectionData(data string) *CollectionState {
	coll.parent.logger.Printf("setCollectionData:updating the collection %s ", coll.name)
	if data == "" {
		coll.parent.logger.Printf("setCollectionData: no data for %s", coll.name)
	}

	coll.mu.Lock()
	defer coll.mu.Unlock()
	configName, err := parseCollectionData([]byte(data))
	if err != nil {
		coll.parent.logger.Printf("Unable to parse collection[%s] collection data. Error %s", coll.name, err)
	}

	coll.configName = configName
	if collectionState := coll.collectionState; collectionState != nil {
		// shallow copy is okay, we are just setting the top field
		copy := *collectionState
		coll.carryOverConfigName(&copy)
		coll.collectionState = &copy
	}

	return coll.collectionState
}

func (coll *collection) setStateData(data string, version int32) *CollectionState {
	coll.parent.logger.Printf("setStateData:updating the collection state.json %s ", coll.name)
	if data == "" {
		coll.parent.logger.Printf("setStateData: no data for %s", coll.name)
	}

	coll.mu.Lock()
	defer coll.mu.Unlock()
	// we need to parse data here as we need to know PRS is enable for collection ot not; if enable then keep watch on coll/state.json children
	newState, err := parseStateData(coll.name, []byte(data), coll.zkNodeVersion)
	if err != nil {
		coll.parent.logger.Printf("Unable to parse collection[%s] state data. Error %s", coll.name, err)
	}

	var oldState = coll.collectionState

	coll.updateReplicaVersionAndState(newState, oldState)
	coll.carryOverConfigName(newState)
	coll.zkNodeVersion = version
	coll.collectionState = newState

	return coll.collectionState
}

func (coll *collection) updateReplicaVersionAndState(newState *CollectionState, oldState *CollectionState) {
	if oldState == nil || newState == nil || !newState.IsPRSEnabled() {
		return
	}
	for shradName, newShardState := range newState.Shards {
		oldShardState, found := oldState.Shards[shradName]
		if found {
			for replicaName, newReplicasState := range newShardState.Replicas {
				oldReplicaState, replicaFound := oldShardState.Replicas[replicaName]
				if replicaFound {
					newReplicasState.Version = oldReplicaState.Version
					newReplicasState.State = oldReplicaState.State
					newReplicasState.Leader = oldReplicaState.Leader
				}
			}
		}
	}
}

func (coll *collection) carryOverConfigName(newState *CollectionState) {
	if newState == nil {
		return
	}
	// Config name is managed separately, carry over as needed.
	if len(coll.configName) > 0 {
		newState.ConfigName = coll.configName
	}
}

func (coll *collection) startMonitoringReplicaStatus() {
	path := coll.parent.solrRoot + "/collections/" + coll.name + "/state.json"

	// TODO: need to revisit coll.isWatched flag(if zk disconnects?). we need to create watch once only Scott?
	if !coll.hasWatch() {
		err := coll.parent.zkWatcher.MonitorChildren(path)
		if err == nil {
			coll.parent.logger.Printf("startMonitoringReplicaStatus: watching collection [%s] children for PRS", coll.name)
			coll.setWatch(true)
		}
	}
}

func (coll *collection) setWatch(watch bool) {
	coll.mu.Lock()
	defer coll.mu.Unlock()
	coll.isWatched = watch
}

func (coll *collection) hasWatch() bool {
	coll.mu.RLock()
	defer coll.mu.RUnlock()
	return coll.isWatched
}

func (coll *collection) isPRSEnabled() bool {
	coll.mu.RLock()
	defer coll.mu.RUnlock()
	return coll.collectionState != nil && coll.collectionState.IsPRSEnabled()
}

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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samuel/go-zookeeper/zk"
)

// Keeps an in-memory copy of the current state of the Solr cluster; automatically updates on ZK changes.
type SolrMonitor struct {
	logger    zk.Logger // where to debug log
	zkCli     ZkCli     // the ZK client
	solrRoot  string    // e.g. "/solr"
	zkWatcher *ZkWatcherMan

	mu          sync.RWMutex
	collections map[string]*collection // map of all currently-known collections
	liveNodes   []string               // current set of live_nodes
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
func NewSolrMonitor(zkCli ZkCli, zkWatcher *ZkWatcherMan) (*SolrMonitor, error) {
	return NewSolrMonitorWithLogger(zkCli, zkWatcher, zk.DefaultLogger)
}

// Create a new solrmonitor.  Solrmonitor takes ownership of the provided zkCli and zkWatcher-- they
// will be closed when Solrmonitor is closed, and should not be used by any other caller.
// The provided zkWatcher must already be wired to the associated zkCli to receive all global events.
func NewSolrMonitorWithLogger(zkCli ZkCli, zkWatcher *ZkWatcherMan, logger zk.Logger) (*SolrMonitor, error) {
	return NewSolrMonitorWithRoot(zkCli, zkWatcher, logger, "/solr")
}

// Create a new solrmonitor.  Solrmonitor takes ownership of the provided zkCli and zkWatcher-- they
// will be closed when Solrmonitor is closed, and should not be used by any other caller.
// The provided zkWatcher must already be wired to the associated zkCli to receive all global events.
func NewSolrMonitorWithRoot(zkCli ZkCli, zkWatcher *ZkWatcherMan, logger zk.Logger, solrRoot string) (*SolrMonitor, error) {
	c := &SolrMonitor{
		logger:      logger,
		zkCli:       zkCli,
		solrRoot:    solrRoot,
		zkWatcher:   zkWatcher,
		collections: make(map[string]*collection),
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
	return c.SolrMonitor.dataChanged(path, data, stat.Version)
}

func (c callbacks) ShouldWatchChildren(path string) bool {
	return c.SolrMonitor.shouldWatchChildren(path)
}

func (c callbacks) ShouldWatchData(path string) bool {
	return c.SolrMonitor.shouldWatchData(path)
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

func (c *SolrMonitor) doGetCollectionState(name string) (*CollectionState, error) {
	coll := c.collections[name]
	if coll == nil {
		return nil, nil
	}
	return coll.GetData()
}

func (c *SolrMonitor) GetLiveNodes() ([]string, error) {
	if c.zkCli.State() != zk.StateHasSession {
		return nil, errors.New("not currently connected to zk")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return append([]string{}, c.liveNodes...), nil
}

func (c *SolrMonitor) childrenChanged(path string, children []string) error {
	switch path {
	case c.solrRoot + "/collections":
		return c.updateCollections(children)
	case c.solrRoot + "/live_nodes":
		return c.updateLiveNodes(children)
	default:
		return fmt.Errorf("solrmonitor: unknown childrenChanged: %s", path)
	}
}

func (c *SolrMonitor) shouldWatchChildren(path string) bool {
	switch path {
	case c.solrRoot + "/collections":
		return true
	case c.solrRoot + "/live_nodes":
		return true
	default:
		return false
	}
}

func (c *SolrMonitor) dataChanged(path string, data string, version int32) error {
	if !strings.HasPrefix(path, c.solrRoot+"/collections/") || !strings.HasSuffix(path, "/state.json") {
		return fmt.Errorf("unknown dataChanged: %s", path)
	}

	coll := c.getCollFromPath(path)
	if coll != nil {
		coll.setData(data, version)
	}
	return nil
}

func (c *SolrMonitor) shouldWatchData(path string) bool {
	coll := c.getCollFromPath(path)
	return coll != nil
}

func (c *SolrMonitor) getCollFromPath(path string) *collection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	name := strings.TrimPrefix(path, c.solrRoot+"/collections/")
	name = strings.TrimSuffix(name, "/state.json")
	return c.collections[name]
}

func (c *SolrMonitor) start() error {
	// Synchronously check the initial calls, then setup event listening.

	// Ensure no state format v1 collections exist.
	globalClusterStatePath := c.solrRoot + "/clusterstate.json"
	globalClusterState, _, err := c.zkCli.Get(globalClusterStatePath)
	if err != nil {
		c.logger.Printf("%s: error fetching zk node: %s", globalClusterStatePath, err)
		return err
	}
	if len(globalClusterState) > 2 {
		err := fmt.Errorf("%s: solrmonitor does not support state format v1; zk node should contain only '{}'", globalClusterStatePath)
		c.logger.Printf("please use Solr's MIGRATESTATEFORMAT collections command: %s", err)
	}

	collectionsPath := c.solrRoot + "/collections"
	liveNodesPath := c.solrRoot + "/live_nodes"
	c.zkWatcher.Start(c.zkCli, callbacks{c})
	if err := c.zkWatcher.MonitorChildren(collectionsPath); err != nil {
		return err
	}
	if err := c.zkWatcher.MonitorChildren(liveNodesPath); err != nil {
		return err
	}
	return nil
}

// Update the set of active collections.
func (c *SolrMonitor) updateCollections(collections []string) error {
	var logAdded, logRemoved []string
	logOldSize, logNewSize := len(c.collections), len(collections)

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
				c.parent.logger.Printf("solrmonitor: error starting coll: %s: %s", coll.name, err)
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

func (c *SolrMonitor) updateLiveNodes(liveNodes []string) error {
	c.logger.Printf("live_nodes (%d): %s", len(liveNodes), liveNodes)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.liveNodes = liveNodes
	return nil
}

// Represents an individual collection.
type collection struct {
	mu            sync.RWMutex
	name          string       // the name of the collection
	stateData     string       // the current state.json data, or empty if no state.json node exists
	zkNodeVersion int32        // the version of the state.json data, or -1 if no state.json node exists
	parent        *SolrMonitor // if nil, this collection object was removed from the ClusterState

	cachedState *parsedCollectionState // cached of the current parsed stateData if non-nil
}

type parsedCollectionState struct {
	collectionState *CollectionState // if non-nil, the parsed stateData
	err             error            // if non-nil, the error generated parsing stateData
}

// Returns the current collection state data.
func (coll *collection) GetData() (*CollectionState, error) {
	cachedState := func() *parsedCollectionState {
		coll.mu.RLock()
		defer coll.mu.RUnlock()
		return coll.cachedState
	}()

	if cachedState == nil {
		cachedState = func() *parsedCollectionState {
			coll.mu.Lock()
			defer coll.mu.Unlock()

			// Could have been initialized in between our first read and getting the write lock.
			if coll.cachedState == nil {
				coll.cachedState = parseStateData(coll.name, []byte(coll.stateData), coll.zkNodeVersion)
			}

			return coll.cachedState
		}()
	}

	return cachedState.collectionState, cachedState.err
}

func parseStateData(name string, data []byte, version int32) *parsedCollectionState {
	if len(data) == 0 {
		return &parsedCollectionState{}
	}
	// The individual per-collection state.json files are kind of weird; they are a single-element map from collection
	// name to a CollectionState object.  In other words they are a ClusterState, containing only the relevant collection.
	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return &parsedCollectionState{err: err}
	}

	var keys []string
	for k := range state {
		keys = append(keys, k)
	}

	if len(keys) != 1 || keys[0] != name {
		err := fmt.Errorf("Expected 1 key, got %s", keys)
		return &parsedCollectionState{err: err}
	}

	collState := state[name]
	collState.ZkNodeVersion = version
	return &parsedCollectionState{collectionState: collState}
}

func (coll *collection) start() error {
	path := coll.parent.solrRoot + "/collections/" + coll.name + "/state.json"
	return coll.parent.zkWatcher.MonitorData(path)
}

func (coll *collection) setData(data string, version int32) {
	if data == "" {
		coll.parent.logger.Printf("%s: no data", coll.name)
	}
	coll.mu.Lock()
	defer coll.mu.Unlock()
	coll.stateData = data
	coll.zkNodeVersion = version
	coll.cachedState = nil
}

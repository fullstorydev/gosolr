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

//go:build integration
// +build integration

package solrmonitor

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/smtestutil"
	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/zk"
)

type testutil struct {
	t                         *testing.T
	conn                      *zk.Conn
	root                      string
	sm                        *SolrMonitor
	logger                    *smtestutil.ZkTestLogger
	solrEventListener         *SEListener
	collectionStateFetchCount *int32
}

func (tu *testutil) teardown() {
	if err := smutil.DeleteRecursive(tu.conn, tu.root); err != nil {
		tu.t.Error(err)
	}
	if tu.sm != nil {
		tu.sm.Close()
	}
	tu.logger.AssertNoErrors(tu.t)
}

func setup(t *testing.T) (*SolrMonitor, *testutil) {
	t.Parallel()

	pc, _, _, _ := runtime.Caller(1)
	callerFunc := runtime.FuncForPC(pc)
	splits := strings.Split(callerFunc.Name(), "/")
	callerName := splits[len(splits)-1]
	callerName = strings.Replace(callerName, ".", "_", -1)
	if !strings.HasPrefix(callerName, "solrmonitor_Test") {
		t.Fatalf("Unexpected callerName: %s should start with smservice_Test", callerName)
	}

	root := "/" + callerName

	logger := smtestutil.NewZkTestLogger(t)
	watcher := NewZkWatcherMan(logger)
	connOption := func(c *zk.Conn) { c.SetLogger(logger) }
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Hour*5, connOption, zk.WithEventCallback(watcher.EventCallback))
	if err != nil {
		t.Fatal(err)
	}

	// So seed that file.
	_, err = conn.Create(root, nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		conn.Close()
		t.Fatal(err)
	}

	l := &SEListener{
		liveNodes:        0,
		queryNodes:       0,
		collections:      0,
		collStateEvents:  0,
		collectionStates: make(map[string]*CollectionState),
	}

	collectionStateFetchCount := int32(0)
	proxyZkClient := &proxyZkCli{
		delegate:                  conn,
		collectionStateFetchCount: &collectionStateFetchCount,
	}

	sm, err := NewSolrMonitorWithRoot(proxyZkClient, watcher, logger, root, l)
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return sm, &testutil{
		t:                         t,
		conn:                      conn,
		root:                      root,
		sm:                        sm,
		logger:                    logger,
		solrEventListener:         l,
		collectionStateFetchCount: &collectionStateFetchCount,
	}
}

// For manual testing, hangs open for a long time.
func disabledTestManual(t *testing.T) {
	_, testutil := setup(t)
	defer testutil.teardown()
	time.Sleep(10 * time.Minute)
}

func TestCollectionChanges(t *testing.T) {
	sm, testutil := setup(t)
	defer testutil.teardown()

	shouldNotExist(t, sm, "c1")

	zkCli := testutil.conn
	zkCli.Create(sm.solrRoot+"/collections", nil, 0, zk.WorldACL(zk.PermAll))
	zkCli.Create(sm.solrRoot+"/live_nodes", nil, 0, zk.WorldACL(zk.PermAll))
	zkCli.Create(sm.solrRoot+"/live_query_nodes", nil, 0, zk.WorldACL(zk.PermAll))

	_, err := zkCli.Create(sm.solrRoot+"/live_nodes/localhost:8983", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	_, err = zkCli.Create(sm.solrRoot+"/live_query_nodes/localhost:8984", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1", []byte(`{"configName":"_FS5"}`), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{ \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\", \"Base_url\":\"solr\", \"node_name\":\"8984_solr\", \"state\":\"active\", \"leader\":\"false\", \"type\": \"NRT\", \"force_set_state\":\"false\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}

	collectionAssertions := func(configName string) func(collectionState *CollectionState) error {
		return func(collectionState *CollectionState) error {
			if collectionState.ConfigName != configName {
				return errors.New(fmt.Sprintf("wrong config name: got %s, expected %s", collectionState.ConfigName, configName))
			}
			shard, _ := collectionState.Shards["shard_1"]
			rep, _ := shard.Replicas["R1"]
			if rep.Leader != "false" {
				return errors.New(fmt.Sprintf("replica is not leader %+v", rep))
			}
			return nil
		}
	}

	shouldExist(t, sm, "c1", collectionAssertions("_FS5"))

	_, err = zkCli.Create(sm.solrRoot+"/collections/c9", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c9")

	_, err = zkCli.Create(sm.solrRoot+"/collections/c9/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c9")

	_, err = zkCli.Set(sm.solrRoot+"/collections/c9/state.json", []byte("{\"c9\":{ \"configName\": \"_FS7\", \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\", \"Base_url\":\"solr\", \"node_name\":\"8984_solr\", \"state\":\"active\", \"leader\":\"false\", \"type\": \"NRT\", \"force_set_state\":\"false\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}

	shouldExist(t, sm, "c9", collectionAssertions("_FS7"))

	if len(testutil.solrEventListener.collectionStates) != 2 || testutil.solrEventListener.collections != 2 {
		t.Fatalf("Event listener didn't  not get event for collection  = %d, collectionstate = %d", testutil.solrEventListener.collections, len(testutil.solrEventListener.collectionStates))
	}

	if testutil.solrEventListener.liveNodes != 1 || testutil.solrEventListener.queryNodes != 1 {
		t.Fatalf("Event listener didn't  not get event for livenodes  = %d, querynodes = %d", testutil.solrEventListener.liveNodes, testutil.solrEventListener.queryNodes)
	}

	// Get a fresh new solr monitor and make sure it starts in the right state.
	w2 := NewZkWatcherMan(testutil.logger)
	connOption := func(c *zk.Conn) { c.SetLogger(testutil.logger) }
	conn2, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Hour*5, connOption, zk.WithEventCallback(w2.EventCallback))
	if err != nil {
		t.Fatal(err)
	}
	sm2, err := NewSolrMonitorWithRoot(conn2, w2, testutil.logger, testutil.root, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sm2.Close()

	shouldExist(t, sm2, "c1", collectionAssertions("_FS5"))

	// if the config name changes (not common), we should get the updates
	_, err = zkCli.Set(sm.solrRoot+"/collections/c1", []byte(`{"configName":"_FS6"}`), 0)
	if err != nil {
		t.Fatal(err)
	}

	shouldExist(t, sm2, "c1", collectionAssertions("_FS6"))

	err = smutil.DeleteRecursive(zkCli, sm.solrRoot+"/collections/c1")
	if err != nil {
		t.Fatal(err)
	}

	// which should propagate to the sm instances
	shouldNotExist(t, sm, "c1")
	shouldNotExist(t, sm2, "c1")
}

func TestPRSProtocol(t *testing.T) {
	sm, testSetup := setup(t)
	defer testSetup.teardown()

	shouldNotExist(t, sm, "c1")

	zkCli := testSetup.conn
	zkCli.Create(sm.solrRoot+"/collections", nil, 0, zk.WorldACL(zk.PermAll))
	_, err := zkCli.Create(sm.solrRoot+"/collections/c1", []byte(`{"configName":"_FS4"}`), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	currentFetchCount := testSetup.collectionStateFetchCount
	shouldNotExist(t, sm, "c1")
	checkCollectionStateCallback(t, 1, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)
	//checkFetchCount(t, currentFetchCount, 2) //state.json fetch and 1 children fetch on PRS from coll.start()
	checkFetchCount(t, currentFetchCount, 1) //state.json fetch

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")
	checkCollectionStateCallback(t, 2, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)
	//checkFetchCount(t, currentFetchCount, 3) //state.json fetch from new state.json
	checkFetchCount(t, currentFetchCount, 2) //state.json fetch from new state.json

	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{\"perReplicaState\":\"true\",	 \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\", \"state\":\"down\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}

	collectionAssertions := func(collectionState *CollectionState) error {
		if collectionState.ConfigName != "_FS4" {
			return errors.New(fmt.Sprintf("wrong config name: got %s, expected %s", collectionState.ConfigName, "_FS4"))
		}
		return nil
	}

	shouldExist(t, sm, "c1", collectionAssertions)
	checkCollectionStateCallback(t, 3, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)
	checkFetchCount(t, currentFetchCount, 4) //state.json fetch from updated state.json

	// 1. adding PRS for replica R1, version 1, state down
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:1:D", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "down", "false", 1)
	checkCollectionStateCallback(t, 4, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)
	// 2. adding PRS for replica R1, version 1 -same, state active => should ignore as same version
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:1:R", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "down", "false", 1)
	checkCollectionStateCallback(t, 5, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)

	// 3. adding PRS for replica R1, version 2, state active
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:2:A", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	err = zkCli.Delete(sm.solrRoot+"/collections/c1/state.json/R1:1:D", int32(-1))
	if err != nil {
		t.Fatal(err)
	}
	err = zkCli.Delete(sm.solrRoot+"/collections/c1/state.json/R1:1:R", int32(-1))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "active", "false", 2)
	checkCollectionStateCallback(t, 7, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)

	// 4. adding PRS for replica R1, version 3, state active and leader
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:3:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	err = zkCli.Delete(sm.solrRoot+"/collections/c1/state.json/R1:2:A", int32(-1))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "active", "true", 3)
	checkCollectionStateCallback(t, 8, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)

	//5. split shard
	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{\"perReplicaState\":\"true\",	 \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\", \"state\":\"down\"}}}, \"shard_1_0\":{\"replicas\":{\"R1_0\":{\"core\":\"core1\", \"state\":\"down\"}}}, \"shard_1_1\":{\"replicas\":{\"R1_1\":{\"core\":\"core1\", \"state\":\"down\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5000 * time.Millisecond)
	checkCollectionStateCallback(t, 9, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)
	//checkFetchCount(t, currentFetchCount, 5) //state.json fetch from updated state.json
	checkFetchCount(t, currentFetchCount, 10) //state.json fetch from updated state.json + PRS child changes

	// 6. replica R1_0 should exist
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1_0:1:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1_0", "R1_0", "active", "true", 1)
	checkCollectionStateCallback(t, 10, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)

	// 7. replica R1_1 should exist
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1_1:1:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1_1", "R1_1", "active", "true", 1)
	checkCollectionStateCallback(t, 11, testSetup.solrEventListener.collStateEvents+testSetup.solrEventListener.collReplicaChangeEvents)

	if testSetup.solrEventListener.collStateEvents != 4 || testSetup.solrEventListener.collections != 1 {
		t.Fatalf("Event listener didn't  not get event for collection  = %d, collectionstateEvents = %d", testSetup.solrEventListener.collections, testSetup.solrEventListener.collStateEvents)
	}

	// and after all of the updates, should still exist with same config name
	shouldExist(t, sm, "c1", collectionAssertions)

	//test on a brand new solrmonitor/conn with an existing collection, it should load correctly
	logger := smtestutil.NewZkTestLogger(t)
	watcher := NewZkWatcherMan(logger)
	connOption := func(c *zk.Conn) { c.SetLogger(logger) }
	fetchCount := int32(0)

	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption, zk.WithEventCallback(watcher.EventCallback))
	if err != nil {
		t.Fatal(err)
	}
	sm, err = NewSolrMonitorWithRoot(&proxyZkCli{conn, &fetchCount}, watcher, logger, sm.solrRoot, nil) //make a new solrmonitor
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	collState, err := sm.GetCollectionState("c1")
	if err != nil {
		t.Fatal(err)
	}
	replicaState := collState.Shards["shard_1"].Replicas["R1"]
	if replicaState.State != "active" {
		t.Fatalf("Expected replica R1 state active but found %s", replicaState.State)
	}
	if replicaState.Leader != "true" {
		t.Fatalf("Expected replica R1 leadership but found %s", replicaState.Leader)
	}
	if replicaState.Version != 3 {
		t.Fatalf("Expected replica R1 version 3 but found %v", replicaState.Version)
	}
	//For brevity, just check states for child shards
	if collState.Shards["shard_1_0"].Replicas["R1_0"].State != "active" {
		t.Fatalf("Expected replica R1_0 state active, but it's not")
	}
	if collState.Shards["shard_1_1"].Replicas["R1_1"].State != "active" {
		t.Fatalf("Expected replica R1_1 state active, but it's not")
	}
	checkFetchCount(t, &fetchCount, 2) //state.json fetch and 1 children fetch on PRS from coll.start()
}

func checkCollectionStateCallback(t *testing.T, expected int, found int) {
	if expected != found {
		t.Fatalf("listener event is %d, expected %d ", found, expected)
	}
}

// that was meant for cachedState, which we removed as now we need to deserialize the stream as need to know PRS state of collection
func DisabledTestBadStateJson(t *testing.T) {
	sm, testutil := setup(t)
	defer testutil.teardown()

	shouldNotExist(t, sm, "c1")

	zkCli := testutil.conn
	zkCli.Create(sm.solrRoot+"/collections", nil, 0, zk.WorldACL(zk.PermAll))
	_, err := zkCli.Create(sm.solrRoot+"/collections/c1", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json", []byte("asdf"), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldError(t, sm, "c1")
	shouldBecomeEq(t, 1, testutil.logger.GetErrorCount)
	testutil.logger.Clear()
}

type SEListener struct {
	liveNodes               int
	queryNodes              int
	collections             int
	collStateEvents         int
	collReplicaChangeEvents int
	clusterPropChangeEvents int
	collectionStates        map[string]*CollectionState
}

func (l *SEListener) SolrLiveNodesChanged(livenodes []string) {
	l.liveNodes = len(livenodes)
}

func (l *SEListener) SolrQueryNodesChanged(querynodes []string) {
	l.queryNodes = len(querynodes)
}

func (l *SEListener) SolrCollectionsChanged(collections []string) {
	l.collections = len(collections)
}

func (l *SEListener) SolrCollectionStateChanged(name string, collectionState *CollectionState) {
	l.collStateEvents++
	l.collectionStates[name] = collectionState
}

func (l *SEListener) SolrCollectionReplicaStatesChanged(name string, replicaStates map[string]*PerReplicaState) {
	l.collReplicaChangeEvents++
}

func (l *SEListener) SolrClusterPropsChanged(clusterprops map[string]string) {
	l.clusterPropChangeEvents++
}

// proxyZkCli to ensure the zk collection state fetch count is as expected
type proxyZkCli struct {
	delegate                  ZkCli
	collectionStateFetchCount *int32 //fetches on collection state.json including PRS entries
}

var _ ZkCli = &proxyZkCli{}

func (p *proxyZkCli) Children(path string) ([]string, *zk.Stat, error) {
	if strings.Contains(path, "/state.json") {
		atomic.AddInt32(p.collectionStateFetchCount, 1)
	}
	return p.delegate.Children(path)
}

func (p *proxyZkCli) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if strings.Contains(path, "/state.json") {
		atomic.AddInt32(p.collectionStateFetchCount, 1)
	}
	return p.delegate.ChildrenW(path)
}

func (p *proxyZkCli) Get(path string) ([]byte, *zk.Stat, error) {
	if strings.Contains(path, "/state.json") {
		atomic.AddInt32(p.collectionStateFetchCount, 1)
	}
	return p.delegate.Get(path)
}

func (p *proxyZkCli) GetW(path string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	if strings.Contains(path, "/state.json") {
		atomic.AddInt32(p.collectionStateFetchCount, 1)
	}
	return p.delegate.GetW(path)
}

func (p *proxyZkCli) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	return p.delegate.ExistsW(path)
}

func (p *proxyZkCli) State() zk.State {
	return p.delegate.State()
}

func (p *proxyZkCli) Close() {
	p.delegate.Close()
}

func (p *proxyZkCli) AddPersistentWatch(path string, mode zk.AddWatchMode) (ch zk.EventQueue, err error) {
	return p.delegate.AddPersistentWatch(path, mode)
}

func (p *proxyZkCli) RemoveAllPersistentWatches(path string) (err error) {
	return p.delegate.RemoveAllPersistentWatches(path)
}

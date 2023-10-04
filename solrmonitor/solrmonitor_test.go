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
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/smtestutil"
	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/zk"
)

type testutil struct {
	t                 *testing.T
	conn              *zk.Conn
	root              string
	sm                *SolrMonitor
	logger            *smtestutil.ZkTestLogger
	solrEventListener *SEListener
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
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption, zk.WithEventCallback(watcher.EventCallback))
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
	sm, err := NewSolrMonitorWithRoot(conn, watcher, logger, root, false, l)
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return sm, &testutil{
		t:                 t,
		conn:              conn,
		root:              root,
		sm:                sm,
		logger:            logger,
		solrEventListener: l,
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

	//create a sys collection, it should NOT be detected
	_, err = zkCli.Create(sm.solrRoot+"/collections/.sys.COORDINATOR-COLL-_FS7", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	shouldNotExist(t, sm, ".sys.COORDINATOR-COLL-_FS7")

	_, err = zkCli.Create(sm.solrRoot+"/collections/.sys.COORDINATOR-COLL-_FS7/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	shouldNotExist(t, sm, ".sys.COORDINATOR-COLL-_FS7")

	_, err = zkCli.Set(sm.solrRoot+"/collections/.sys.COORDINATOR-COLL-_FS7/state.json", []byte("{\".sys.COORDINATOR-COLL-_FS7\":{ \"configName\": \"_FS7\", \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\", \"Base_url\":\"solr\", \"node_name\":\"8984_solr\", \"state\":\"active\", \"leader\":\"false\", \"type\": \"NRT\", \"force_set_state\":\"false\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}
	shouldNotExist(t, sm, ".sys.COORDINATOR-COLL-_FS7")

	if len(testutil.solrEventListener.collectionStates) != 2 || testutil.solrEventListener.collections != 2 {
		t.Fatalf("Event listener didn't  not get event for collection  = %d, collectionstate = %d", testutil.solrEventListener.collections, len(testutil.solrEventListener.collectionStates))
	}

	if testutil.solrEventListener.liveNodes != 1 || testutil.solrEventListener.queryNodes != 1 {
		t.Fatalf("Event listener didn't  not get event for livenodes  = %d, querynodes = %d", testutil.solrEventListener.liveNodes, testutil.solrEventListener.queryNodes)
	}

	// Get a fresh new solr monitor and make sure it starts in the right state.
	w2 := NewZkWatcherMan(testutil.logger)
	connOption := func(c *zk.Conn) { c.SetLogger(testutil.logger) }
	conn2, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption, zk.WithEventCallback(w2.EventCallback))
	if err != nil {
		t.Fatal(err)
	}
	sm2, err := NewSolrMonitorWithRoot(conn2, w2, testutil.logger, testutil.root, false, nil)
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
	sm, testutil := setup(t)
	defer testutil.teardown()

	shouldNotExist(t, sm, "c1")

	zkCli := testutil.conn
	zkCli.Create(sm.solrRoot+"/collections", nil, 0, zk.WorldACL(zk.PermAll))
	_, err := zkCli.Create(sm.solrRoot+"/collections/c1", []byte(`{"configName":"_FS4"}`), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")
	checkCollectionStateCallback(t, 1, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")
	checkCollectionStateCallback(t, 2, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{\"perReplicaState\":\"true\",	 \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\"}}}}}}"), -1)
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
	checkCollectionStateCallback(t, 3, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	// 1. adding PRS for replica R1, version 1, state down
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:1:D", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "down", "false", 1)
	checkCollectionStateCallback(t, 4, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)
	// 2. adding PRS for replica R1, version 1 -same, state active => should ignore as same version
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:1:R", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "down", "false", 1)
	checkCollectionStateCallback(t, 5, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	// 3. adding PRS for replica R1, version 2, state active
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:2:A", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "active", "false", 2)
	checkCollectionStateCallback(t, 6, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	// 4. adding PRS for replica R1, version 3, state active and leader
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1:3:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1", "R1", "active", "true", 3)
	checkCollectionStateCallback(t, 7, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	//5. split shard
	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{\"perReplicaState\":\"true\",	 \"shards\":{\"shard_1\":{\"replicas\":{\"R1\":{\"core\":\"core1\"}}}, \"shard_1_0\":{\"replicas\":{\"R1_0\":{\"core\":\"core1\"}}}, \"shard_1_1\":{\"replicas\":{\"R1_1\":{\"core\":\"core1\"}}}}}}"), -1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5000 * time.Millisecond)
	checkCollectionStateCallback(t, 8, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	// 6. replica R1_0 should exist
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1_0:1:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1_0", "R1_0", "active", "true", 1)
	checkCollectionStateCallback(t, 9, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	// 7. replica R1_1 should exist
	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json/R1_1:1:A:L", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	prsShouldExist(t, sm, "c1", "shard_1_1", "R1_1", "active", "true", 1)
	checkCollectionStateCallback(t, 10, testutil.solrEventListener.collStateEvents+testutil.solrEventListener.collReplicaChangeEvents)

	if testutil.solrEventListener.collStateEvents != 4 || testutil.solrEventListener.collections != 1 {
		t.Fatalf("Event listener didn't  not get event for collection  = %d, collectionstateEvents = %d", testutil.solrEventListener.collections, testutil.solrEventListener.collStateEvents)
	}

	// and after all of the updates, should still exist with same config name
	shouldExist(t, sm, "c1", collectionAssertions)

	ref1, e1 := sm.GetCollectionState("c1")

	// forcing collection re-fetched
	e2 := sm.InvalidateCollectionState("c1")
	if e2 != nil {
		t.Fatalf("Unable to invalidate the collection %s", e2.Error())
	}

	ref2, e3 := sm.GetCollectionState("c1")

	// making sure collection reference are different
	if e1 != nil || e3 != nil || ref1 == ref2 {
		t.Fatalf("Collection ref is same or error e1: %s, e3:%s", e1.Error(), e3.Error())
	}
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

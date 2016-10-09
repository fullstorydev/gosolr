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

// +build integration

package solrmonitor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/solrmonitor/smtesting"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	checkTimeout  = 3 * time.Second
	checkInterval = 10 * time.Millisecond
)

func createSolrMonitor(t *testing.T) (*zk.Conn, *SolrMonitor, *smtesting.ZkTestLogger, error) {
	logger := smtesting.NewZkTestLogger(t)
	connOption := func(c *zk.Conn) { c.SetLogger(logger) }
	zkCli, zkEvent, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption)
	if err != nil {
		return nil, nil, nil, err
	}

	// Keep a running log.
	go func() {
		for e := range zkEvent {
			t.Logf("Global: %s", e)
		}
	}()

	sm, err := NewSolrMonitorWithRoot(zkCli, logger, "/solrmonitortest")
	if err != nil {
		zkCli.Close()
		return nil, nil, nil, err
	}
	return zkCli, sm, logger, nil
}

// For manual testing, hangs open for a long time.
func disabledTestManual(t *testing.T) {
	zkCli, sm, logger, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.AssertNoErrors(t)
	defer zkCli.Close()
	defer sm.Close()

	time.Sleep(10 * time.Minute)
}

// Test we shut down cleanly when the ZK client is closed.
func TestCleanCloseZk(t *testing.T) {
	zkCli, sm, logger, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.AssertNoErrors(t)
	//defer zkCli.Close()
	defer sm.Close()

	getRunningProcs := func() int32 { return atomic.LoadInt32(&sm.dispatcher.runningProcs) }
	shouldBecomeEq(t, 1, getRunningProcs)
	zkCli.Close()
	shouldBecomeEq(t, 0, getRunningProcs)
}

// Test we shut down cleanly when the ClusterState is closed.
func TestCleanCloseSolrMonitor(t *testing.T) {
	zkCli, sm, logger, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.AssertNoErrors(t)
	defer zkCli.Close()
	//defer sm.Close()

	getRunningProcs := func() int32 { return atomic.LoadInt32(&sm.dispatcher.runningProcs) }
	shouldBecomeEq(t, 1, getRunningProcs)
	sm.Close()
	shouldBecomeEq(t, 0, getRunningProcs)
}

func TestCollectionChanges(t *testing.T) {
	zkCli, sm, logger, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.AssertNoErrors(t)
	defer zkCli.Close()
	defer sm.Close()

	// Clean out any garbage from previous runs.
	zkCli.Delete("/solrmonitortest/collections/c1/state.json", -1)
	zkCli.Delete("/solrmonitortest/collections/c1", -1)
	zkCli.Delete("/solrmonitortest/collections", -1)
	zkCli.Delete("/solrmonitortest", -1)

	shouldNotExist(t, sm, "c1")

	zkCli.Create("/solrmonitortest", []byte(""), 0, zk.WorldACL(zk.PermAll))
	zkCli.Create("/solrmonitortest/collections", []byte(""), 0, zk.WorldACL(zk.PermAll))
	_, err = zkCli.Create("/solrmonitortest/collections/c1", []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Delete("/solrmonitortest/collections/c1", -1)
	defer zkCli.Delete("/solrmonitortest/collections", -1)
	defer zkCli.Delete("/solrmonitortest", -1)

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Create("/solrmonitortest/collections/c1/state.json", []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Delete("/solrmonitortest/collections/c1/state.json", -1)

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Set("/solrmonitortest/collections/c1/state.json", []byte("{\"c1\":{}}"), -1)
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Delete("/solrmonitortest/collections/c1/state.json", -1)

	shouldExist(t, sm, "c1")

	// Get a fresh new solr monitor and make sure it starts in the right state.
	zkCli2, sm2, logger2, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer logger2.AssertNoErrors(t)
	defer zkCli2.Close()
	defer sm2.Close()

	shouldExist(t, sm2, "c1")
}

func TestBadStateJson(t *testing.T) {
	zkCli, sm, logger, err := createSolrMonitor(t)
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Close()
	defer sm.Close()

	// Clean out any garbage from previous runs.
	zkCli.Delete("/solrmonitortest/collections/c1/state.json", -1)
	zkCli.Delete("/solrmonitortest/collections/c1", -1)
	zkCli.Delete("/solrmonitortest/collections", -1)
	zkCli.Delete("/solrmonitortest", -1)

	shouldNotExist(t, sm, "c1")

	zkCli.Create("/solrmonitortest", []byte(""), 0, zk.WorldACL(zk.PermAll))
	zkCli.Create("/solrmonitortest/collections", []byte(""), 0, zk.WorldACL(zk.PermAll))
	_, err = zkCli.Create("/solrmonitortest/collections/c1", []byte(""), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Delete("/solrmonitortest/collections/c1", -1)
	defer zkCli.Delete("/solrmonitortest/collections", -1)
	defer zkCli.Delete("/solrmonitortest", -1)

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Create("/solrmonitortest/collections/c1/state.json", []byte("asdf"), 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}
	defer zkCli.Delete("/solrmonitortest/collections/c1/state.json", -1)

	shouldError(t, sm, "c1")
	shouldBecomeEq(t, 1, logger.GetErrorCount)
}

func shouldBecomeEq(t *testing.T, expected int32, actualFunc func() int32) {
	var actual int32
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		actual = actualFunc()
		if expected == actual {
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %d, got: %d", expected, actual)
}

func shouldExist(t *testing.T, sm *SolrMonitor, name string) {
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		collectionState, err := sm.GetCollectionState(name)
		if err != nil {
			t.Fatal(err)
			return
		}
		if collectionState != nil {
			// GetCurrentState should be consistent
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val == nil || !ok {
				t.Errorf("expected %s to exist in state map, but it does not", name)
			}
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %s to exist, but it does not", name)
}

func shouldNotExist(t *testing.T, sm *SolrMonitor, name string) {
	// Wait a moment before checking so we don't false-positive.
	time.Sleep(200 * time.Millisecond)
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		collectionState, err := sm.GetCollectionState(name)
		if err != nil {
			t.Fatal(err)
			return
		}
		if collectionState == nil {
			// GetCurrentState should be consistent
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val != nil || ok {
				t.Errorf("expected %s to not exist in state map, but it does", name)
			}
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %s to not exist, but it does", name)
}

func shouldError(t *testing.T, sm *SolrMonitor, name string) {
	for end := time.Now().Add(checkTimeout); time.Now().Before(end); {
		_, err := sm.GetCollectionState(name)
		if err != nil {
			// GetCurrentState should only silently record an error, however
			state, err := sm.GetCurrentState()
			if err != nil {
				t.Fatal(err)
				return
			}
			val, ok := state[name]
			if val != nil || ok {
				t.Errorf("expected %s to not exist in state map, but it does", name)
			}
			return // success
		}
		time.Sleep(checkInterval)
	}
	t.Errorf("expected %s to error, but no error", name)
}

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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/smtestutil"
	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/zk"
)

type testutil struct {
	t      *testing.T
	conn   *zk.Conn
	root   string
	sm     *SolrMonitor
	logger *smtestutil.ZkTestLogger
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

	// Solrmonitor checks the "clusterstate.json" file in the root node it is given.
	// So seed that file.
	_, err = conn.Create(root, nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		conn.Close()
		t.Fatal(err)
	}
	_, err = conn.Create(root+"/clusterstate.json", []byte("{}"), 0, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		conn.Close()
		t.Fatal(err)
	}

	sm, err := NewSolrMonitorWithRoot(conn, watcher, logger, root)
	if err != nil {
		conn.Close()
		t.Fatal(err)
	}
	return sm, &testutil{
		t:      t,
		conn:   conn,
		root:   root,
		sm:     sm,
		logger: logger,
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
	_, err := zkCli.Create(sm.solrRoot+"/collections/c1", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Create(sm.solrRoot+"/collections/c1/state.json", nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		t.Fatal(err)
	}

	shouldNotExist(t, sm, "c1")

	_, err = zkCli.Set(sm.solrRoot+"/collections/c1/state.json", []byte("{\"c1\":{}}"), -1)
	if err != nil {
		t.Fatal(err)
	}

	shouldExist(t, sm, "c1")

	// Get a fresh new solr monitor and make sure it starts in the right state.
	w2 := NewZkWatcherMan(testutil.logger)
	connOption := func(c *zk.Conn) { c.SetLogger(testutil.logger) }
	conn2, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption, zk.WithEventCallback(w2.EventCallback))
	if err != nil {
		t.Fatal(err)
	}
	sm2, err := NewSolrMonitorWithRoot(conn2, w2, testutil.logger, testutil.root)
	if err != nil {
		t.Fatal(err)
	}
	defer sm2.Close()

	shouldExist(t, sm2, "c1")
}

func TestBadStateJson(t *testing.T) {
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

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

package smutil_test

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fullstorydev/gosolr/smtestutil"
	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/zk"
)

type testutil struct {
	t      *testing.T
	root   string
	conn   *zk.Conn
	logger *smtestutil.ZkTestLogger
}

func (tu *testutil) teardown() {
	if err := smutil.DeleteRecursive(tu.conn, tu.root); err != nil {
		tu.t.Error(err)
	}
	tu.conn.Close()
	tu.logger.AssertNoErrors(tu.t)
}

func (tu *testutil) create(path string) {
	if !strings.HasPrefix(path, tu.root) {
		panic(fmt.Sprintf("cannot create node at %s, must be under %s", path, tu.root))
	}

	if _, err := tu.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll)); err != nil && err != zk.ErrNodeExists {
		tu.t.Fatal(err)
	}
}

func (tu *testutil) del(path string) {
	if !strings.HasPrefix(path, tu.root) {
		panic(fmt.Sprintf("cannot delete node at %s, must be under %s", path, tu.root))
	}

	if err := tu.conn.Delete(path, -1); err != nil && err != zk.ErrNoNode {
		tu.t.Fatal(err)
	}
}

func setup(t *testing.T) *testutil {
	pc, _, _, _ := runtime.Caller(1)
	callerFunc := runtime.FuncForPC(pc)
	splits := strings.Split(callerFunc.Name(), "/")
	callerName := splits[len(splits)-1]
	callerName = strings.Replace(callerName, ".", "_", -1)
	if !strings.HasPrefix(callerName, "smutil_test_Test") {
		t.Fatalf("Unexpected callerName: %s should start with smutil_test_Test", callerName)
	}

	root := "/" + callerName

	logger := smtestutil.NewZkTestLogger(t)
	connOption := func(c *zk.Conn) {
		c.SetLogger(logger)
	}
	conn, zkEvent, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*5, connOption)
	if err != nil {
		t.Fatal(err)
	}

	// Keep a running log.
	go func() {
		for e := range zkEvent {
			t.Logf("Global: %s", e)
		}
	}()

	tu := &testutil{
		t:      t,
		root:   root,
		conn:   conn,
		logger: logger,
	}

	tu.create(tu.root)
	return tu
}

func TestAcquireAndMonitorZkMutexSerial(t *testing.T) {
	t.Parallel()

	onLostMutex := func() {
		t.Error("should not happen")
	}

	// Successive acquisitions should be very fast.
	const expectDuration = 3 * time.Second
	start := time.Now()
	for i := 0; i < 3; i++ {
		// Use a different ZK connection per client (otherwise the session IDs collide)
		tu := setup(t)
		defer tu.teardown()

		closer, err := smutil.AcquireAndMonitorZkMutex(smtestutil.NewSmTestLogger(t), tu.conn, tu.root+"/mutex", onLostMutex)
		if err != nil {
			t.Fatal(err)
		}
		closer()
	}
	elapsed := time.Since(start)
	if elapsed > expectDuration {
		t.Errorf("Expected < %s, took %s", expectDuration, elapsed)
	}
}

func TestAcquireAndMonitorZkMutexParallel(t *testing.T) {
	t.Parallel()

	onLostMutex := func() {
		t.Error("should not happen")
	}

	var wg sync.WaitGroup
	mutexHolders := int32(0)

	const expectDuration = 10 * time.Second
	start := time.Now()
	for i := 0; i < 10; i++ {
		wg.Add(1)

		// Use a different ZK connection per client (otherwise the session IDs collide)
		tu := setup(t)
		defer tu.teardown()
		go func(tu *testutil) {
			defer wg.Done()

			closer, err := smutil.AcquireAndMonitorZkMutex(smtestutil.NewSmTestLogger(t), tu.conn, tu.root+"/mutex", onLostMutex)
			if err != nil {
				t.Fatal(err)
			}
			defer closer()
			if !atomic.CompareAndSwapInt32(&mutexHolders, 0, 1) {
				t.Error("Expected mutex holders to be 0 after acquiring mutex, but it wasn't!")
			}
			runtime.Gosched()
			if !atomic.CompareAndSwapInt32(&mutexHolders, 1, 0) {
				t.Error("Expected mutex holders to be 1 before releasing mutex, but it wasn't!")
			}
		}(tu)
	}

	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > expectDuration {
		t.Errorf("Expected < %s, took %s", expectDuration, elapsed)
	}
}

func TestAcquireAndMonitorZkMutexLost(t *testing.T) {
	t.Parallel()

	mutexWasLost := make(chan struct{})
	onLostMutex := func() {
		close(mutexWasLost)
	}

	tu := setup(t)
	defer tu.teardown()

	closer, err := smutil.AcquireAndMonitorZkMutex(smtestutil.NewSmTestLogger(t), tu.conn, tu.root+"/mutex", onLostMutex)
	if err != nil {
		t.Fatal(err)
	}
	defer closer()

	select {
	case <-mutexWasLost:
		t.Fatal("shouldn't have lost mutex yet")
	case <-time.After(100 * time.Millisecond):
		// expected
	}

	// Now forcibly delete the ZK node.
	tu.del(tu.root + "/mutex")
	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("should have lost mutex")
	case <-mutexWasLost:
		// expected
	}
}

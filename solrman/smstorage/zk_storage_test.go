// Copyright 2017 FullStory, Inc.
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

package smstorage

import (
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
	t      *testing.T
	s      *ZkStorage
	logger *smtestutil.ZkTestLogger
}

func (tu *testutil) teardown() {
	if err := smutil.DeleteRecursive(tu.s.conn, tu.s.root); err != nil {
		tu.t.Error(err)
	}
	tu.s.conn.Close()
	tu.logger.AssertNoErrors(tu.t)
}

func (tu *testutil) create(path string) {
	if !strings.HasPrefix(path, tu.s.root) {
		panic(fmt.Sprintf("cannot create node at %s, must be under %s", path, tu.s.root))
	}

	if _, err := tu.s.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		tu.t.Fatal(err)
	}
}

func (tu *testutil) createWithData(path string, data string) {
	if !strings.HasPrefix(path, tu.s.root) {
		panic(fmt.Sprintf("cannot create node at %s, must be under %s", path, tu.s.root))
	}

	if _, err := tu.s.conn.Create(path, []byte(data), 0, zk.WorldACL(zk.PermAll)); err != nil {
		tu.t.Fatal(err)
	}

}

func (tu *testutil) del(path string) {
	if !strings.HasPrefix(path, tu.s.root) {
		panic(fmt.Sprintf("cannot delete node at %s, must be under %s", path, tu.s.root))
	}

	if err := tu.s.conn.Delete(path, -1); err != nil {
		tu.t.Fatal(err)
	}
}

func setup(t *testing.T) (*ZkStorage, *testutil) {
	t.Parallel()

	pc, _, _, _ := runtime.Caller(1)
	callerFunc := runtime.FuncForPC(pc)
	splits := strings.Split(callerFunc.Name(), "/")
	callerName := splits[len(splits)-1]
	callerName = strings.Replace(callerName, ".", "_", -1)
	if !strings.HasPrefix(callerName, "smstorage_Test") {
		t.Fatalf("Unexpected callerName: %s should start with smstorage_Test", callerName)
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

	s, err := NewZkStorage(conn, root, smtestutil.NewSmTestLogger(t))
	if err != nil {
		t.Fatal(err)
	}

	return s, &testutil{
		t:      t,
		s:      s,
		logger: logger,
	}
}

func TestZkStorage_InProgressOps(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()
	testStorage_InProgressOps(t, s)
}

func TestZkStorage_CompletedOps(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()
	testStorage_CompletedOps(t, s)
}

func TestZkStorage_IsDisabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if isDisabled, _ := s.IsDisabled(); isDisabled {
		t.Error("expected to not be disabled")
	}

	testutil.createWithData(s.disabledPath()+"/test", "testDisabled")

	if isDisabled, _ := s.IsDisabled(); !IsDisabled {
		t.Error("expected to be disabled")
	} else if reasons, _ := s.GetDisabledReasons(); reasons[0] != "testDisabled" {
		t.Errorf("expect reason is \"testDisabled\"; got %s", reason)
	}

}

func TestZkStorage_SetDisabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if isDisabled, reason := s.IsDisabled(); isDisabled {
		t.Error("expected to not be disabled; reason found was %s", reason)
	}
	if ok, _, _ := s.conn.Exists(s.disabledPath()); ok {
		t.Errorf("%s should not exist", s.disabledPath())
	}

	s.AddDisabledReason("testor", "testSetDisabled")
	if isDisabled, _ := s.IsDisabled(); !isDisabled {
		t.Error("expected to be disabled")
	}
	if ok, _, _ := s.conn.Exists(s.disabledPath() + "/testor"); !ok {
		t.Errorf("%s should exist", s.disabledPath())
	}

	s.RemoveDisabledReason("testor")
	if isDisabled, _ := s.IsDisabled(); isDisabled {
		t.Error("expected to not be disabled")
	}
	if ok, _, _ := s.conn.Exists(s.disabledPath() + "/testor"); ok {
		t.Errorf("%s should not exist", s.disabledPath())
	}
}

func TestZkStorage_IsMovesDisabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if s.IsMovesDisabled() {
		t.Error("expected moves to not be disabled")
	}

	testutil.create(s.disableMovesPath())

	if !s.IsMovesDisabled() {
		t.Error("expected moves to be disabled")
	}
}

func TestZkStorage_IsSplitsDisabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if s.IsSplitsDisabled() {
		t.Error("expected splits to not be disabled")
	}

	testutil.create(s.disableSplitsPath())

	if !s.IsSplitsDisabled() {
		t.Error("expected splits to be disabled")
	}
}

func TestZkStorage_IsStabbingEnabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if s.IsStabbingEnabled() {
		t.Error("expected stabbing to be disabled")
	}

	testutil.create(s.enableStabbingPath())

	if !s.IsStabbingEnabled() {
		t.Error("expected stabbing to be enabled")
	}
}

func TestZkStorage_IsQueryAggregatorStabbingEnabled(t *testing.T) {
	s, testutil := setup(t)
	defer testutil.teardown()

	if s.IsQueryAggregatorStabbingEnabled() {
		t.Error("expected query aggregator stabbing to be disabled")
	}

	testutil.create(s.enableQueryAggregatorStabbingPath())

	if !s.IsQueryAggregatorStabbingEnabled() {
		t.Error("expected query aggregator stabbing to be enabled")
	}
}

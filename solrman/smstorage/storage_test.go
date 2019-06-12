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

package smstorage

import (
	"sort"
	"strconv"
	"testing"

	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
)

func testStorage_InProgressOps(t *testing.T, s SolrManStorage) {
	assertOps := func(expectOps ...string) {
		ops, err := s.GetInProgressOps()
		if err != nil {
			t.Errorf("GetInProgressOps failed: %s", err)
			return
		}
		if len(expectOps) != len(ops) {
			t.Errorf("expect len %d != actual len %d", len(expectOps), len(ops))
			return
		}
		for i := range ops {
			if expectOps[i] != ops[i].Key() {
				t.Errorf("expect %s != actual %s", expectOps[i], ops[i].Key())
			}
		}
	}

	assertOps()

	ops := []solrmanapi.OpRecord{
		{Collection: "foo", Shard: "1", StartedMs: 1},
		{Collection: "bar", Shard: "2", StartedMs: 2},
		{Collection: "baz", Shard: "3", StartedMs: 3},
	}

	s.AddInProgressOp(ops[0])
	assertOps("SolrOp:foo:1")

	s.AddInProgressOp(ops[2])
	assertOps("SolrOp:baz:3", "SolrOp:foo:1")

	s.AddInProgressOp(ops[1])
	assertOps("SolrOp:baz:3", "SolrOp:bar:2", "SolrOp:foo:1")

	s.DelInProgressOp(ops[0])
	assertOps("SolrOp:baz:3", "SolrOp:bar:2")

	s.DelInProgressOp(ops[2])
	assertOps("SolrOp:bar:2")

	s.DelInProgressOp(ops[1])
	assertOps()
}

func testStorage_CompletedOps(t *testing.T, s SolrManStorage) {
	assertOps := func(count int, expectOps ...string) {
		ops, err := s.GetCompletedOps(count)
		if err != nil {
			t.Errorf("GetInProgressOps failed: %s", err)
			return
		}
		if len(expectOps) != len(ops) {
			t.Errorf("expect len %d != actual len %d", len(expectOps), len(ops))
			return
		}
		for i := range ops {
			if expectOps[i] != ops[i].Key() {
				t.Errorf("expect %s != actual %s", expectOps[i], ops[i].Key())
			}
		}
	}

	assertOps(0)
	assertOps(1)
	assertOps(99)

	ops := []solrmanapi.OpRecord{
		{Collection: "foo", Shard: "1", FinishedMs: 1},
		{Collection: "bar", Shard: "2", FinishedMs: 2},
		{Collection: "baz", Shard: "3", FinishedMs: 3},
	}

	s.AddCompletedOp(ops[0])
	assertOps(99, "SolrOp:foo:1")
	assertOps(1, "SolrOp:foo:1")
	assertOps(0)

	s.AddCompletedOp(ops[1])
	assertOps(99, "SolrOp:bar:2", "SolrOp:foo:1")
	assertOps(2, "SolrOp:bar:2", "SolrOp:foo:1")
	assertOps(1, "SolrOp:bar:2")
	assertOps(0)

	s.AddCompletedOp(ops[2])
	assertOps(99, "SolrOp:baz:3", "SolrOp:bar:2", "SolrOp:foo:1")
	assertOps(3, "SolrOp:baz:3", "SolrOp:bar:2", "SolrOp:foo:1")
	assertOps(2, "SolrOp:baz:3", "SolrOp:bar:2")
	assertOps(1, "SolrOp:baz:3")
	assertOps(0)

	// Now add a bunch and make sure only NumStoredCompletedOps come back.
	bigNum := NumStoredCompletedOps * 2
	var expectOps []string
	for i := 0; i < bigNum; i++ {
		op := solrmanapi.OpRecord{Collection: "max", Shard: strconv.Itoa(i), FinishedMs: int64(i)}
		s.AddCompletedOp(op)
		expectOps = append(expectOps, op.Key())
	}
	expectOps = expectOps[len(expectOps)-NumStoredCompletedOps:]
	sort.Sort(sort.Reverse(sort.StringSlice(expectOps))) // should come back in reverse order
	assertOps(bigNum, expectOps...)
        // A negative count also returns all values.
	assertOps(-1, expectOps...)
}

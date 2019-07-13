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

package smmodel

import (
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"fs/fsmath"
)

func TestEmptyModel(t *testing.T) {
	t.Parallel()
	m := &Model{}
	moves := m.ComputeBestMoves(1)
	if len(moves) != 0 {
		t.Errorf("Expected no moves")
	}
}

func TestTinyModel(t *testing.T) {
	t.Parallel()
	m := createTestModel(tinyModel)
	moves := m.ComputeBestMoves(1)
	if len(moves) != 0 {
		t.Errorf("Expected no moves")
	}
}

func TestSmallModel(t *testing.T) {
	t.Parallel()
	m := createTestModel(smallModel)
	moves := m.ComputeBestMoves(3)
	assertEquals(t, []string{
		`{"core":"A_shard1_0_replica2","collection":"A","shard":"shard1_0","from_node":"solr-1.node","to_node":"solr-2.node"}`,
		`{"core":"B_shard1_replica1","collection":"B","shard":"shard1","from_node":"solr-2.node","to_node":"solr-1.node"}`,
	}, moves)
}

func TestSmallModel_EvacuatingNodes(t *testing.T) {
	t.Parallel()
	m := createTestModel(smallModel, "solr-1.node")
	moves := m.ComputeBestMoves(3)
	assertEquals(t, []string{
		`{"core":"A_shard1_0_replica2","collection":"A","shard":"shard1_0","from_node":"solr-1.node","to_node":"solr-2.node"}`,
		`{"core":"A_shard1_1_replica2","collection":"A","shard":"shard1_1","from_node":"solr-1.node","to_node":"solr-2.node"}`,
	}, moves)
}

func TestDeletesExtraReplicas(t *testing.T) {
	t.Parallel()
	m := createTestModel(extraReplicaModel)
	moves := m.ComputeBestMoves(3)
	assertEquals(t, []string{
		`{"core":"A_shard1_replica1","collection":"A","shard":"shard1","from_node":"solr-2.node","to_node":"solr-1.node"}`,
	}, moves)
}

func TestCollectionBalanceModel(t *testing.T) {
	t.Parallel()
	m := createTestModel(collectionBalanceModel)
	moves := m.ComputeBestMoves(3)

	assertEquals(t, []string{
		`{"core":"A_shard1_replica1","collection":"A","shard":"shard1","from_node":"solr-1.node","to_node":"solr-2.node"}`,
	}, moves)
}

func TestLargeModel(t *testing.T) {
	t.Parallel()
	data, err := ioutil.ReadFile("large_model.txt")
	if err != nil {
		t.Fatalf("failed to read large model: %s", err)
	}

	// TODO(jh): review results
	m := createTestModel(string(data))
	moves := m.ComputeBestMoves(5)
	assertEquals(t, []string{
		`{"core":"collD_shard1_0_0_0_replica1","collection":"collD","shard":"shard1_0_0_0","from_node":"solr-1.node","to_node":"solr-6.node"}`,
		`{"core":"collD_shard1_0_0_1_replica1","collection":"collD","shard":"shard1_0_0_1","from_node":"solr-1.node","to_node":"solr-9.node"}`,
		`{"core":"collD_shard1_0_1_0_replica1","collection":"collD","shard":"shard1_0_1_0","from_node":"solr-1.node","to_node":"solr-7.node"}`,
		`{"core":"coll3F_shard1_0_0_0_replica1","collection":"coll3F","shard":"shard1_0_0_0","from_node":"solr-1.node","to_node":"solr-6.node"}`,
		`{"core":"coll8A_shard1_0_1_0_replica1","collection":"coll8A","shard":"shard1_0_1_0","from_node":"solr-4.node","to_node":"solr-6.node"}`,
	}, moves)
}

func TestLargeModel_EvacuatingNodes(t *testing.T) {
	t.Parallel()
	data, err := ioutil.ReadFile("evac_model.txt")
	if err != nil {
		t.Fatalf("failed to read evac model: %s", err)
	}

	// TODO(jh): revamp this test; I'm also not sure if the underlying evacuate nodes logic is working right at all.
	m := createTestModel(string(data), "solr-8.node", "solr-9.node")
	moves := m.ComputeBestMoves(20)

	// Should be just evacuation moves.
	assertEquals(t, []string{
		`{"core":"coll23_shard1_1_0_1_1_replica2","collection":"coll23","shard":"shard1_1_0_1_1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll23_shard1_1_0_1_0_replica2","collection":"coll23","shard":"shard1_1_0_1_0","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll7A_shard1_1_1_replica2","collection":"coll7A","shard":"shard1_1_1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll32_shard1_1_0_1_1_1_replica1","collection":"coll32","shard":"shard1_1_0_1_1_1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll13_shard1_0_1_1_replica1","collection":"coll13","shard":"shard1_0_1_1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll3AD_shard1_replica1","collection":"coll3AD","shard":"shard1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll3AC_shard1_replica2","collection":"coll3AC","shard":"shard1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll3AE_shard1_replica1","collection":"coll3AE","shard":"shard1","from_node":"solr-8.node","to_node":"solr-6.node"}`,
		`{"core":"coll15_shard1_1_1_1_1_0_replica2","collection":"coll15","shard":"shard1_1_1_1_1_0","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll32_shard1_1_1_1_0_0_replica1","collection":"coll32","shard":"shard1_1_1_1_0_0","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll2C_shard1_1_1_1_1_replica2","collection":"coll2C","shard":"shard1_1_1_1_1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll3E_shard1_1_0_1_1_replica2","collection":"coll3E","shard":"shard1_1_0_1_1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll15_shard1_1_1_1_1_1_replica2","collection":"coll15","shard":"shard1_1_1_1_1_1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll3B5_shard1_replica1","collection":"coll3B5","shard":"shard1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll3B4_shard1_replica1","collection":"coll3B4","shard":"shard1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
		`{"core":"coll3B0_shard1_replica1","collection":"coll3B0","shard":"shard1","from_node":"solr-9.node","to_node":"solr-6.node"}`,
	}, moves[:16])

	// Execute the moves.
	curModel := m
	for _, move := range moves[:16] {
		curModel = curModel.WithMove(move)
	}

	// After evacuations are done, normal moves should commence.
	moves = curModel.ComputeBestMoves(3)
	assertEquals(t, []string{
		`{"core":"collD_shard1_0_0_0_replica1","collection":"collD","shard":"shard1_0_0_0","from_node":"solr-1.node","to_node":"solr-6.node"}`,
		`{"core":"collD_shard1_0_0_1_replica1","collection":"collD","shard":"shard1_0_0_1","from_node":"solr-1.node","to_node":"solr-7.node"}`,
		`{"core":"collD_shard1_0_1_0_replica1","collection":"collD","shard":"shard1_0_1_0","from_node":"solr-1.node","to_node":"solr-10.node"}`,
	}, moves)

}

func assertEquals(t *testing.T, expected []string, actual []Move) {
	t.Helper()
	for i, c := 0, fsmath.MaxInt(len(expected), len(actual)); i < c; i++ {
		if i >= len(actual) {
			t.Errorf("at index: %d\nexpected: %s\n  actual: <nil>", i, expected[i])
		} else if i >= len(expected) {
			t.Errorf("at index: %d\nunexpected: %s", i, &actual[i])
		} else if actual[i].String() != expected[i] {
			t.Errorf("at index: %d\nexpected: %s\n  actual: %s", i, expected[i], &actual[i])
		}
	}
}

func createTestModel(data string, evacuatingNodes ...string) *Model {
	var currentNode *Node
	m := &Model{}
	seenNodeNames := map[string]bool{}
	collectionMap := make(map[string]*Collection)
	lines := strings.Split(data, "\n")
	evacuatingNodeSet := map[string]bool{}
	for _, n := range evacuatingNodes {
		evacuatingNodeSet[n] = true
	}

	for _, line := range lines {
		if strings.HasPrefix(line, "solr-") {
			parts := strings.Split(line, ",")
			if len(parts) != 2 {
				panic("unexpected split on: " + line)
			}
			name := parts[0]
			address := parts[1]
			currentNode = &Node{
				Name:       name,
				Address:    address,
				Evacuating: evacuatingNodeSet[name],
			}
			if seenNodeNames[name] {
				panic("already seen: " + name)
			}
			seenNodeNames[name] = true
			m.AddNode(currentNode)
		} else if strings.Contains(line, "_shard") {
			parts := strings.Split(line, ",")
			if len(parts) != 3 {
				panic("unexpected split on: " + line)
			}
			name := parts[0]
			_ = parts[1] // docs
			size := parts[2]

			collName := getCollName(name)
			shardName := getShardName(name)
			collection := collectionMap[collName]
			if collection == nil {
				collection = &Collection{Name: collName}
				collectionMap[collName] = collection
				m.AddCollection(collection)
			}

			core := &Core{Name: name, Collection: collName, Shard: shardName, Size: toBytes(size)}
			collection.Add(core)
			currentNode.Add(core)
			m.Add(core)
		} else {
			if len(line) > 0 {
				panic("unexpected line: " + line)
			}
		}
	}
	return m
}

func getCollName(coreName string) string {
	pos := strings.Index(coreName, "_shard")
	if pos < 1 {
		panic("expected _shard in " + coreName)
	}
	return coreName[0:pos]
}

func getShardName(coreName string) string {
	start := strings.Index(coreName, "_shard")
	end := strings.Index(coreName, "_replica")

	if start < 1 {
		panic("expected _shard in " + coreName)
	}
	if end < 1 {
		panic("expected _replica in " + coreName)
	}
	return coreName[start+1 : end]
}

func toCount(str string) int64 {
	var v float64
	var err error
	if strings.HasSuffix(str, "K") {
		v, err = strconv.ParseFloat(str[0:len(str)-1], 64)
		v *= 1024
	} else if strings.HasSuffix(str, "M") {
		v, err = strconv.ParseFloat(str[0:len(str)-1], 64)
		v *= 1024 * 1024
	} else if strings.HasSuffix(str, "G") {
		v, err = strconv.ParseFloat(str[0:len(str)-1], 64)
		v *= 1024 * 1024 * 1024
	} else {
		v, err = strconv.ParseFloat(str, 64)
	}

	if err != nil {
		panic(err)
	}
	return int64(v)
}

func toBytes(str string) int64 {
	if !strings.HasSuffix(str, "B") {
		panic("expected to end with B: " + str)
	}
	return toCount(str[0 : len(str)-1])
}

const (
	tinyModel = "" +
		"solr-1.node,1.1.1.1:8983_solr\n" +
		"A_shard1_replica2,15.0M,12.0GB\n" +
		"solr-2.node,2.2.2.2:8983_solr\n" +
		""

	smallModel = "" +
		"solr-1.node,1.1.1.1:8983_solr\n" +
		"A_shard1_0_replica2,15.0M,12.0GB\n" +
		"A_shard1_1_replica2,12.9M,10.1GB\n" +
		"solr-2.node,2.2.2.2:8983_solr\n" +
		"B_shard1_replica1,1.8M,1.4GB\n" +
		""

	// Should delete the extra replica from solr-2
	extraReplicaModel = "" +
		"solr-1.node,1.1.1.1:8983_solr\n" +
		"A_shard1_replica2,15.0M,12.0GB\n" +
		"solr-2.node,2.2.2.2:8983_solr\n" +
		"A_shard1_replica1,15.0M,12.0GB\n" +
		"B_shard1_replica1,1.8M,1.4GB\n" +
		""

	// Should move A to solr-2 instead of B.
	collectionBalanceModel = "" +
		"solr-1.node,1.1.1.1:8983_solr\n" +
		"A_shard1_replica1,15.5M,11.5GB\n" +
		"B_shard1_replica1,5.0M,5.0GB\n" +
		"solr-2.node,2.2.2.2:8983_solr\n" +
		"B_shard2_replica1,1.0M,1.0GB\n" +
		""
)

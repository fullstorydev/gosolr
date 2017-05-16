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
)

func TestEmptyModel(t *testing.T) {
	t.Parallel()
	m := &Model{}
	mPrime, move := m.ComputeNextMove(1, nil)
	if m != mPrime {
		t.Errorf("Expected no moves")
	}
	assertNil(t, move)
}

func TestTinyModel(t *testing.T) {
	t.Parallel()
	m := createTestModel(tinyModel)
	mPrime, move := m.ComputeNextMove(1, nil)
	if m != mPrime {
		t.Errorf("Expected no moves")
	}
	assertNil(t, move)
}

func TestSmallModel(t *testing.T) {
	t.Parallel()
	baseModel := createTestModel(smallModel)

	var moves []*Move
	immobileCores := map[string]bool{}
	m := baseModel
	for i := 0; i < 3; i++ {
		mPrime, move := m.ComputeNextMove(1, immobileCores)
		if m == mPrime {
			assertNil(t, move)
			break
		}
		assertNotNil(t, move)
		moves = append(moves, move)
		immobileCores[move.Core.Name] = true
		m = mPrime
	}

	assertEquals(t, []string{
		`{"core":"A_shard1_1_replica2","collection":"A","shard":"shard1_1","from_node":"solr-1.node","to_node":"solr-2.node"}`,
	}, moves)
}

func TestDeletesExtraReplicas(t *testing.T) {
	t.Parallel()
	baseModel := createTestModel(extraReplicaModel)

	var moves []*Move
	immobileCores := map[string]bool{}
	m := baseModel
	for i := 0; i < 3; i++ {
		mPrime, move := m.ComputeNextMove(1, immobileCores)
		if m == mPrime {
			assertNil(t, move)
			break
		}
		assertNotNil(t, move)
		moves = append(moves, move)
		immobileCores[move.Core.Name] = true
		m = mPrime
	}

	assertEquals(t, []string{
		`{"core":"A_shard1_replica1","collection":"A","shard":"shard1","from_node":"solr-2.node","to_node":"solr-1.node"}`,
	}, moves)
}

func TestCollectionBalanceModel(t *testing.T) {
	t.Parallel()
	baseModel := createTestModel(collectionBalanceModel)

	var moves []*Move
	immobileCores := map[string]bool{}
	m := baseModel
	for i := 0; i < 3; i++ {
		mPrime, move := m.ComputeNextMove(1, immobileCores)
		if m == mPrime {
			assertNil(t, move)
			break
		}
		assertNotNil(t, move)
		moves = append(moves, move)
		immobileCores[move.Core.Name] = true
		m = mPrime
	}

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
	baseModel := createTestModel(string(data))

	var moves []*Move
	immobileCores := map[string]bool{}
	m := baseModel
	for i := 0; i < 3; i++ {
		mPrime, move := m.ComputeNextMove(1, immobileCores)
		if m == mPrime {
			assertNil(t, move)
			break
		}
		assertNotNil(t, move)
		moves = append(moves, move)
		immobileCores[move.Core.Name] = true
		m = mPrime
	}

	assertEquals(t, []string{
		`{"core":"coll12_shard1_0_replica1","collection":"coll12","shard":"shard1_0","from_node":"solr-1.node","to_node":"solr-9.node"}`,
		`{"core":"coll3_shard1_0_replica1","collection":"coll3","shard":"shard1_0","from_node":"solr-1.node","to_node":"solr-9.node"}`,
		`{"core":"coll63_shard1_0_replica1","collection":"coll63","shard":"shard1_0","from_node":"solr-2.node","to_node":"solr-10.node"}`,
	}, moves)
}

func TestLargeModel_EvacuatingNodes(t *testing.T) {
	t.Parallel()
	data, err := ioutil.ReadFile("evac_model.txt")
	if err != nil {
		t.Fatalf("failed to read evac model: %s", err)
	}
	baseModel := createTestModel(string(data), "solr-8.node", "solr-9.node")

	immobileCores := map[string]bool{}
	m := baseModel
	for {
		// all moves should be from solr-8 or solr-9 until they are evacuated
		mPrime, move := m.ComputeNextMove(1, immobileCores)
		assertNotNil(t, move)
		if move.FromNode.Name != "solr-8.node" && move.FromNode.Name != "solr-9.node" {
			// we should now be done evacuating these two nodes
			for _, n := range m.Nodes {
				if n.Name == "solr-8.node" || n.Name == "solr-9.node" {
					if n.coreCount > 0 {
						t.Errorf("Computed move from %s (%f -> %f) even though evacuating node %s still has %d cores", move.FromNode.Name, m.cost, mPrime.cost, n.Name, n.coreCount)
					}
				}
			}
			break
		}
		immobileCores[move.Core.Name] = true
		m = mPrime
	}
}

func assertEquals(t *testing.T, expected []string, actual []*Move) {
	if len(expected) != len(actual) {
		t.Errorf("expected: %v, actual: %v", expected, actual)
		return
	}
	for i := range expected {
		if actual[i].String() != expected[i] {
			t.Errorf("at index: %v, expected: %v, actual: %v", i, expected[i], actual[i])
		}
	}
}

func assertNil(t *testing.T, move *Move) {
	if move != nil {
		t.Errorf("expect nil, got: %v", move)
	}
}

func assertNotNil(t *testing.T, move *Move) {
	if move == nil {
		t.Errorf("expect not-nil")
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
			docs := parts[1]
			size := parts[2]

			collName := getCollName(name)
			shardName := getShardName(name)
			collection := collectionMap[collName]
			if collection == nil {
				collection = &Collection{Name: collName}
				collectionMap[collName] = collection
				m.Collections = append(m.Collections, collection)
			}

			core := &Core{Name: name, Collection: collName, Shard: shardName, Docs: toCount(docs), Size: toBytes(size)}
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

func toCount(str string) float64 {
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
	return v
}

func toBytes(str string) float64 {
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

	// Should move A to solr-2 for better collection balance (even if worse cluster balance)
	collectionBalanceModel = "" +
		"solr-1.node,1.1.1.1:8983_solr\n" +
		"A_shard1_replica1,15.5M,11.5GB\n" +
		"B_shard1_replica1,15.0M,11.0GB\n" +
		"solr-2.node,2.2.2.2:8983_solr\n" +
		"B_shard2_replica1,1.0M,1.0GB\n" +
		""
)

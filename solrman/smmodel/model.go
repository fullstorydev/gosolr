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
	"fmt"
	"sort"
)

type Model struct {
	Nodes       []*Node       `json:"nodes"`
	Collections []*Collection `json:"collections"`
	Cores       []*Core

	MaxSizePerNode int64 // maximum allowed size on a
}

func (m *Model) Add(core *Core) {
	core.id = coreId(len(m.Cores))
	m.Cores = append(m.Cores, core)
}

func (m *Model) AddNode(node *Node) {
	node.id = nodeId(len(m.Nodes))
	m.Nodes = append(m.Nodes, node)
}

func (m *Model) AddCollection(coll *Collection) {
	coll.id = collectionId(len(m.Collections))
	m.Collections = append(m.Collections, coll)
}

// Return a new model with the given move applied
func (m *Model) WithMove(move Move) *Model {
	// make sure we're using objects from this model
	toNode := m.Nodes[move.ToNode.id]
	fromNode := m.Nodes[move.FromNode.id]
	core := m.Cores[move.Core.id]
	coll := m.Collections[core.collectionId]

	if core.Collection != coll.Name {
		panic(fmt.Sprintf("core.Collection(%s) != coll.Name(%s)", core.Collection, coll.Name))
	}

	if !fromNode.Contains(core) {
		panic("fromNode does not contain core")
	}
	if toNode.Contains(core) {
		return m
	}

	completeExistingMove := func() bool {
		// See if any cores exist on the target node with the same shardname.
		shardName := core.shardName()
		for _, c := range coll.cores {
			if toNode.Contains(c) && c.shardName() == shardName {
				return true
			}
		}
		return false
	}()

	var newCore *Core
	var newColl *Collection
	if completeExistingMove {
		newCore = nil
		newColl = coll.Without(core)
	} else {
		coreCopy := *core
		newCore = &coreCopy
		newCore.nodeId = toNode.id
		newColl = coll.Replace(core, newCore)
	}

	// Update the node list to reflect the move.
	newNodes := make([]*Node, len(m.Nodes))
	copy(newNodes, m.Nodes)
	newNodes[fromNode.id] = fromNode.Without(core)
	if !completeExistingMove {
		// Replace with a node that includes this core, unless it was a move completion
		newNodes[toNode.id] = toNode.With(newCore)
	}

	// Update the collection list to reflect the move
	newCollections := make([]*Collection, len(m.Collections))
	copy(newCollections, m.Collections)
	newCollections[core.collectionId] = newColl

	// Update the core list to reflect the move.
	newCores := make([]*Core, len(m.Cores))
	copy(newCores, m.Cores)
	newCores[core.id] = newCore

	return &Model{
		Nodes:       newNodes,
		Collections: newCollections,
		Cores:       newCores,
	}
}

func (m *Model) computeNextMove(immobileCores []bool) *Move {
	if len(m.Nodes) < 2 || len(m.Cores) < 1 {
		// can't balance a single-node or empty cluster
		return nil
	}

	// Compute balance info.
	balanceInfo := make([]balanceInfo, 0, len(m.Collections))
	for _, c := range m.Collections {
		balanceInfo = append(balanceInfo, c.balance(len(m.Nodes)))
	}
	sort.Slice(balanceInfo, func(i, j int) bool {
		return balanceInfo[i].score > balanceInfo[j].score
	})

	// Step 1: remove duplicate replicas
	for _, bi := range balanceInfo {
		coll := bi.coll
		cores := make([]*Core, len(coll.cores))
		copy(cores, coll.cores)
		sort.Slice(cores, func(i, j int) bool {
			return cores[i].Shard < cores[j].Shard
		})

		for i, core := range cores {
			if i == 0 {
				continue
			}
			last := cores[i-1]
			if core.Shard != last.Shard {
				continue
			}

			// found a dup
			from := m.Nodes[core.nodeId]
			to := m.Nodes[last.nodeId]

			// Move from the node that:
			// - has more cores in the collection
			// - is on the larger node
			if bi.coresPerNode[to.id] > bi.coresPerNode[from.id] || to.Size > from.Size {
				from, to = to, from
				core = last
			}

			return &Move{
				Core:     core,
				FromNode: from,
				ToNode:   to,
			}
		}
	}

	nodesBySize := make([]*Node, len(m.Nodes))
	copy(nodesBySize, m.Nodes)
	sort.Slice(nodesBySize, func(i, j int) bool {
		return nodesBySize[i].Size < nodesBySize[j].Size
	})

	// Try to move a core from the given node.
	tryMoveCoreFrom := func(source *Node, force bool) *Move {
		for _, target := range nodesBySize {
			if target == source {
				continue
			}

			// Move the largest core that doesn't violate constraints.
			var candidates []*Core
			for i, c := range m.Cores {
				if c == nil || immobileCores[i] {
					continue
				}
				if source.Contains(c) {
					candidates = append(candidates, c)
				}
			}

			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].Size > candidates[j].Size
			})

			if force {
				// ignore constrants
				return &Move{
					Core:     candidates[0],
					FromNode: source,
					ToNode:   target,
				}
			}

			if 9*source.Size < 10*target.Size {
				// if the target node is >=90% of the source node, don't bother
				return nil
			}

			for _, core := range candidates {
				// Make sure moving this core won't violate collection balance.
				coll := m.Collections[core.collectionId]
				if coll.balanceInfo.coresPerNode[target.id] >= coll.balanceInfo.maxCoresPerNode {
					continue
				}

				// Don't bother moving this core if the target node would become bigger than the source node.
				if target.Size+core.Size >= source.Size {
					continue
				}

				// Found a good candidate.
				return &Move{
					Core:     core,
					FromNode: source,
					ToNode:   target,
				}
			}
		}
		return nil
	}

	// Step 2: balance collections next, respecting node max size.
	for _, bi := range balanceInfo {
		if bi.score == 0 {
			continue
		}

		// Find a suitable source node with the greatest number of collection replicas and the largest node size
		sources := make([]*Node, len(m.Nodes))
		copy(sources, m.Nodes)
		sort.Slice(sources, func(i, j int) bool {
			a := sources[i]
			b := sources[j]
			if bi.coresPerNode[a.id] != bi.coresPerNode[b.id] {
				return bi.coresPerNode[a.id] > bi.coresPerNode[b.id]
			}
			return a.Size > b.Size
		})
		fromNode := sources[0]
		var core *Core

		// Pick a random core to move
		for _, c := range bi.coll.cores {
			if immobileCores[c.id] {
				continue
			}
			if fromNode.Contains(c) {
				core = c
				break
			}
		}

		if core == nil {
			// all of the cores must be immobile
			continue
		}

		// Find a suitable target node with the least number of collection replicas and the smallest node size
		targets := make([]*Node, len(m.Nodes))
		copy(targets, m.Nodes)
		sort.Slice(targets, func(i, j int) bool {
			a := targets[i]
			b := targets[j]
			if bi.coresPerNode[a.id] != bi.coresPerNode[b.id] {
				return bi.coresPerNode[a.id] < bi.coresPerNode[b.id]
			}
			return a.Size < b.Size
		})

		for _, target := range targets {
			if bi.coresPerNode[target.id] >= bi.maxCoresPerNode {
				// no good choices
				break
			}
			if m.MaxSizePerNode > 0 && target.Size >= m.MaxSizePerNode {
				// too much data already on this node
				continue
			}

			// Found a good choice.
			return &Move{
				Core:     core,
				FromNode: fromNode,
				ToNode:   target,
			}
		}
	}

	// Step 3: balance nodes next, respecting collection balance.
	// Take the largest core from the largest node, and move it to the smallest node, provided we don't violate constraints.
	if len(nodesBySize) > 1 {
		biggest := nodesBySize[len(nodesBySize)-1]
		m := tryMoveCoreFrom(biggest, false)
		if m != nil {
			return m
		}
	}

	return nil
}

func (m *Model) ComputeBestMoves(count int) []Move {
	var moves []Move

	curModel := m
	immobileCores := make([]bool, len(m.Cores)) // cores that have already moved
	for i := 0; i < count; i++ {
		move := curModel.computeNextMove(immobileCores)
		if move == nil {
			// no good moves
			break
		}
		immobileCores[move.Core.id] = true
		moves = append(moves, *move)
		curModel = curModel.WithMove(*move)
	}

	return moves
}

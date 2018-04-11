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
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

const debug = 0 // 0 means no debug output; 3 is maximum debug output

type Model struct {
	Docs        float64       `json:"docs"`
	Size        float64       `json:"size"` // in bytes
	Nodes       []*Node       `json:"nodes"`
	Collections []*Collection `json:"collections"`
	cores       []*Core

	cost float64 // cached cost
}

func (m *Model) FindCore(coreName string) *Core {
	for _, core := range m.cores {
		if core.Name == coreName {
			return core
		}
	}
	return nil
}

func (m *Model) FindNode(nodeName string) *Node {
	for _, node := range m.Nodes {
		if node.Name == nodeName {
			return node
		}
	}
	return nil
}

func (m *Model) Add(core *Core) {
	m.Docs += core.Docs
	m.Size += core.Size
	m.cores = append(m.cores, core)
}

func (m *Model) AddNode(node *Node) {
	node.id = len(m.Nodes)
	m.Nodes = append(m.Nodes, node)
}

// Return a new model with the given move applied
func (m *Model) WithMove(core *Core, toNode *Node) *Model {
	if toNode.Contains(core) {
		return m
	}

	completeExistingMove := func() bool {
		// See if any cores exist on the target node with the same shardname.
		shardName := core.shardName()
		for _, coll := range m.Collections {
			if coll.Name == core.Collection {
				for _, c := range coll.cores {
					if toNode.Contains(c) && c.shardName() == shardName {
						return true
					}
				}
				return false
			}
		}
		panic(fmt.Sprintf("could not find collection %s for core %s in model", core.Collection, core.Name))
	}()

	var newCore *Core
	if !completeExistingMove {
		coreCopy := *core
		newCore = &coreCopy
		newCore.nodeId = toNode.id
	}

	// Update the contents of the nodes to reflect the move.
	newNodes := make([]*Node, len(m.Nodes))
	for i, node := range m.Nodes {
		if node.Contains(core) {
			// Replace with a node that lacks this core
			newNodes[i] = node.Without(core)
		} else if node == toNode {
			if completeExistingMove {
				// Do nothing; the target node already contains a replica core.
				newNodes[i] = node
			} else {
				// Replace with a node that includes this core
				newNodes[i] = node.With(newCore)
			}
		} else {
			// Do nothing.
			newNodes[i] = node
		}
	}

	// Update the collections to reflect the move
	newCollections := make([]*Collection, len(m.Collections))
	for i, c := range m.Collections {
		if c.Name == core.Collection {
			if completeExistingMove {
				newCollections[i] = c.Without(core)
			} else {
				newCollections[i] = c.Replace(core, newCore)
			}
		} else {
			newCollections[i] = c
		}
	}

	// Update the core list to reflect the move.
	newCores := make([]*Core, 0, len(m.cores))
	for _, c := range m.cores {
		if c == core {
			if !completeExistingMove {
				newCores = append(newCores, newCore)
			}
		} else {
			newCores = append(newCores, c)
		}
	}

	// Subtle: do NOT adjust m.Docs / m.Size for a core deletion. Otherwise deletions would score badly.
	return &Model{
		Nodes:       newNodes,
		Collections: newCollections,
		cores:       newCores,
		Docs:        m.Docs,
		Size:        m.Size,
	}
}

type permutation struct {
	cost float64
	move Move
}

func (m *Model) computeNextMoveShard(mustEvacuate bool, shard int, shardCount int, chanPerm chan<- permutation) {
	// Try moving every core to every other node, find the best score.
	count := 0
	for _, core := range m.cores {
		var fromNode *Node
		for _, node := range m.Nodes {
			if node.Contains(core) {
				fromNode = node
				break
			}
		}
		if core.Docs < 500000 && !fromNode.Evacuating {
			// HACK, optimization: don't even consider move cores with less than 500K docs
			continue
		}
		if mustEvacuate && !fromNode.Evacuating {
			// skip moves off of nodes that aren't trying to evacuate their contents
			continue
		}
		if fromNode == nil {
			panic("cannot find owner node for: " + core.Name)
		}
		for _, toNode := range m.Nodes {
			if toNode == fromNode {
				continue
			}
			if toNode.Evacuating {
				// don't bother considering a move that adds a core to a node that is being evacuated
				continue
			}

			count += 1
			if count%shardCount == shard {
				mPrime := m.WithMove(core, toNode)
				perm := permutation{
					cost: mPrime.Cost(),
					move: Move{Core: core, FromNode: fromNode, ToNode: toNode},
				}
				if debug >= 1 {
					fmt.Printf("move cost: %f: %s\n", perm.cost, perm.move)
				}
				chanPerm <- perm
			}
		}
	}
}

func (m *Model) ComputeBestMoves(numCPU int, count int) []Move {
	baseCost := m.Cost()
	if debug >= 1 {
		fmt.Printf("base model cost: %f\n", baseCost)
	}

	// check to see if our next move must be an evacuation
	mustEvacuate := false
	for _, n := range m.Nodes {
		if n.Evacuating && n.coreCount > 0 {
			mustEvacuate = true
			break
		}
	}
	if debug >= 1 {
		fmt.Printf("Move must remove core from evacuating node? %v\n", mustEvacuate)
	}

	// Try moving every core to every other node, compute scores.
	chanPerm := make(chan permutation)
	var wg sync.WaitGroup
	for i := 0; i < numCPU; i += 1 {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			m.computeNextMoveShard(mustEvacuate, shard, numCPU, chanPerm)
		}(i)
	}

	go func() {
		wg.Wait()
		close(chanPerm)
	}()

	var allPerms []permutation
	for perm := range chanPerm {
		if perm.cost < baseCost || perm.move.FromNode.Evacuating {
			allPerms = append(allPerms, perm)
		}
	}

	sort.Slice(allPerms, func(i, j int) bool {
		return allPerms[i].cost < allPerms[j].cost
	})

	// Now that we have all moves in score order, greedily attempt to apply each move in order
	// if it improves the current score, until we run out of moves or find the number requested.
	var moves []Move
	curModel := m
	curCost := baseCost
	immobileCores := map[string]bool{} // cores that have already moved
	for _, p := range allPerms {
		move := p.move

		if immobileCores[move.Core.Name] {
			continue
		}

		// We have to re-resolve the core and node in the current model.
		core := curModel.FindCore(move.Core.Name)
		node := curModel.FindNode(move.ToNode.Name)
		newModel := curModel.WithMove(core, node)
		newCost := newModel.Cost()

		// Skip any moves that would increase the cost; this happens because moves were originally considered in isolation.
		if newCost < curCost {
			curModel = newModel
			curCost = newCost
			moves = append(moves, move)
			immobileCores[core.Name] = true
		}
		if len(moves) >= count {
			// Found enough
			return moves
		}
	}
	return moves
}

type Node struct {
	Name       string  `json:"name"`
	Address    string  `json:"address"`
	Evacuating bool    `json:"evacuating"`
	Docs       float64 `json:"docs"`
	Size       float64 `json:"size"` // in bytes
	coreCount  int
	id         int // unique id of this node for internal tracking

	cost float64 // cached cost
}

func (n *Node) Add(core *Core) {
	n.Docs += core.Docs
	n.Size += core.Size
	n.coreCount += 1
	core.nodeId = n.id
}

func (n *Node) Contains(core *Core) bool {
	return core.nodeId == n.id
}

func (n *Node) With(core *Core) *Node {
	if !n.Contains(core) {
		panic(fmt.Sprintf("core %s assigned to %d cannot be added to %d", core.Name, core.nodeId, n.id))
	}

	nCopy := *n
	nCopy.cost = 0

	nCopy.Docs += core.Docs
	nCopy.Size += core.Size
	nCopy.coreCount++
	return &nCopy
}

func (n *Node) Without(core *Core) *Node {
	if !n.Contains(core) {
		return n
	}

	nCopy := *n
	nCopy.cost = 0

	nCopy.Docs -= core.Docs
	nCopy.Size -= core.Size
	nCopy.coreCount--
	return &nCopy
}

type Collection struct {
	Name  string  `json:"name"`
	Docs  float64 `json:"docs"`
	Size  float64 `json:"size"` // in bytes
	cores []*Core

	cost float64 // cached cost
}

func (c *Collection) Add(core *Core) {
	c.Docs += core.Docs
	c.Size += core.Size
	c.cores = append(c.cores, core)
}

func (c *Collection) Without(core *Core) *Collection {
	found := false
	newCores := make([]*Core, 0, len(c.cores))
	for _, c := range c.cores {
		if c == core {
			found = true
		} else {
			newCores = append(newCores, c)
		}
	}

	if !found {
		panic(fmt.Sprintf("core %s not found in collection %s", core.Name, c.Name))
	}

	return &Collection{
		Name:  c.Name,
		Docs:  c.Docs - core.Docs,
		Size:  c.Size - core.Size,
		cores: newCores,
	}
}

func (c *Collection) Replace(core *Core, newCore *Core) *Collection {
	found := false
	newCores := make([]*Core, 0, len(c.cores))
	for _, c := range c.cores {
		if c == core {
			found = true
			newCores = append(newCores, newCore)
		} else {
			newCores = append(newCores, c)
		}
	}

	if !found {
		panic(fmt.Sprintf("core %s not found in collection %s", core.Name, c.Name))
	}

	return &Collection{
		Name:  c.Name,
		Docs:  c.Docs - core.Docs + newCore.Docs,
		Size:  c.Size - core.Size + newCore.Size,
		cores: newCores,
	}
}

type Core struct {
	Name       string  `json:"name"`
	Collection string  `json:"collection"`
	Shard      string  `json:"shard"`
	Docs       float64 `json:"docs"`
	Size       float64 `json:"size"` // in bytes
	nodeId     int     // the node I currently belong to
}

func (c *Core) shardName() string {
	return c.Collection + "_" + c.Shard
}

type Move struct {
	Core     *Core
	FromNode *Node
	ToNode   *Node
}

func (m *Move) String() string {
	result, _ := json.Marshal(m.AsJson())
	return string(result)
}

func (m *Move) AsJson() interface{} {
	return &struct {
		Name       string `json:"core"`
		Collection string `json:"collection"`
		Shard      string `json:"shard"`
		FromNode   string `json:"from_node"`
		ToNode     string `json:"to_node"`
	}{
		Name:       m.Core.Name,
		Collection: m.Core.Collection,
		Shard:      m.Core.Shard,
		FromNode:   m.FromNode.Name,
		ToNode:     m.ToNode.Name,
	}
}

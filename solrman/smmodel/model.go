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
)

type Model struct {
	Docs        float64       `json:"docs"`
	Size        float64       `json:"size"` // in bytes
	Nodes       []*Node       `json:"nodes"`
	Collections []*Collection `json:"collections"`
	cores       []*Core
}

func (m *Model) Add(core *Core) {
	m.Docs += core.Docs
	m.Size += core.Size
	m.cores = append(m.cores, core)
}

// Return a new model with the given move applied
func (m *Model) WithMove(core *Core, toNode *Node) *Model {
	newCollections := m.Collections
	completeExistingMove := toNode.ContainsShard(core.shardName())

	if completeExistingMove {
		// The target node already contains a copy of this shard; this could be an incomplete move.
		// Update the model to reflect deleting this core; i.e. if this move finishes.
		// Copy-on-modify a new collections list where this core has been deleted from its collection.
		newCollections = make([]*Collection, len(m.Collections))
		for i, c := range m.Collections {
			if c.Name == core.Collection {
				newCollections[i] = c.Without(core)
			} else {
				newCollections[i] = c
			}
		}
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
				newNodes[i] = node.With(core)
			}
		} else {
			// Do nothing.
			newNodes[i] = node
		}
	}

	return &Model{
		Nodes:       newNodes,
		Collections: newCollections,
		cores:       m.cores,
		Docs:        m.Docs,
		Size:        m.Size,
	}
}

type permutation struct {
	score float64
	model *Model
	move  *Move
}

func (m *Model) computeNextMoveShard(immobileCores map[string]bool, shard int, shardCount int, c chan *permutation) {
	// Try moving every core to every other node, find the best score.
	count := 0
	for _, core := range m.cores {
		if immobileCores[core.Name] {
			continue
		}
		if core.Docs < 500000 {
			// HACK, optimization: don't even consider move cores with less than 500K docs
			continue
		}
		var fromNode *Node
		for _, node := range m.Nodes {
			if node.Contains(core) {
				fromNode = node
				break
			}
		}
		if fromNode == nil {
			panic("cannot find owner node for: " + core.Name)
		}
		for _, toNode := range m.Nodes {
			if toNode == fromNode {
				continue
			}
			count += 1
			if count%shardCount == shard {
				mPrime := m.WithMove(core, toNode)
				c <- &permutation{
					score: mPrime.Score(),
					model: mPrime,
					move:  &Move{Core: core, FromNode: fromNode, ToNode: toNode},
				}
			}
		}
	}
	c <- nil // sentinel termination value
}

func (m *Model) countPerms(immobileCores map[string]bool) int {
	// Try moving every core to every other node, find the best score.
	count := 0
	for _, core := range m.cores {
		if immobileCores[core.Name] {
			continue
		}
		if core.Docs < 500000 {
			// HACK, optimization: don't even consider move cores with less than 500K docs
			continue
		}
		var fromNode *Node
		for _, node := range m.Nodes {
			if node.Contains(core) {
				fromNode = node
				break
			}
		}
		if fromNode == nil {
			panic("cannot find owner node for: " + core.Name)
		}
		for _, toNode := range m.Nodes {
			if toNode == fromNode {
				continue
			}
			count += 1
		}
	}
	return count
}

func (m *Model) ComputeNextMove(numCPU int, immobileCores map[string]bool) (*Model, *Move) {
	best := &permutation{
		score: m.Score(),
		model: m,
		move:  nil,
	}

	// Try moving every core to every other node, find the best score.
	c := make(chan *permutation)
	for i := 0; i < numCPU; i += 1 {
		go m.computeNextMoveShard(immobileCores, i, numCPU, c)
	}

	count := m.countPerms(immobileCores) + numCPU // count + 1 sentinel per goroutine
	for ; count > 0; count -= 1 {
		move := <-c
		if move == nil {
			continue // sentinel
		}
		if move.score > best.score {
			best = move
		}
	}

	return best.model, best.move
}

// Create a composite score fot the model, based on hard-coded rules.
// Higher score is better.
func (m *Model) Score() float64 {
	weighted := make([]float64, len(Rules))
	for i, rule := range Rules {
		score := rule.Score(m)
		weighted[i] = score * rule.GetWeight()
	}
	var score float64
	for _, w := range weighted {
		score += w
	}
	return score
}

type Node struct {
	Name     string           `json:"name"`
	Address  string           `json:"address"`
	Docs     float64          `json:"docs"`
	Size     float64          `json:"size"`  // in bytes
	CoreMap  map[string]*Core `json:"cores"` // index all the cores on this node by coreName
	shardMap map[string]*Core // index all the cores on this node by shardName
}

func (n *Node) Add(core *Core) {
	n.Docs += core.Docs
	n.Size += core.Size
	if n.CoreMap == nil {
		n.CoreMap = make(map[string]*Core)
	}
	if n.shardMap == nil {
		n.shardMap = make(map[string]*Core)
	}
	n.CoreMap[core.Name] = core
	n.shardMap[core.shardName()] = core
}

func (n *Node) Contains(core *Core) bool {
	val, ok := n.CoreMap[core.Name]
	return ok && val == core
}

func (n *Node) ContainsShard(shardName string) bool {
	_, ok := n.shardMap[shardName]
	return ok
}

func (n *Node) With(core *Core) *Node {
	if n.Contains(core) {
		return n
	}

	// Copy the core map & shard map, everything else is immutable
	coreMap := make(map[string]*Core, len(n.CoreMap)+1)
	for k, v := range n.CoreMap {
		coreMap[k] = v
	}
	coreMap[core.Name] = core

	shardMap := make(map[string]*Core, len(n.shardMap)+1)
	for k, v := range n.shardMap {
		shardMap[k] = v
	}
	shardMap[core.shardName()] = core

	return &Node{
		Name:     n.Name,
		Address:  n.Address,
		CoreMap:  coreMap,
		Docs:     n.Docs + core.Docs,
		Size:     n.Size + core.Size,
		shardMap: shardMap,
	}
}

func (n *Node) Without(core *Core) *Node {
	if !n.Contains(core) {
		return n
	}

	// Copy the core map & shard map, everything else is immutable
	coreMap := make(map[string]*Core, len(n.CoreMap)+1)
	for k, v := range n.CoreMap {
		coreMap[k] = v
	}
	delete(coreMap, core.Name)

	shardMap := make(map[string]*Core, len(n.shardMap)+1)
	for k, v := range n.shardMap {
		shardMap[k] = v
	}
	delete(shardMap, core.shardName())

	return &Node{
		Name:     n.Name,
		Address:  n.Address,
		CoreMap:  coreMap,
		Docs:     n.Docs - core.Docs,
		Size:     n.Size - core.Size,
		shardMap: shardMap,
	}
}

type Collection struct {
	Name  string  `json:"name"`
	Docs  float64 `json:"docs"`
	Size  float64 `json:"size"` // in bytes
	cores []*Core
}

func (c *Collection) Add(core *Core) {
	c.Docs += core.Docs
	c.Size += core.Size
	c.cores = append(c.cores, core)
}

func (c *Collection) Without(core *Core) *Collection {
	found := -1
	for i, c := range c.cores {
		if c == core {
			found = i
		}

	}
	if found < 0 {
		return c
	}

	newCores := make([]*Core, 0, len(c.cores)-1)
	newCores = append(newCores, c.cores[0:found]...)
	newCores = append(newCores, c.cores[found+1:len(c.cores)]...)

	return &Collection{
		Name:  c.Name,
		Docs:  c.Docs - core.Docs,
		Size:  c.Size - core.Size,
		cores: newCores,
	}
}

type Core struct {
	Name       string  `json:"name"`
	Collection string  `json:"collection"`
	Shard      string  `json:"shard"`
	Docs       float64 `json:"docs"`
	Size       float64 `json:"size"` // in bytes
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

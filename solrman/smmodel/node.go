// Copyright 2019 FullStory, Inc.
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

import "fmt"

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

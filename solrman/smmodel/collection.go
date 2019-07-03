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

// unique id of a core for internal tracking, 0-based and contiguous so we can store in slices by id
type collectionId int

type Collection struct {
	Name string `json:"name"`

	id    collectionId
	cores []*Core

	// cached balance info
	balanceInfo *balanceInfo
}

func (c *Collection) Add(core *Core) {
	core.collectionId = c.id
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

	cCopy := *c
	cCopy.cores = newCores
	cCopy.balanceInfo = nil
	return &cCopy
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

	cCopy := *c
	cCopy.cores = newCores
	cCopy.balanceInfo = nil
	return &cCopy

}

type balanceInfo struct {
	maxCoresPerNode int
	coresPerNode    []int
	score           int64
	coll            *Collection
}

func (c *Collection) balance(nodeCount int) balanceInfo {
	if c.balanceInfo == nil {
		// integral math: we want e.g. 12/12 = 1 but 13/12 = 2; so
		// (12-1)/12 = 0+1, (13-1)/12 = 1+1
		maxCoresPerNode := (len(c.cores)-1)/nodeCount + 1

		coresPerNode := make([]int, nodeCount)
		for _, core := range c.cores {
			coresPerNode[core.nodeId]++
		}

		// sum of squares, 10 extra cores on one machine is way worse than 1 extra core on 10 machines.
		var score int64
		for _, v := range coresPerNode {
			if v > maxCoresPerNode {
				score += int64((v - maxCoresPerNode) * (v - maxCoresPerNode))
			}
		}

		c.balanceInfo = &balanceInfo{
			maxCoresPerNode: maxCoresPerNode,
			coresPerNode:    coresPerNode,
			score:           score,
			coll:            c,
		}
	}
	return *c.balanceInfo
}

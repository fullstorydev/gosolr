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

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

// unique id of a core for internal tracking, 0-based and contiguous so we can store in slices by id
type coreId int

type Core struct {
	Name       string `json:"name"`
	Collection string `json:"collection"`
	Shard      string `json:"shard"`
	Size       int64  `json:"size"` // in bytes

	id           coreId
	collectionId collectionId // the collection i belong to
	nodeId       nodeId       // the node I currently belong to
}

func (c *Core) shardName() string {
	return c.Collection + "_" + c.Shard
}

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

package solrmonitor

import (
	"fmt"
	"strconv"
)

type CollectionState struct {
	Shards           map[string]*ShardState `json:"shards"`           // map from shard name to shard state
	Router           Router                 `json:"router"`           // e.g. {"name":"compositeId"}
	MaxShardsPerNode string                 `json:"maxShardsPerNode"` // e.g. "1" (yes, these are strings, not numbers)
	AutoAddReplicas  string                 `json:"autoAddReplicas"`  // e.g. "false" (yes, these are strings, not bools)
	PerReplicaState  interface{}            `json:"perReplicaState"`  // whether collection keeps state for each replica separately
	// These following fields are set manually, not from state.json in Zookeeper.

	// ConfigName indicates the name of the node in solr/configs (in ZK) that this collection uses.
	// This value is set from the root zk node for the collection.
	ConfigName string `json:"configName,omitempty"`
	// ZkNodeVersion indicates the ZK node version this state snapshot represents.
	ZkNodeVersion int32 `json:"znodeVersion,omitempty"`
}

func (cs *CollectionState) String() string {
	return fmt.Sprintf("CollectionState\n{Shards:%+v}\n", cs.Shards)
}

func (cs *CollectionState) IsPRSEnabled() bool {
	switch v := cs.PerReplicaState.(type) {
	case bool:
		return v
	case string:
		boolVal, err := strconv.ParseBool(v)
		if err == nil {
			return boolVal
		}
	}
	return true //default as true
}

// zkCollectionState is used to parse top level collection zk nodes.
type zkCollectionState struct {
	// ConfigName indicates the name of the node in solr/configs (in ZK) that this collection uses.
	ConfigName string `json:"configName,omitempty"`
}

type Router struct {
	Name string `json:"name"` // e.g. "compositeId"
}

type PerReplicaState struct {
	// name of the replica
	Name string `json:"name"`
	// replica's state version
	Version int32 `json:"version"`
	// is replica active
	State string `json:"state"`
	// If "true", this replica is the shard leader
	Leader string `json:"leader,omitempty"`
}

func (prs *PerReplicaState) IsActive() bool {
	return prs.State == "active"
}

func (prs *PerReplicaState) IsLeader() bool {
	return prs.Leader == "true"
}

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

import "fmt"

type CollectionState struct {
	Shards            map[string]*ShardState `json:"shards"`            // map from shard name to shard state
	ReplicationFactor string                 `json:"replicationFactor"` // e.g. "1" (yes, these are strings, not numbers)
	Router            Router                 `json:"router"`            // e.g. {"name":"compositeId"}
	MaxShardsPerNode  string                 `json:"maxShardsPerNode"`  // e.g. "1" (yes, these are strings, not numbers)
	AutoAddReplicas   string                 `json:"autoAddReplicas"`   // e.g. "false" (yes, these are strings, not bools)

	// These fields are synthetic. They ARE present in COLLECTIONSTATUS response in
	// solr collection API, but they are NOT in state.json docs in Zookeeper.

	ConfigName      string `json:"configName,omitempty"`   // the name of the node in solr/configs (in ZK) that this collection uses
	ZkNodeVersion   int32  `json:"znodeVersion,omitempty"` // the ZK node version this state snapshot represents
	PerReplicaState string `json:"perReplicaState"`        // whether collection keeps state for each replica separately
}

func (cs *CollectionState) String() string {
	return fmt.Sprintf("CollectionState{Shards:%+v, PerReplicaState:%s}", cs.Shards, cs.PerReplicaState)
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

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

type CollectionState struct {
	Shards            map[string]ShardState `json:"shards"`            // map from shard name to shard state
	ReplicationFactor string                `json:"replicationFactor"` // e.g. "1" (yes, these are strings, not numbers)
	Router            Router                `json:"router"`            // e.g. {"name":"compositeId"}
	MaxShardsPerNode  string                `json:"maxShardsPerNode"`  // e.g. "1" (yes, these are strings, not numbers)
	AutoAddReplicas   string                `json:"autoAddReplicas"`   // e.g. "false" (yes, these are strings, not bools)
}

type Router struct {
	Name string `json:"name"` // e.g. "compositeId"
}

// LookupCore finds and returns the replica with the given core name, if one exists.
func (c CollectionState) LookupCore(coreName string) (shard, replica string) {
	for shardName, shard := range c.Shards {
		for replicaName, replica := range shard.Replicas {
			if replica.Core == coreName {
				// found it!
				return shardName, replicaName
			}
		}
	}

	return "", ""
}

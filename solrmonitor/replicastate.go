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

type ReplicaState struct {
	Core     string `json:"core"`      // e.g. "delta_shard1_replica1"
	BaseUrl  string `json:"base_url"`  // e.g. "http://10.240.110.3:8983/solr"
	NodeName string `json:"node_name"` // e.g. "10.240.110.3:8983_solr"
	State    string `json:"state"`     // e.g. "active", "inactive", "down", "recovering"
	Leader   string `json:"leader"`    // e.g. "true" or "false" (yes, these are strings, not bools)
	Version  int32  `json:"version"`   // e.g. "1" version of replica state
	Type     string `json:"type"`      // e.g. "NRT", "PULL"
}

func (r *ReplicaState) String() string {
	return fmt.Sprintf("\nReplicaState{Core:%s, BaseUrl:%s, NodeName=%s, State=%s, Leader=%s, Version=%d, Type=%s}\n", r.Core, r.BaseUrl, r.NodeName, r.State, r.Leader, r.Version, r.Type)
}

func (r ReplicaState) IsActive() bool {
	return r.State == "active"
}

func (r ReplicaState) IsLeader() bool {
	return r.Leader == "true"
}

func (r ReplicaState) IsNRT() bool {
	return r.Type == "NRT"
}

func (r ReplicaState) IsPull() bool {
	return r.Type == "PULL"
}

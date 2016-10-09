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

type ReplicaState struct {
	State    string `json:"state"`     // e.g. "active", "inactive", "down", "recovering"
	Core     string `json:"core"`      // e.g. "delta_shard1_replica1"
	NodeName string `json:"node_name"` // e.g. "10.240.110.3:8983_solr"
	BaseUrl  string `json:"base_url"`  // e.g. "http://10.240.110.3:8983/solr"
	Leader   string `json:"leader"`    // e.g. "true" or "false" (yes, these are strings, not bools)
}

func (r ReplicaState) IsActive() bool {
	return r.State == "active"
}

func (r ReplicaState) IsLeader() bool {
	return r.Leader == "true"
}

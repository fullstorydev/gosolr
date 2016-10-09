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

package solrmanapi

type SolrCloudStatus map[string]*SolrNodeStatus // keys are hostnames

type SolrNodeStatus struct {
	Hostname string                     // the node's hostname, as determined by gethostbyaddr
	NodeName string                     // the node's identifier within SolrCloud (e.g. "1.1.1.1:8983_solr")
	Cores    map[string]*SolrCoreStatus // keys are core names
}

type SolrCoreStatus struct {
	Name           string
	NodeName       string  // the node's identifier within SolrCloud (e.g. "1.1.1.1:8983_solr")
	Collection     string  // collection name (which is our case is always [orgId]-v[version])
	Shard          string  // shard name (e.g. "shard1")
	ShardState     string  // e.g. "active", "inactive"
	Replica        string  // replica name (e.g. "core_node_2")
	ReplicaState   string  // e.g. "active", "inactive", "recovering", "down"
	IsLeader       bool    // whether this replica is the leader of its shard
	NumDocs        int64   // total number of indexed documents
	IndexSize      int64   // in bytes
	HashRangeShare float64 // what fraction of the collection's hash range is mapped to this shard
}

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

import "time"

// TODO: rename 'NodeStatuses'
type SolrCloudStatus map[string]*SolrNodeStatus // keys are hostnames

type SolrNodeStatus struct {
	Hostname               string                     // the node's hostname, as determined by gethostbyaddr
	NodeName               string                     // the node's identifier within SolrCloud (e.g. "1.1.1.1:8983_solr")
	Cores                  map[string]*SolrCoreStatus // keys are core names
	Zone                   string                     // cloud zone where host exists
	DiskSize               float64
	IndexDiskSize          float64
	IndexDiskSizeTotal     int64
	IndexDiskSizeAvailable int64
}

type SolrCoreStatus struct {
	Name         string
	NodeName     string    // the node's identifier within SolrCloud (e.g. "1.1.1.1:8983_solr")
	Collection   string    // collection name (e.g. "1A00E" or "thefullstory.com")
	Shard        string    // shard name (e.g. "shard1")
	ShardState   string    // e.g. "active", "inactive"
	Range        string    // // e.g. "80000000-b332ffff"
	Replica      string    // replica name (e.g. "core_node_2")
	ReplicaState string    // e.g. "active", "inactive", "recovering", "down"
	IsLeader     bool      // whether this replica is the leader of its shard
	HasStats     bool      // if false, core status could not be queried and following attributes are expected to be -1
	NumDocs      int64     // total number of indexed documents
	IndexSize    int64     // in bytes
	Type         string    // e.g. "NRT", "PULL", "TLOG"
	Version      int64     // Update version of the replica (may be empty)
	LastModified time.Time // Last time the replica was modified (may be empty)
}

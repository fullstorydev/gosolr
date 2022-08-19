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

type SolrmanStatusResponse struct {
	SolrNodes         SolrCloudStatus
	InProgressSolrOps []OpRecord
	CompletedSolrOps  []OpRecord
}

type MoveReplicaRequest struct {
	Collection string
	Shard      string
	Replica    string
	SrcNode    string // Solr node name of the source node (e.g. "1.1.1.1:8983_solr"
	DstNode    string // Solr node name of the destination node (e.g. "1.1.1.1:8983_solr"
	Requestor  string // Username of who requested the operation ("solrman" if through automation)
	NumDocs    int64  // The number of docs in this shard
	IndexSize  int64  // The size of the shard in bytes
	Type       string // e.g. "NRT", "PULL", "TLOG"
}

type SplitShardRequest struct {
	Collection string
	Shard      string
	SrcNode    string // Node that the shard to be split lives on.
	Requestor  string // Username of who requested the operation ("solrman" if through automation)
	NumDocs    int64  // The number of docs in this shard
	IndexSize  int64  // The size of the shard in bytes
}

type OpStatusResponse struct {
	Error string
}

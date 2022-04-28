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

import (
	"fmt"
)

const (
	OpStatus     = "Status"
	OpMoveShard  = "MoveShard"
	OpSplitShard = "SplitShard"
)

type OpRecord struct {
	StartedMs  int64  `json:"Started"`  // start time, in millis since epoch
	FinishedMs int64  `json:"Finished"` // end time, in millis since epoch
	NumDocs    int64  `json:"NumDocs"`
	IndexSize  int64  `json:"IndexSize"`
	Operation  string // one of the Op* constants
	Collection string
	Shard      string
	Replica    string // replica name (i.e. core_node09)
	SrcNode    string // instance name of source node (in the case of a split, this node has the parent)
	DstNode    string // instance name of destination node
	Requestor  string // Who requested the operation (either a user, or "solrman" if automation)
	Error      string // non-empty if the operation failed; always set on a Status op
	AsyncId    string // Async ID if present
}

func (r *OpRecord) String() string {
	switch r.Operation {
	case OpStatus:
		return fmt.Sprintf("status: %s", r.Error)
	case OpMoveShard:
		var replica string
		if r.Replica != "" {
			replica = "_" + r.Replica
		}
		return fmt.Sprintf("move %s_%s%s from %s to %s", r.Collection, r.Shard, replica, r.SrcNode, r.DstNode)
	case OpSplitShard:
		return fmt.Sprintf("split %s_%s", r.Collection, r.Shard)
	default:
		return fmt.Sprintf("unknown operation %q", r.Operation)
	}
}

func (r *OpRecord) Key() string {
	return fmt.Sprintf("SolrOp:%s:%s", r.Collection, r.Shard)
}

type ByStartedRecently []OpRecord

func (a ByStartedRecently) Len() int           { return len(a) }
func (a ByStartedRecently) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByStartedRecently) Less(i, j int) bool { return a[i].StartedMs > a[j].StartedMs }

type ByFinishedRecently []OpRecord

func (a ByFinishedRecently) Len() int           { return len(a) }
func (a ByFinishedRecently) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFinishedRecently) Less(i, j int) bool { return a[i].FinishedMs > a[j].FinishedMs }

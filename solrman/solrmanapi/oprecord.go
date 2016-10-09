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
	Operation  string // one of the Op* constants
	Collection string
	Shard      string
	SrcNode    string // instance name of source node
	DstNode    string // instance name of destination node
	Error      string // non-empty if the operation failed; always set on a Status op
}

func (r *OpRecord) String() string {
	switch r.Operation {
	case OpStatus:
		return fmt.Sprintf("status: %s", r.Error)
	case OpMoveShard:
		return fmt.Sprintf("move %s_%s to %s", r.Collection, r.Shard, r.DstNode)
	case OpSplitShard:
		return fmt.Sprintf("split %s_%s", r.Collection, r.Shard)
	default:
		return fmt.Sprintf("unknown operation %q", r.Operation)
	}
}

func (r *OpRecord) Key() string {
	return fmt.Sprintf("SolrOp:%s:%s", r.Collection, r.Shard)
}

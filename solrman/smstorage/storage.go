// Copyright 2017 FullStory, Inc.
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

package smstorage

import (
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
)

// Completed ops have names like `completed-0000001157` (20 bytes). ZK's GetChildren() is a
// single call whose response payload must fit into less than ~4MB, so we have to keep this
// number semi-reasonable.
const NumStoredCompletedOps = 1000

type SolrManStorage interface {
	// Record an op as being in-progress.  op.Key() must be unique.
	AddInProgressOp(op solrmanapi.OpRecord) error

	// Delete an op that was in-progress.  op.Key() must be unique.  Does not error on no-op.
	DelInProgressOp(op solrmanapi.OpRecord) error

	// Returns the list of ops currently in progress.
	GetInProgressOps() ([]solrmanapi.OpRecord, error)

	// Record an op as being completed.
	AddCompletedOp(op solrmanapi.OpRecord) error

	// Returns the N most recent completed ops.
	GetCompletedOps(count int) ([]solrmanapi.OpRecord, error)

	// Returns a list of solr nodes that should be evacuated (e.g. move cores off these nodes onto other nodes)
	GetEvacuateNodeList() ([]string, error)

	IsDisabled() bool       // if true, solrman is entirely disabled
	SetDisabled(bool) error // disable solrman due to unrecoverable error completing an op

	IsSplitsDisabled() bool       // if true, don't do splits
	SetSplitsDisabled(bool) error // disable splits

	IsMovesDisabled() bool       // if true, don't do moves
	SetMovesDisabled(bool) error // disable moves

	IsStabbingEnabled() bool       // if true, Solrman will automatically restart problematic nodes
	SetStabbingEnabled(bool) error // enable auutomatic node restarts
}

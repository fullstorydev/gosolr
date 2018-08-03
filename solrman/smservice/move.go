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

package smservice

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
)

// Long call; run in a go routine. Run the given move operation and records the result when done.
func (s *SolrManService) runMoveOperation(move solrmanapi.OpRecord) {
	err := s.doRunMoveOperation(move)
	if err != nil {
		move.Error = fmt.Sprintf("failed MoveShard request: %s with err: %s", move.String(), err)
		move.FinishedMs = nowMillis()
		s.Logger.Errorf("failed MoveShard request: %s with err: %s", move.String(), err)
	} else {
		move.FinishedMs = nowMillis()
		s.Logger.Infof("completed MoveShard request: %s", move.String())
	}

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.inProgressOps, move.Key())
	}()

	if err := s.Storage.DelInProgressOp(move); err != nil {
		s.Logger.Warningf("failed to DelInProgressOp completed move %s: %s", move.Key(), err)
	}
	if err := s.Storage.AddCompletedOp(move); err != nil {
		s.Logger.Warningf("failed to AddCompletedOp completed move %s: %s", move.Key(), err)
	}
}

// Run the given move operation; returns an error, or nil if it succeeds.
func (s *SolrManService) doRunMoveOperation(move solrmanapi.OpRecord) error {
	// Fail if we can't retrieve the initial state.
	coll, err := s.SolrMonitor.GetCollectionState(move.Collection)
	if err != nil {
		return err
	}
	if coll == nil {
		return smutil.Cherrf(err, "no such collection (maybe it disappeared?)")
	}

	var replicas map[string]solrmonitor.ReplicaState
	if shard, ok := coll.Shards[move.Shard]; !ok {
		// guard against no-such-shard
		return smutil.Cherrf(err, "no such shard %s in collection %s", move.Shard, move.Collection)
	} else {
		replicas = shard.Replicas
	}

	s.Audit.BeforeOp(move, *coll)
	lastZkVersion := coll.ZkNodeVersion
	success := false
	defer func() {
		var newCollState *solrmonitor.CollectionState
		for retry := 0; retry < 3; retry++ {
			var err error
			if newCollState, err = s.SolrMonitor.GetCollectionState(move.Collection); err != nil {
				s.Logger.Errorf("failed to retrieve collection state after op %s: %s", move.String(), err)
				return
			} else {
				if newCollState.ZkNodeVersion > lastZkVersion {
					break
				}
			}
			// Haven't seen an update yet, wait a little and try again.
			time.Sleep(5 * time.Second)
		}
		if success {
			s.Audit.SuccessOp(move, *newCollState)
		} else {
			s.Audit.FailedOp(move, *newCollState)
			s.disable()
		}
	}()

	// Add a replica if none exists.
	// TODO: scottb handle timeout separately from other failures, we should loop and retry?
	if replica := findReplica(replicas, move.DstNode, anyReplica); replica == "" {
		// No replica exists, need to add one.
		if err := s.solrClient.AddReplica(move.Collection, move.Shard, move.DstNode, ""); err != nil {
			return smutil.Cherrf(err, "failed to issue ADDREPLICA command")
		}
		s.Logger.Debugf("ADDREPLICA command issued successfully MoveShard request: %s", move.String())
	}

	// Wait on the replica to sync up and be "live"
	for {
		coll, err := s.SolrMonitor.GetCollectionState(move.Collection)
		if err != nil {
			// TODO: give up eventually after too many retries?
			s.Logger.Warningf("Error retrieving collection state for %s: %s", move.Collection, err)
		} else {
			if shard, ok := coll.Shards[move.Shard]; ok {
				// guard against no-such-shard
				replicas = shard.Replicas
			}
			if replica := findReplica(replicas, move.DstNode, activeReplica); replica != "" {
				// found a good replica!  sync complete
				s.Logger.Debugf("active replica %q found on %s - time to delete the original", replica, move.DstNode)
				s.Audit.SuccessOp(move, *coll) // should log state where both replicas exist
				lastZkVersion = coll.ZkNodeVersion
				break
			}
		}
		// consider something event-driven instead of polling
		time.Sleep(10 * time.Second)
	}

	// Now delete the original
	original := findReplica(replicas, move.SrcNode, activeReplica)
	if original == "" {
		return smutil.Errorf("no original found for shard %s of collection %q on node %s!?", move.Shard, move.Collection, move.SrcNode)
	}

	if err := s.solrClient.DeleteReplica(move.Collection, move.Shard, original, ""); err != nil {
		// If this fails because the replica doesn't exist, then we assume that some prior attempt finished the job
		// (probably the http request timed out, but the actual api action eventually succeeded within Solr).  So we
		// just treat this as a success.
		if solrErr := isNoSuchReplicaError(err, original); solrErr != nil {
			s.Logger.Debugf("assuming replica was previously deleted: %s", solrErr)
			success = true
			return nil
		} else {
			return smutil.Cherrf(err, "failed to issue DELETEREPLICA command")
		}
	}
	s.Logger.Debugf("DELETEREPLICA command issued successfully MoveShard request: %s", move.String())
	success = true
	return nil
}

func isNoSuchReplicaError(err error, replica string) *ErrorRsp {
	if err, ok := smutil.Root(err).(*ErrorRsp); ok {
		s := fmt.Sprintf("Invalid replica : %s", replica)
		if err.Code == http.StatusBadRequest && strings.Contains(err.Msg, s) {
			return err
		}
	}

	return nil
}

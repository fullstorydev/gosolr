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
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrman/smstorage"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/samuel/go-zookeeper/zk"
)

type SolrManService struct {
	ClusterName string
	HttpClient  *http.Client
	SolrMonitor *solrmonitor.SolrMonitor
	ZooClient   *zk.Conn
	Storage     smstorage.SolrManStorage
	Logger      smutil.Logger // for "normal" logging
	AlertLog    smutil.Logger // for "alert" logging that should notify engineers
	Audit       Audit
	solrClient  *SolrClient

	mu            sync.Mutex
	inProgressOps map[string]solrmanapi.OpRecord
	statusOp      *solrmanapi.OpRecord // when non-nil, provides admin visibility into solrman's state
}

// ClusterState responds with a struct containing info on the cores in SolrCloud.
func (s *SolrManService) ClusterState() (*solrmanapi.SolrmanStatusResponse, error) {
	cluster, err := s.SolrMonitor.GetCurrentState()
	if err != nil {
		return nil, smutil.Cherrf(err, "failed to read cluster state")
	}

	solrNodes, err := s.SolrMonitor.GetLiveNodes()
	if err != nil {
		return nil, smutil.Cherrf(err, "failed to get live_nodes")
	}

	var rsp solrmanapi.SolrmanStatusResponse
	rsp.SolrNodes = s.getLiveNodesStatuses(solrNodes, cluster)

	rsp.CompletedSolrOps, err = s.Storage.GetCompletedOps(100)
	if err != nil {
		s.Logger.Warningf("failed to get completed solr op list: %s", err)
	}

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, op := range s.inProgressOps {
			rsp.InProgressSolrOps = append(rsp.InProgressSolrOps, op)
		}
		if s.statusOp != nil {
			rsp.InProgressSolrOps = append(rsp.InProgressSolrOps, *s.statusOp)
		}
	}()

	sort.Sort(solrmanapi.ByFinishedRecently(rsp.CompletedSolrOps))
	sort.Sort(solrmanapi.ByStartedRecently(rsp.InProgressSolrOps))
	return &rsp, nil
}

func (s *SolrManService) getLiveNodesStatuses(liveNodes []string, cluster solrmonitor.ClusterState) solrmanapi.SolrCloudStatus {
	var wg sync.WaitGroup
	ch := make(chan *solrmanapi.SolrNodeStatus, len(liveNodes))

	for _, solrNode := range liveNodes {
		wg.Add(1)
		go func(solrNode string) {
			defer wg.Done()

			// note: we are passing 'cluster' to multiple concurrent goroutines; they must not write to it!
			status, err := s.getNodeStatus(solrNode, cluster)
			if err != nil {
				s.Logger.Errorf("failed to get status for solr node %q", solrNode)
			} else {
				ch <- status
			}
		}(solrNode)
	}

	wg.Wait()
	close(ch)

	nodesStatus := make(solrmanapi.SolrCloudStatus)
	for status := range ch {
		if _, ok := nodesStatus[status.Hostname]; ok {
			// not normal!!
			s.Logger.Warningf("multiple SolrNodeStatus results received for %s", status.Hostname)
		}
		nodesStatus[status.Hostname] = status
	}
	return nodesStatus
}

func (s *SolrManService) getNodeStatus(solrNode string, cluster solrmonitor.ClusterState) (*solrmanapi.SolrNodeStatus, error) {
	params := url.Values{}
	params.Add("action", "STATUS")
	var coreStatusRsp CoreStatusRsp
	if err := s.solrClient.doCoreCall(solrNode, params, &coreStatusRsp); err != nil {
		s.Logger.Warningf("failed to get core status for %s: %s", solrNode, err)
		// we can just continue; since coreStatusRsp is empty the code below will still work out
	}

	rsp := solrmanapi.SolrNodeStatus{
		Hostname: smutil.GetHostname(solrNode),
		NodeName: solrNode,
		Cores:    make(map[string]*solrmanapi.SolrCoreStatus),
	}

	for collName, collState := range cluster {
		for shardName, shard := range collState.Shards {
			for replicaName, replica := range shard.Replicas {
				if replica.NodeName == solrNode {
					rsp.Cores[replica.Core] = &solrmanapi.SolrCoreStatus{
						Name:         replica.Core,
						NodeName:     solrNode,
						Collection:   collName,
						Shard:        shardName,
						ShardState:   shard.State,
						Range:        shard.Range,
						Replica:      replicaName,
						ReplicaState: replica.State,
						IsLeader:     replica.IsLeader(),
						HasStats:     false,
						NumDocs:      -1,
						IndexSize:    -1,
					}

					if core, ok := coreStatusRsp.Status[replica.Core]; ok {
						c := rsp.Cores[replica.Core]
						c.HasStats = true
						c.NumDocs = core.Index.NumDocs
						c.IndexSize = core.Index.SizeInBytes
					}
				}
			}
		}
	}

	return &rsp, nil
}

func (s *SolrManService) Init() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.solrClient = &SolrClient{
		HttpClient:  s.HttpClient,
		SolrMonitor: s.SolrMonitor,
		Logger:      s.Logger,
	}

	if s.inProgressOps != nil {
		panic("SolrManService initialized more than once")
	}
	s.inProgressOps = map[string]solrmanapi.OpRecord{}

	inProgressOps, err := s.Storage.GetInProgressOps()
	if err != nil {
		s.Logger.Warningf("failed to get in progress ops: %s", err)
		return
	}

	for _, op := range inProgressOps {
		s.inProgressOps[op.Key()] = op
		s.Logger.Infof("Resuming from storage: %s", op)
		switch op.Operation {
		case solrmanapi.OpMoveShard:
			go s.runMoveOperation(op)
		case solrmanapi.OpSplitShard:
			go s.runSplitOperation(op)
		default:
			s.Logger.Errorf("In progress operation found in storage with unknown op type: %+v", op)
			delete(s.inProgressOps, op.Key())
		}
	}
}

func (s *SolrManService) MoveShard(params *solrmanapi.MoveShardRequest) (*solrmanapi.OpStatusResponse, error) {
	var rsp solrmanapi.OpStatusResponse

	s.Logger.Debugf("MoveShard request: %+v", params)

	coll, err := s.SolrMonitor.GetCollectionState(params.Collection)
	if err != nil {
		return nil, smutil.Cherrf(err, "failed to read cluster state")
	}
	if coll == nil {
		// Error, no such collection
		rsp.Error = fmt.Sprintf("No such collection %q", params.Collection)
		return &rsp, nil
	}

	liveNodes, err := s.SolrMonitor.GetLiveNodes()
	if err != nil {
		return nil, smutil.Cherrf(err, "failed to read live_nodes")
	}

	liveNodeSet := map[string]bool{}
	for _, liveNode := range liveNodes {
		liveNodeSet[liveNode] = true
	}

	if !liveNodeSet[params.SrcNode] {
		// Error, invalid source node
		rsp.Error = fmt.Sprintf("SrcNode %q is not in live_nodes", params.SrcNode)
		return &rsp, nil
	}
	if !liveNodeSet[params.DstNode] {
		// Error, invalid source node
		rsp.Error = fmt.Sprintf("DstNode %q is not in live_nodes", params.DstNode)
		return &rsp, nil
	}

	replicas := make(map[string]solrmonitor.ReplicaState)
	if shard, ok := coll.Shards[params.Shard]; ok { // guard against no-such-shard
		replicas = shard.Replicas
	}

	if params.SrcNode == params.DstNode {
		// Error, invalid source node
		rsp.Error = "SrcNode and DstNode cannot be the same"
		return &rsp, nil
	}

	if replica := findReplica(replicas, params.SrcNode, activeReplica); replica == "" {
		// Error, invalid source node
		rsp.Error = fmt.Sprintf("Shard %q does not exist on SrcNode %q", params.Shard, params.SrcNode)
		return &rsp, nil
	}

	// Basic sanity checks passed.
	move := solrmanapi.OpRecord{
		StartedMs:  nowMillis(),
		Operation:  solrmanapi.OpMoveShard,
		Collection: params.Collection,
		Shard:      params.Shard,
		SrcNode:    params.SrcNode,
		DstNode:    params.DstNode,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// See if any op is already in progress for this shard.
	if inProgressOp, ok := s.inProgressOps[move.Key()]; ok {
		if inProgressOp.Operation == solrmanapi.OpMoveShard &&
			inProgressOp.SrcNode == move.SrcNode &&
			inProgressOp.DstNode == move.DstNode {
			// identical move in progress, just report success
		} else {
			// conflicting operation in progress
			rsp.Error = fmt.Sprintf("That shard is busy: %s", inProgressOp.String())
		}
	} else {
		// Run the operation
		if err := s.Storage.AddInProgressOp(move); err != nil {
			return nil, smutil.Cherrf(err, "failed to write operation to storage")
		}
		s.inProgressOps[move.Key()] = move
		go s.runMoveOperation(move)
	}
	return &rsp, nil
}

func (s *SolrManService) SplitShard(params *solrmanapi.SplitShardRequest) (*solrmanapi.OpStatusResponse, error) {
	var rsp solrmanapi.OpStatusResponse

	s.Logger.Debugf("SplitShard request: %+v", params)

	coll, err := s.SolrMonitor.GetCollectionState(params.Collection)
	if err != nil {
		return nil, smutil.Cherrf(err, "failed to read cluster state")
	}
	if coll == nil {
		// Error, no such collection
		rsp.Error = fmt.Sprintf("No such collection %q", params.Collection)
		return &rsp, nil
	}

	if _, ok := coll.Shards[params.Shard]; !ok { // guard against no-such-shard
		// Error, no such shard
		rsp.Error = fmt.Sprintf("No such shard %q in collection %q", params.Shard, params.Collection)
		return &rsp, nil
	}

	// Basic sanity checks passed.
	split := solrmanapi.OpRecord{
		StartedMs:  nowMillis(),
		Operation:  solrmanapi.OpSplitShard,
		Collection: params.Collection,
		Shard:      params.Shard,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// See if any op is already in progress for this shard.
	if inProgressOp, ok := s.inProgressOps[split.Key()]; ok {
		if inProgressOp.Operation == solrmanapi.OpSplitShard {
			// identical split in progress, just report success
		} else {
			// conflicting operation in progress
			rsp.Error = fmt.Sprintf("That shard is busy: %s", inProgressOp.String())
		}
	} else {
		// Run the operation
		if err := s.Storage.AddInProgressOp(split); err != nil {
			return nil, smutil.Cherrf(err, "failed to write operation to storage")
		}
		s.inProgressOps[split.Key()] = split
		go s.runSplitOperation(split)
	}
	return &rsp, nil
}

// findReplica returns the name of a replica on the specified node, or "" if no such replica currently exists.
func findReplica(replicas map[string]solrmonitor.ReplicaState, node string, filter func(solrmonitor.ReplicaState) bool) string {
	for replicaName, replica := range replicas {
		if replica.NodeName != node {
			continue
		}

		if !filter(replica) {
			continue
		}

		return replicaName
	}

	return ""
}

func anyReplica(solrmonitor.ReplicaState) bool {
	return true
}

func activeReplica(replica solrmonitor.ReplicaState) bool {
	return replica.IsActive()
}

func unixMillis(t time.Time) int64 {
	return t.UnixNano() / 1e6
}

func nowMillis() int64 {
	return unixMillis(time.Now())
}

// Disables solrman
func (s *SolrManService) disable() {
	if s.Storage.IsDisabled() {
		return // already disabled, nothing to do
	}

	if err := s.Storage.SetDisabled(true); err != nil {
		s.Logger.Errorf("failed to set disabled state: %s", err)
	}

	s.AlertLog.Errorf("automatically disabling after encountering operation error; see logs for details")
}

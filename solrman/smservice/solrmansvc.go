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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/garyburd/redigo/redis"
	"github.com/samuel/go-zookeeper/zk"
)

type SolrManService struct {
	HttpClient  *http.Client
	SolrMonitor *solrmonitor.SolrMonitor
	ZooClient   *zk.Conn
	RedisPool   *redis.Pool
	Logger      Logger
	solrClient  *SolrClient

	mu            sync.Mutex
	inProgressOps map[string]*solrmanapi.OpRecord
	statusOp      *solrmanapi.OpRecord // when non-nil, provides admin visibility into solrman's state
}

const (
	OpMapRedisKey       = "Solrman:in_progress_op_map"
	CompletedOpRedisKey = "Solrman:completed_op_list"
	DisableRedisKey     = "Solrman:disabled"
)

type byStartedRecently []solrmanapi.OpRecord

func (a byStartedRecently) Len() int           { return len(a) }
func (a byStartedRecently) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStartedRecently) Less(i, j int) bool { return a[i].StartedMs > a[j].StartedMs }

type byFinishedRecently []solrmanapi.OpRecord

func (a byFinishedRecently) Len() int           { return len(a) }
func (a byFinishedRecently) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFinishedRecently) Less(i, j int) bool { return a[i].FinishedMs > a[j].FinishedMs }

// ClusterState responds with a struct containing info on the cores in SolrCloud.
func (s *SolrManService) ClusterState() (*solrmanapi.SolrmanStatusResponse, error) {
	cluster, err := s.SolrMonitor.GetCurrentState()
	if err != nil {
		return nil, cherrf(err, "failed to read cluster state")
	}

	solrNodes, err := s.SolrMonitor.GetLiveNodes()
	if err != nil {
		return nil, cherrf(err, "failed to get live_nodes")
	}

	var wg sync.WaitGroup
	ch := make(chan *solrmanapi.SolrNodeStatus, len(solrNodes))

	for _, solrNode := range solrNodes {
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

	var rsp solrmanapi.SolrmanStatusResponse
	var payload = make(solrmanapi.SolrCloudStatus)
	rsp.SolrNodes = payload

	for status := range ch {
		if _, ok := rsp.SolrNodes[status.Hostname]; ok {
			// not normal!!
			s.Logger.Warningf("multiple SolrNodeStatus results received for %s", status.Hostname)
		}

		rsp.SolrNodes[status.Hostname] = status
	}

	func() {
		conn := s.RedisPool.Get()
		defer conn.Close()
		completedOps, err := redis.Strings(conn.Do("LRANGE", CompletedOpRedisKey, 0, 99))
		if err != nil {
			s.Logger.Warningf("failed to LRANGE %s: %s", CompletedOpRedisKey, err)
		} else {
			rsp.CompletedSolrOps = make([]solrmanapi.OpRecord, len(completedOps))
			for i, completedOp := range completedOps {
				json.NewDecoder(strings.NewReader(completedOp)).Decode(&rsp.CompletedSolrOps[i])
			}
		}
	}()

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, op := range s.inProgressOps {
			rsp.InProgressSolrOps = append(rsp.InProgressSolrOps, *op)
		}
		if s.statusOp != nil {
			rsp.InProgressSolrOps = append(rsp.InProgressSolrOps, *s.statusOp)
		}
	}()

	sort.Sort(byFinishedRecently(rsp.CompletedSolrOps))
	sort.Sort(byStartedRecently(rsp.InProgressSolrOps))
	return &rsp, nil
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
		Hostname: gethostname(solrNode),
		NodeName: solrNode,
		Cores:    make(map[string]*solrmanapi.SolrCoreStatus),
	}

	for collName, collState := range cluster {
		for shardName, shard := range collState.Shards {
			for replicaName, replica := range shard.Replicas {
				if replica.NodeName == solrNode {
					rsp.Cores[replica.Core] = &solrmanapi.SolrCoreStatus{
						Name:           replica.Core,
						NodeName:       solrNode,
						Collection:     collName,
						Shard:          shardName,
						ShardState:     shard.State,
						Replica:        replicaName,
						ReplicaState:   replica.State,
						IsLeader:       replica.IsLeader(),
						NumDocs:        -1,
						IndexSize:      -1,
						HashRangeShare: collState.Shards[shardName].HashRangeShare(),
					}

					if core, ok := coreStatusRsp.Status[replica.Core]; ok {
						c := rsp.Cores[replica.Core]
						c.NumDocs = core.Index.NumDocs
						c.IndexSize = core.Index.SizeInBytes
					}
				}
			}
		}
	}

	return &rsp, nil
}

// gethostname performs a DNS lookup on ip and returns the first hostname returned.  If the lookup fails, ip is
// returned.
func gethostname(solrNode string) string {
	ip, _, _, err := parseNodeName(solrNode)
	if err != nil {
		return ""
	}
	if names, err := net.LookupAddr(ip); err != nil {
		return ip // fall back to just using the IP
	} else {
		// Just return the first part of the hostname
		hostname := names[0]
		i := strings.Index(hostname, ".")
		if i > -1 {
			hostname = hostname[:i]
		}
		return hostname
	}
}

func (s *SolrManService) Init() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.solrClient = &SolrClient{
		HttpClient:  s.HttpClient,
		SolrMonitor: s.SolrMonitor,
	}

	if s.inProgressOps != nil {
		panic("SolrManService initialized more than once")
	}
	s.inProgressOps = make(map[string]*solrmanapi.OpRecord)

	inProgressPairs, err := func() ([]string, error) {
		conn := s.RedisPool.Get()
		defer conn.Close()
		return redis.Strings(conn.Do("HGETALL", OpMapRedisKey))
	}()
	if err != nil {
		s.Logger.Warningf("failed to HGETALL %s: %s", OpMapRedisKey, err)
		return
	}

	for i, val := range inProgressPairs {
		if i%2 == 0 {
			// Skip the keys, we just need the values
			continue
		}

		op := &solrmanapi.OpRecord{}
		if err := json.NewDecoder(strings.NewReader(val)).Decode(op); err != nil {
			s.Logger.Errorf("In progress operation found in redis failed to parse: %s", val)
			continue
		}
		s.inProgressOps[op.Key()] = op
		s.Logger.Infof("Resuming from redis: %s", op)
		switch op.Operation {
		case solrmanapi.OpMoveShard:
			go s.runMoveOperation(op)
		case solrmanapi.OpSplitShard:
			go s.runSplitOperation(op)
		default:
			s.Logger.Errorf("In progress operation found in redis with unknown op type: %+v", op)
			delete(s.inProgressOps, op.Key())
		}
	}
}

func (s *SolrManService) MoveShard(params *solrmanapi.MoveShardRequest) (*solrmanapi.OpStatusResponse, error) {
	var rsp solrmanapi.OpStatusResponse

	s.Logger.Debugf("MoveShard request: %+v", params)

	coll, err := s.SolrMonitor.GetCollectionState(params.Collection)
	if err != nil {
		return nil, cherrf(err, "failed to read cluster state")
	}
	if coll == nil {
		// Error, no such collection
		rsp.Error = fmt.Sprintf("No such collection %q", params.Collection)
		return &rsp, nil
	}

	liveNodes, err := s.SolrMonitor.GetLiveNodes()
	if err != nil {
		return nil, cherrf(err, "failed to read live_nodes")
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
	move := &solrmanapi.OpRecord{
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
	inProgressOp := s.inProgressOps[move.Key()]
	if inProgressOp != nil {
		if inProgressOp.Operation == solrmanapi.OpMoveShard &&
			inProgressOp.SrcNode == move.SrcNode &&
			inProgressOp.DstNode == move.DstNode {
			// identical move in progress, just report success
		} else {
			// conflicting operation in progress
			rsp.Error = fmt.Sprintf("That shard is busy: %s", inProgressOp)
		}
	} else {
		// Run the operation
		conn := s.RedisPool.Get()
		defer conn.Close()
		result, err := redis.Int(conn.Do("HSET", OpMapRedisKey, move.Key(), jsonString(move)))
		if result != 1 || err != nil {
			return nil, cherrf(err, "failed to write operation to redis")
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
		return nil, cherrf(err, "failed to read cluster state")
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
	split := &solrmanapi.OpRecord{
		StartedMs:  nowMillis(),
		Operation:  solrmanapi.OpSplitShard,
		Collection: params.Collection,
		Shard:      params.Shard,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// See if any op is already in progress for this shard.
	inProgressOp := s.inProgressOps[split.Key()]
	if inProgressOp != nil {
		if inProgressOp.Operation == solrmanapi.OpSplitShard {
			// identical split in progress, just report success
		} else {
			// conflicting operation in progress
			rsp.Error = fmt.Sprintf("That shard is busy: %s", inProgressOp)
		}
	} else {
		// Run the operation
		conn := s.RedisPool.Get()
		defer conn.Close()
		result, err := redis.Int(conn.Do("HSET", OpMapRedisKey, split.Key(), jsonString(split)))
		if result != 1 || err != nil {
			return nil, cherrf(err, "failed to write operation to redis")
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

func jsonString(val interface{}) string {
	if val == nil {
		return ""
	}

	b, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		panic(err.Error())
	}
	return string(b)
}

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
	"fs/metrics"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/fullstorydev/gosolr/smutil"
	"github.com/fullstorydev/gosolr/solrcheck"
	"github.com/fullstorydev/gosolr/solrman/smmodel"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	movesPerCycle                 = 10               // How many shards to move at a time
	iterationSleep                = 1 * time.Minute  // How long to sleep each attempt, no matter what
	quiescenceSleep               = 10 * time.Minute // How long to sleep when stability is reached
	splitsPerMachine              = 2                // How many shard splitting jobs to schedule on 1 physical machine at a time
	splitShardsWithDocCount       = 4000000          // Split shards with doc count > this
	allSplitsDocCountTrigger      = 4004000          // But don't do any splits until at least one shard > this
	allowedMinToMaxShardSizeRatio = 0.2              // Ratio of smallest shard to biggest shard < this then warn about imbalance
	maxShardsPerMachine           = 16               // Maximum number of shards per machine in the cluster
	disabledNoticePeriod          = time.Hour * 6    // Duration between notices (to alert logger) that solrman is still disabled
	waitBeforeAlerting            = time.Minute * 5  // Time before solrman sends a slack notification about not being golden
)

var (
	// Prometheus Metrics
	// NOTE: because of the for..continue design of RunSolrMan, some metrics could get out of date if issues are
	// detected before they get processed. ZkConnected will  always be correct since it's the first metric processed.
	// Metrics are processed in the following order: ZkIsConnected -> InProgressOps -> SolrLiveNodes -> ProblemCount.
	metricLabels = metrics.Labels{"cluster"} // Labels used for prometheus metrics
	// Is solrman currently connected to zookeeper. 1 if connected and 0 otherwise
	ZkIsConnected = metrics.NewLabelledGauge("solrman.zk.connected", "Is solrman currently connected to zookeeper. 1 if connected and 0 otherwise", metricLabels)
	// The number of operations currently in progress
	InProgressOps = metrics.NewLabelledGauge("solrman.ops.inprogress", "The number of operations currently in progress", metricLabels)
	// The count of live solr nodes.
	SolrLiveNodes = metrics.NewLabelledGauge("solrman.livenode.count", "The number of problems that the solr cluster currently has", metricLabels)
	// The number of problems that we currently have
	ProblemCount = metrics.NewLabelledGauge("solrman.problem.count", "The number of problems that the solr cluster currently has", metricLabels)
)

// Runs the main solr management loop, never returns.
func (s *SolrManService) RunSolrMan() {
	s.setStatusOp("solrman is starting up")
	clusterStateGolden := time.Now()
	isAlerting := false

	// The values for metrics in this cluster
	labelVals := metrics.LabelValues{metricLabels[0]: s.ClusterName}

	var nextDisabledNotice time.Time

	first := true
	for {
		if !first {
			time.Sleep(iterationSleep)
		}
		first = false

		if s.ZooClient.State() != zk.StateHasSession {
			ZkIsConnected.Set(labelVals, 0)
			s.setStatusOp("not connected to zk")
			s.Logger.Warningf("not connected to zk")
			continue
		}
		ZkIsConnected.Set(labelVals, 1)

		InProgressOps.Set(labelVals, float64(s.countInProgressOps()))
		if s.hasInProgressOps() {
			s.clearStatusOp()
			s.Logger.Debugf("in progress ops")
			continue
		}

		if s.Storage.IsDisabled() {
			s.setStatusOp("solrman is disabled")
			s.Logger.Infof("solrman is disabled")
			if now := time.Now(); now.After(nextDisabledNotice) {
				// alert periodically while solrman is disabled
				s.AlertLog.Warningf("solrman is disabled")
				nextDisabledNotice = now.Add(disabledNoticePeriod)
			}
			continue
		} else {
			// reset
			nextDisabledNotice = time.Time{}
		}

		evacuatingNodes, err := s.Storage.GetEvacuateNodeList()
		if err != nil {
			s.setStatusOp("failed to determine hosts to evacuate")
			s.Logger.Errorf("failed to determine hosts to evacuate: %s", err)
			continue
		}

		clusterState, err := s.SolrMonitor.GetCurrentState()
		if err != nil {
			s.setStatusOp("failed to retrieve cluster state")
			s.Logger.Errorf("failed to retrieve cluster state: %s", err)
			continue
		}
		liveNodes, err := s.SolrMonitor.GetLiveNodes()
		if err != nil {
			SolrLiveNodes.Set(labelVals, 0)
			s.setStatusOp("failed to retrieve live nodes")
			s.Logger.Errorf("failed to retrieve live nodes: %s", err)
			continue
		}
		SolrLiveNodes.Set(labelVals, float64(len(liveNodes)))
		solrStatus := s.getLiveNodesStatuses(liveNodes, clusterState)

		problems := solrcheck.FindClusterProblems(s.ZooClient, clusterState, liveNodes)
		problems = append(problems, solrcheck.FindCloudStatusProblems(solrStatus, liveNodes)...)
		// Set the number of problems in the current cluster
		ProblemCount.Set(labelVals, float64(len(problems)))
		if len(problems) > 0 {
			for _, p := range problems {
				s.Logger.Infof("PROBLEM: %v", p)
			}
			if !isAlerting && time.Now().Sub(clusterStateGolden) > waitBeforeAlerting {
				isAlerting = true
				s.AlertLog.Errorf("cluster state became not golden; see logs for details")
			}
			s.setStatusOp("cluster state is not golden; waiting for cluster state to become golden")
			s.Logger.Warningf("cluster state is not golden, skipping; see logs for details")
			continue
		}

		s.clearStatusOp()

		clusterStateGolden = time.Now()
		if isAlerting {
			s.AlertLog.Infof("cluster state became golden; resuming operation")
			isAlerting = false
		}

		badlyBalancedOrgs := flagBadlyBalancedOrgs(s.Logger, s, solrStatus)
		if len(badlyBalancedOrgs) > 0 {
			s.Logger.Warningf(fmt.Sprintf("There are %d orgs with badly balanced shards.", len(badlyBalancedOrgs)))
		}

		if s.Storage.IsSplitsDisabled() {
			s.Logger.Infof("solrman splits are disabled")
		} else {
			shardSplits := computeShardSplits(s, solrStatus)
			anySplits := false
			for _, shardSplit := range shardSplits {
				if _, ok := badlyBalancedOrgs[shardSplit.Collection]; ok {
					s.Logger.Warningf("skipping split for badly balanced org %s_%s", shardSplit.Collection, shardSplit.Shard)
					continue
				}

				s.Logger.Infof("Scheduling split operation %v", shardSplit)
				result, err := s.SplitShard(shardSplit)
				if err != nil {
					s.Logger.Errorf("failed to schedule autogenerated split %+v: %s", shardSplit, err)
				} else if result.Error != "" {
					s.Logger.Warningf("failed to schedule autogenerated split %+v: %s", shardSplit, result.Error)
				} else {
					s.Logger.Infof("scheduled autogenerated split %+v", shardSplit)
					anySplits = true
				}
			}

			if anySplits {
				continue
			}
		}

		if s.Storage.IsMovesDisabled() {
			s.Logger.Infof("solrman moves are disabled")
		} else {
			// Long-running computation!
			s.setStatusOp("computing shard moves")
			shardMoves, err := computeShardMoves(solrStatus, evacuatingNodes, movesPerCycle)
			if err != nil {
				s.setStatusOp("failed to compute shard moves")
				s.Logger.Errorf("failed to compute shard moves: %s", err)
				continue
			}

			if len(shardMoves) == 0 {
				// Sleep an extra long time if everything is gucci.
				s.setStatusOp("nothing to do, everything is well balanced!")
				s.Logger.Debugf("nothing to do, everything is well balanced!")
				time.Sleep(quiescenceSleep - iterationSleep)
				continue
			}

			// Computing moves takes a long time; double-check we're in a good state before starting the moves.
			if s.ZooClient.State() != zk.StateHasSession {
				s.setStatusOp("not connected to zk")
				s.Logger.Warningf("not connected to zk")
				continue
			}

			if s.hasInProgressOps() {
				s.clearStatusOp()
				s.Logger.Debugf("in progress ops")
				continue
			}

			if s.Storage.IsDisabled() {
				s.setStatusOp("solrman is disabled")
				s.Logger.Infof("solrman is disabled")
				continue
			}

			if s.Storage.IsMovesDisabled() {
				s.clearStatusOp()
				s.Logger.Infof("solrman moves are disabled")
				continue
			}

			for _, shardMove := range shardMoves {
				result, err := s.MoveShard(shardMove)
				if err != nil {
					s.Logger.Errorf("failed to schedule autogenerated move %+v: %s", shardMove, err)
				} else if result.Error != "" {
					s.Logger.Warningf("failed to schedule autogenerated move %+v: %s", shardMove, result.Error)
				} else {
					s.Logger.Infof("scheduled autogenerated move %+v", shardMove)
				}
			}
		}
	}
}

func (s *SolrManService) countInProgressOps() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inProgressOps)
}

func (s *SolrManService) hasInProgressOps() bool {
	return s.countInProgressOps() > 0
}

// For admin visibility
func (s *SolrManService) clearStatusOp() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusOp = nil
}

// For admin visibility
func (s *SolrManService) setStatusOp(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusOp = &solrmanapi.OpRecord{
		StartedMs: nowMillis(),
		Operation: solrmanapi.OpStatus,
		Error:     status,
	}
}

// For admin visibility
func (s *SolrManService) GetStatusOp() *solrmanapi.OpRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := *s.statusOp
	return &out
}

type SplitShardRequestWithSize struct {
	solrmanapi.SplitShardRequest
	NumDocs int64
}

type byNumDocsDesc []*SplitShardRequestWithSize

func (s byNumDocsDesc) Len() int {
	return len(s)
}
func (s byNumDocsDesc) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s byNumDocsDesc) Less(i, j int) bool {
	return s[i].NumDocs > s[j].NumDocs
}

// MaxInt64 returns the maximum of int64 values.
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinInt64 returns the minimum of int64 values.
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func flagBadlyBalancedOrgs(logger smutil.Logger, s *SolrManService, clusterState solrmanapi.SolrCloudStatus) map[string]bool {
	orgToMin := make(map[string]int64)
	orgToMax := make(map[string]int64)

	getOrg := func(coreName string) string {
		return strings.Split(coreName, "_")[0]
	}

	for _, v := range clusterState {
		for coreName, status := range v.Cores {
			org := getOrg(coreName)

			if min, ok := orgToMin[org]; ok {
				orgToMin[org] = MinInt64(min, status.NumDocs)
			} else {
				orgToMin[org] = status.NumDocs
			}

			if max, ok := orgToMax[org]; ok {
				orgToMax[org] = MaxInt64(max, status.NumDocs)
			} else {
				orgToMax[org] = status.NumDocs
			}
		}
	}

	badlyBalancedOrgs := make(map[string]bool)
	for org, max := range orgToMax {
		if (2+float64(orgToMin[org]))/(2+float64(max)) < allowedMinToMaxShardSizeRatio {
			badlyBalancedOrgs[org] = true
			logger.Warningf("Shards are getting imbalanced for org: " + org)
		}
	}

	return badlyBalancedOrgs
}

func computeShardSplits(s *SolrManService, clusterState solrmanapi.SolrCloudStatus) []*solrmanapi.SplitShardRequest {
	machineToSplitOps := make(map[string][]*SplitShardRequestWithSize)
	anyShardTooBig := false
	for machine, v := range clusterState {
		for _, status := range v.Cores {
			// Continue if this collection has too many shards i.e greater than maxShardsPerMachine * num solr machines
			if collstate, err := s.SolrMonitor.GetCollectionState(status.Collection); err != nil {
				continue
			} else if len(collstate.Shards) > maxShardsPerMachine*len(clusterState) {
				continue
			}

			if status.NumDocs > allSplitsDocCountTrigger {
				anyShardTooBig = true
			}

			if status.NumDocs > splitShardsWithDocCount {
				x := &SplitShardRequestWithSize{
					solrmanapi.SplitShardRequest{
						Collection: status.Collection,
						Shard:      status.Shard,
					},
					status.NumDocs,
				}
				machineToSplitOps[machine] = append(machineToSplitOps[machine], x)
			}
		}
	}

	if !anyShardTooBig {
		return nil
	}

	// sort by biggest to smallest shards
	// keep biggest splitsPerMachine shards only
	// flatten and return

	splitOps := make([]*solrmanapi.SplitShardRequest, 0)
	for _, ops := range machineToSplitOps {
		sort.Sort(byNumDocsDesc(ops))
		for i, op := range ops {
			if i == splitsPerMachine {
				break
			}
			splitOps = append(splitOps, &op.SplitShardRequest)
		}
	}

	return splitOps
}

func computeShardMoves(clusterState solrmanapi.SolrCloudStatus, evacuatingNodes []string, count int) ([]*solrmanapi.MoveShardRequest, error) {
	model, err := createModel(clusterState, evacuatingNodes)
	if err != nil {
		return nil, err
	}

	// Leave 1 cores open for handling queries / etc.
	numCPU := runtime.GOMAXPROCS(0) - 1
	if numCPU < 1 {
		numCPU = 1
	}

	// Compute more moves than we actually need, so we can whittle the results down after.
	moves := model.ComputeBestMoves(numCPU, count)

	var shardMoves []*solrmanapi.MoveShardRequest
	if len(moves) > 0 {
		shardMoves = make([]*solrmanapi.MoveShardRequest, len(moves))
		for i, m := range moves {
			shardMoves[i] = &solrmanapi.MoveShardRequest{
				Collection: m.Core.Collection,
				Shard:      m.Core.Shard,
				SrcNode:    m.FromNode.Address,
				DstNode:    m.ToNode.Address,
			}
		}
	}

	return shardMoves, nil
}

func createModel(clusterState solrmanapi.SolrCloudStatus, evacuatingNodes []string) (*smmodel.Model, error) {
	var currentNode *smmodel.Node
	m := &smmodel.Model{}
	seenNodeNames := map[string]bool{}
	collectionMap := make(map[string]*smmodel.Collection)
	evacuatingNodeSet := map[string]bool{}
	for _, n := range evacuatingNodes {
		evacuatingNodeSet[n] = true
	}

	for _, nodeStatus := range clusterState {
		if seenNodeNames[nodeStatus.NodeName] {
			return nil, smutil.Errorf("already seen: %v", nodeStatus.NodeName)
		}
		seenNodeNames[nodeStatus.NodeName] = true
		currentNode = &smmodel.Node{
			Name:       nodeStatus.Hostname,
			Address:    nodeStatus.NodeName,
			Evacuating: evacuatingNodeSet[nodeStatus.Hostname] || evacuatingNodeSet[nodeStatus.NodeName],
		}
		m.AddNode(currentNode)

		for _, coreStatus := range nodeStatus.Cores {
			collName := coreStatus.Collection
			collection := collectionMap[collName]
			if collection == nil {
				collection = &smmodel.Collection{Name: collName}
				collectionMap[collName] = collection
				m.Collections = append(m.Collections, collection)
			}

			core := &smmodel.Core{
				Name:       coreStatus.Name,
				Collection: collName,
				Shard:      coreStatus.Shard,
				Docs:       float64(coreStatus.NumDocs),
				Size:       float64(coreStatus.IndexSize),
			}
			collection.Add(core)
			currentNode.Add(core)
			m.Add(core)
		}
	}
	return m, nil
}

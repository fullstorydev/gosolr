package solrcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/go-zookeeper/zk"
)

func TestFindClusterProblems(t *testing.T) {
	b, err := ioutil.ReadFile("./problems_test_data.json")
	if err != nil {
		t.Fatalf("Failed to read test data file: %s", err.Error())
	}
	var data testData
	if err := json.Unmarshal(b, &data); err != nil {
		t.Fatalf("Failed to parse test data file: %s", err.Error())
	}

	// construct our input data from the loaded test data file
	clusterState := solrmonitor.ClusterState{}
	solrStatus := solrmanapi.SolrCloudStatus{}
	zkState := fakeZk{}

	for collName, coll := range data.ClusterState {
		collState := &solrmonitor.CollectionState{Shards: map[string]solrmonitor.ShardState{}}
		clusterState[collName] = collState
		for shardName, shard := range coll {
			shardState := solrmonitor.ShardState{
				State:    shard.State,
				Range:    shard.HashRange,
				Replicas: map[string]solrmonitor.ReplicaState{},
			}
			collState.Shards[shardName] = shardState
			for replicaName, replica := range shard.Replicas {
				replicaState := solrmonitor.ReplicaState{
					State:    replica.State,
					Core:     replica.Core,
					BaseUrl:  replica.BaseURL,
					Leader:   strconv.FormatBool(replica.Leader),
					NodeName: replica.NodeName,
				}
				shardState.Replicas[replicaName] = replicaState
				nodeStatus, ok := solrStatus[replica.NodeName]
				if !ok {
					nodeStatus = &solrmanapi.SolrNodeStatus{
						NodeName: replica.NodeName,
						Hostname: replica.NodeName,
						Cores:    map[string]*solrmanapi.SolrCoreStatus{},
					}
					solrStatus[replica.NodeName] = nodeStatus
				}
				coreStatus := &solrmanapi.SolrCoreStatus{
					Name:         replica.Core,
					Collection:   collName,
					Shard:        shardName,
					Replica:      replicaName,
					NodeName:     replica.NodeName,
					Range:        shard.HashRange,
					ShardState:   shard.State,
					ReplicaState: replica.State,
					IsLeader:     replica.Leader,
					HasStats:     false,
					IndexSize:    -1,
					NumDocs:      -1,
				}
				if replica.Stats != nil {
					coreStatus.HasStats = true
					coreStatus.NumDocs = replica.Stats.DocCount
					coreStatus.IndexSize = replica.Stats.IndexSize
				}
				nodeStatus.Cores[replica.Core] = coreStatus
			}
			if shard.ZkLeader != nil {
				zkState[fmt.Sprintf("%s:%s", collName, shardName)] = &zkLeaderNode{
					Core:         shard.ZkLeader.Core,
					BaseUrl:      shard.ZkLeader.BaseURL,
					CoreNodeName: shard.ZkLeader.CoreNodeName,
					NodeName:     shard.ZkLeader.NodeName,
				}
			}
		}
	}

	// compute all problems
	problems := FindClusterProblems(zkState, clusterState, data.LiveNodes)
	problems = append(problems, FindCloudStatusProblems(solrStatus, data.LiveNodes)...)

	// verify they match expectations
	expected := []*ClusterProblem{
		nodeProblem(ProblemNodeDown, "nodeE", ""),
		shardProblem(ProblemInactiveShard, "coll-1", "shard-down", "down"),
		shardProblem(ProblemInactiveShard, "coll-1", "shard-inactive", "inactive"),
		shardProblem(ProblemBadHashRange, "coll-2", "shard-bad-hash-range", "foo-bar"),
		shardProblem(ProblemBadHashRange, "coll-2", "shard-no-hash-range", ""),
		replicaProblem(ProblemCoreStatusFail, "coll-3", "shard-node-down", "nodeE", ""),
		replicaProblem(ProblemNegativeNumDocs, "coll-4", "shard-bad-stats", "nodeC", "-100"),
		replicaProblem(ProblemNegativeIndexSize, "coll-4", "shard-bad-stats", "nodeC", "-101010"),
		shardLeaderProblem(ProblemMismatchLeader, "coll-4", "shard-funny-leader",
			`core="coll-4:shard-funny-leader:replica-2", base url="http://foo/bar/replica-BAD", node name="nodeA"`,
			`core="coll-4:shard-funny-leader:replica-2", base url="http://foo/bar/replica-2", node name="nodeA"`,
			"replica-2_nodeA", ""),
		replicaProblem(ProblemInactiveReplica, "coll-4", "shard-inactive-replicas", "nodeA", "inactive"),
		replicaProblem(ProblemInactiveReplica, "coll-4", "shard-inactive-replicas", "nodeC", "down"),
		shardLeaderProblem(ProblemLeaderCount, "coll-4", "shard-multi-leaders", "1", "2", "replica-1_nodeB", "replica-1_nodeB, replica-2_nodeC"),
		shardLeaderProblem(ProblemWrongLeader, "coll-4", "shard-multi-leaders", "replica-1_nodeB", "replica-2_nodeC", "replica-1_nodeB", ""),
		shardLeaderProblem(ProblemLeaderCount, "coll-4", "shard-no-leader", "1", "0", "replica-BAD", ""),
		shardProblem(ProblemNoReplicas, "coll-4", "shard-no-replicas", ""),
		shardLeaderProblem(ProblemWrongLeader, "coll-4", "shard-wrong-leader", "replica-1_nodeD", "", "replica-1_nodeD", ""),
		shardLeaderProblem(ProblemWrongLeader, "coll-4", "shard-wrong-leader", "replica-1_nodeD", "replica-2_nodeA", "replica-1_nodeD", ""),
		shardError("coll-4", "shard-zk-error", zk.ErrConnectionClosed),
		replicaProblem(ProblemCoreStatusFail, "coll-5", "shard-no-core-stats", "nodeC", ""),
		shardProblem(ProblemNoZkElection, "coll-5", "shard-no-election", ""),
		collProblem(ProblemMissingHashRange, "coll-6", "80000000-8fffffff,68000000-6fffffff,78000000-7fffffff"),
		collProblem(ProblemAmbiguousHashRange, "coll-6", "f8000000-ffffffff,50000000-5fffffff"),
	}
	expectedSet := map[ClusterProblem]struct{}{}
	for _, p := range expected {
		expectedSet[*p] = struct{}{}
	}

	// compute set differences and report if not empty
	actualSet := map[ClusterProblem]struct{}{}
	unwantedProblems := clusterProblems{}
	for _, p := range problems {
		actualSet[*p] = struct{}{}
		if _, ok := expectedSet[*p]; !ok {
			// expected, but not found in actual problems
			unwantedProblems = append(unwantedProblems, p)
		}
	}
	missingProblems := clusterProblems{}
	for _, p := range expected {
		if _, ok := actualSet[*p]; !ok {
			// expected, but not found in actual problems
			missingProblems = append(missingProblems, p)
		}
	}

	if len(missingProblems) > 0 {
		sort.Sort(missingProblems)
		t.Errorf("Some problems were not reported:\n%s", problemsAsString(missingProblems))
	}
	if len(unwantedProblems) > 0 {
		sort.Sort(unwantedProblems)
		t.Errorf("Some problems reported that should not have been:\n%s", problemsAsString(unwantedProblems))
	}
}

func problemsAsString(problems []*ClusterProblem) string {
	var buf bytes.Buffer
	first := true
	for _, p := range problems {
		if first {
			first = false
		} else {
			buf.WriteString("\n")
		}
		buf.WriteString(p.String())
	}
	return buf.String()
}

type testData struct {
	LiveNodes    []string                       `json:"live_nodes"`
	ClusterState map[string]map[string]struct { // keyed by collection name, then shard name
		State     string              `json:"state"`
		HashRange string              `json:"hash_range"`
		Replicas  map[string]struct { // keyed by core node name of replica
			Leader   bool   `json:"leader"`
			State    string `json:"state"`
			Core     string `json:"core"`
			BaseURL  string `json:"base_url"`
			NodeName string `json:"node_name"`
			Stats    *struct {
				DocCount  int64 `json:"doc_count"`
				IndexSize int64 `json:"index_size"`
			} `json:"stats"`
		} `json:"replicas"`
		ZkLeader *struct {
			Core         string `json:"core"`
			CoreNodeName string `json:"core_node_name"`
			BaseURL      string `json:"base_url"`
			NodeName     string `json:"node_name"`
		} `json:"zk_leader"`
	} `json:"cluster_state"`
}

type fakeZk map[string]*zkLeaderNode

var pathPattern = regexp.MustCompile("\\/solr\\/collections\\/([^/]+)\\/leaders\\/([^/]+)\\/leader")

func (z fakeZk) Get(path string) ([]byte, *zk.Stat, error) {
	if !pathPattern.MatchString(path) {
		return nil, nil, fmt.Errorf("Could not parse given path: %s", path)
	}
	matches := pathPattern.FindStringSubmatch(path)
	coll, shard := matches[1], matches[2]
	if shard == "shard-zk-error" {
		return nil, nil, zk.ErrConnectionClosed
	}
	leader := z[fmt.Sprintf("%s:%s", coll, shard)]
	if leader == nil {
		return nil, nil, zk.ErrNoNode
	}
	if b, err := json.Marshal(leader); err != nil {
		return nil, nil, err
	} else {
		return b, &zk.Stat{}, nil
	}
}

type clusterProblems []*ClusterProblem

func (p clusterProblems) Len() int { return len(p) }

func (p clusterProblems) Less(i, j int) bool {
	p1 := p[i]
	p2 := p[j]
	if p1.Collection < p2.Collection {
		return true
	} else if p1.Collection > p2.Collection {
		return false
	}

	if p1.Shard < p2.Shard {
		return true
	} else if p1.Shard > p2.Shard {
		return false
	}

	if p1.Node < p2.Node {
		return true
	} else if p1.Node > p2.Node {
		return false
	}

	if p1.Kind < p2.Kind {
		return true
	}
	return false
}

func (p clusterProblems) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

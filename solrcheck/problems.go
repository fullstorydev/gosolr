package solrcheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/fullstorydev/gosolr/solrmonitor"
	"github.com/fullstorydev/gosolr/solrman/solrmanapi"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
)

// ClusterProblem represents an issue identified with the current solr cluster status.
type ClusterProblem struct {
	Kind       ProblemKind

	// At least one (but possibly two or even all three) of the following defines where
	// the problem exists.

	Collection string // may be empty if problem cuts across multiple collections
	Shard      string // may be empty if problem cuts across multiple shards
	Node       string // may be empty if problem cuts across multiple nodes

	// Problem details. For "mismatch" problem kinds, Expected and Actual are filled in.
	// For leader election problems, if a node is known to be leader per ZK election, then
	// the LeaderName field indicates its core node name. If the problem was caused by a
	// go runtime error, then the Error field is set. Any additional info will be provided via
	// the OtherDetails field.

	Expected     string
	Actual       string
	LeaderName   string
	OtherDetails string
	Error        error
}

func (p *ClusterProblem) String() string {
	var buf bytes.Buffer
	if p.Collection != "" && p.Shard != "" {
		fmt.Fprintf(&buf, "%s, %s: ", p.Collection, p.Shard)
	} else if p.Collection != "" {
		fmt.Fprintf(&buf, "%s: ", p.Collection)
	}
	if p.Node != "" {
		fmt.Fprintf(&buf, "%s: ", p.Node)
	}
	buf.WriteString(p.Kind.String())
	if p.OtherDetails != "" {
		fmt.Fprintf(&buf, ": %s", p.OtherDetails)
	}
	if p.Error != nil {
		fmt.Fprintf(&buf, ": %s", p.Error.Error())
	}

	return buf.String()
}

// ProblemKind categorizes cluster problems.
type ProblemKind int

const (
	ProblemInactiveShard ProblemKind = iota // details will be shard state
	ProblemNoReplicas
	ProblemLeaderCount // actual indicates number of observed leader replicas (expected to be 1)
	ProblemNoZkElection
	ProblemWrongLeader // leader name will be set; expected is the ZK leader, actual is name of wrong replica marked as leader or empty to indicate replica that should be leader is not so marked
	ProblemMismatchLeader // leader name will be set; expected is attributes of leader in ZK, actual is attributes of leader in replica state
	ProblemBadHashRange // details will be shard's configured hash range
	ProblemInactiveReplica // details will be replica state
	ProblemMissingHashRange // details will be comma-separated list of omitted hash ranges
	ProblemAmbiguousHashRange // details will be comma-separated list of overlapping hash ranges
	ProblemNodeDown
	ProblemCoreStatusFail
	ProblemNegativeNumDocs // details will be core's document count
	ProblemNegativeIndexSize // details will be core's index size
	ProblemError // error will be set with more info
)

var problemNames = map[ProblemKind]string{
	ProblemInactiveShard: "shard not active",
	ProblemNoReplicas: "shard has no replicas",
	ProblemLeaderCount: "shard leader count != 1",
	ProblemNoZkElection: "shard has no ZK leader election",
	ProblemWrongLeader: "shard leader replica does not match ZK leader election",
	ProblemMismatchLeader: "shard leader replica has attributes inconsistent with ZK leader election",
	ProblemBadHashRange: "shard has a bad hash range defined",
	ProblemInactiveReplica: "replica not active",
	ProblemMissingHashRange: "collection is missing a hash range",
	ProblemAmbiguousHashRange: "collection has overlapping hash ranges",
	ProblemNodeDown: "node down",
	ProblemCoreStatusFail: "failed to query status of core",
	ProblemNegativeNumDocs: "negative document count",
	ProblemNegativeIndexSize: "negative index size",
	ProblemError: "error",
}

func (k ProblemKind) String() string {
	return problemNames[k]
}

func nodeProblem(kind ProblemKind, node, details string) *ClusterProblem {
	return &ClusterProblem{
		Kind: kind,
		Node: node,
		OtherDetails: details,
	}
}

func collProblem(kind ProblemKind, coll, details string) *ClusterProblem {
	return &ClusterProblem{
		Kind: kind,
		Collection: coll,
		OtherDetails: details,
	}
}

func shardProblem(kind ProblemKind, coll, shard, details string) *ClusterProblem {
	return &ClusterProblem{
		Kind: kind,
		Collection: coll,
		Shard: shard,
		OtherDetails: details,
	}
}

func shardLeaderProblem(kind ProblemKind, coll, shard, expected, actual, leaderName, extraDetails string) *ClusterProblem {
	var details string
	if actual == "" {
		details = fmt.Sprintf("Expected %s; but not found", expected)
	} else {
		details = fmt.Sprintf("Expected %s; got %s", expected, actual)
	}
	if extraDetails != "" {
		details = fmt.Sprintf("%s: %s", details, extraDetails)
	}
	return &ClusterProblem{
		Kind: kind,
		Collection: coll,
		Shard: shard,
		Expected: expected,
		Actual: actual,
		LeaderName: leaderName,
		OtherDetails: details,
	}
}

func shardError(coll, shard string, err error) *ClusterProblem {
	return &ClusterProblem{
		Kind: ProblemError,
		Collection: coll,
		Shard: shard,
		Error: err,
	}
}

func replicaProblem(kind ProblemKind, coll, shard, node, details string) *ClusterProblem {
	return &ClusterProblem{
		Kind: kind,
		Collection: coll,
		Shard: shard,
		Node: node,
		OtherDetails: details,
	}
}

type hashRange struct {
	lo int32
	hi int32
}

func (r hashRange) String() string {
	return fmt.Sprintf("%08x-%08x", uint32(r.lo), uint32(r.hi))
}

type hashRangeSet map[hashRange]struct{}

func (rs hashRangeSet) String() string {
	// print them in sorted order
	slice := make([]hashRange, 0, len(rs))
	for r := range rs {
		slice = append(slice, r)
	}
	sort.Sort(byLoTerm(slice))

	first := true
	var buf bytes.Buffer
	for _, r := range slice {
		if first {
			first = false
		} else {
			buf.WriteString(",")
		}
		buf.WriteString(r.String())
	}
	return buf.String()
}

// Sort by lo term first, then high term on ties so that the smaller slice range comes first
type byLoTerm []hashRange

func (p byLoTerm) Len() int { return len(p) }

func (p byLoTerm) Less(i, j int) bool {
	if p[i].lo < p[j].lo {
		return true
	} else if p[i].lo > p[j].lo {
		return false
	}
	return p[i].hi < p[j].hi
}

func (p byLoTerm) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// FindClusterProblems examines the given cluster state and set of live nodes in order to identify any
// anomalies in solr cluster configuration.
func FindClusterProblems(zkConn zkGetter, clusterState solrmonitor.ClusterState, solrStatus solrmanapi.SolrCloudStatus, liveNodes []string) []*ClusterProblem {
	liveNodeSet := map[string]bool{}
	for _, liveNode := range liveNodes {
		liveNodeSet[liveNode] = true
	}
	missingLiveNodes := map[string]bool{}

	var problems []*ClusterProblem

	for collName, coll := range clusterState {
		hashRanges := []hashRange{} // collect covered hash ranges for the collection
		for shard, shardState := range coll.Shards {
			if !shardState.IsActive() {
				problems = append(problems, shardProblem(ProblemInactiveShard, collName, shard, shardState.State))
			}
			if shardState.Range == "" {
				problems = append(problems, shardProblem(ProblemBadHashRange, collName, shard, shardState.Range))
			} else if lo, hi, err := shardState.RangeBounds(); err != nil {
				problems = append(problems, shardProblem(ProblemBadHashRange, collName, shard, shardState.Range))
			} else {
				hashRanges = append(hashRanges, hashRange{lo, hi})
			}

			if len(shardState.Replicas) == 0 {
				problems = append(problems, shardProblem(ProblemNoReplicas, collName, shard, ""))
				continue
			}

			zkLeader, err := queryElectedLeaderFromZk(zkConn, collName, shard)
			if err != nil {
				if err == zk.ErrNoNode {
					problems = append(problems, shardProblem(ProblemNoZkElection, collName, shard, ""))
				} else {
					problems = append(problems, shardError(collName, shard, err))
				}
			}
			zkLeaderName := ""
			if zkLeader != nil {
				zkLeaderName = zkLeader.CoreNodeName
			}
			var leaders []string

			for coreNodeName, replica := range shardState.Replicas {
				nodeName := replica.NodeName
				if !liveNodeSet[nodeName] {
					// only include one problem for this node (regardless of how many cores it has)
					if !missingLiveNodes[nodeName] {
						missingLiveNodes[nodeName] = true
						problems = append(problems, nodeProblem(ProblemNodeDown, nodeName, ""))
					}
				} else {
					// was in list of live nodes, so we'll have a core status
					coreStatus := solrStatus[nodeName].Cores[replica.Core]
					if !coreStatus.HasStats {
						problems = append(problems, replicaProblem(ProblemCoreStatusFail, collName, shard, nodeName, ""))
					} else {
						if coreStatus.IndexSize < 0 {
							problems = append(problems, replicaProblem(ProblemNegativeIndexSize, collName, shard, nodeName, fmt.Sprintf("%d", coreStatus.IndexSize)))
						}
						if coreStatus.NumDocs < 0 {
							problems = append(problems, replicaProblem(ProblemNegativeNumDocs, collName, shard, nodeName, fmt.Sprintf("%d", coreStatus.NumDocs)))
						}
					}
				}

				if !replica.IsActive() {
					problems = append(problems, replicaProblem(ProblemInactiveReplica, collName, shard, nodeName, replica.State))
				}

				// see if ZK leader info agrees with replica state
				if replica.IsLeader() {
					leaders = append(leaders, coreNodeName)
					if zkLeader != nil {
						if coreNodeName != zkLeader.CoreNodeName {
							problems = append(problems, shardLeaderProblem(ProblemWrongLeader, collName, shard, zkLeaderName, coreNodeName, zkLeaderName, ""))
						} else if replica.Core != zkLeader.Core || replica.BaseUrl != zkLeader.BaseUrl || replica.NodeName != zkLeader.NodeName {
							expected := fmt.Sprintf("core=%q, base url=%q, node name=%q", zkLeader.Core, zkLeader.BaseUrl, zkLeader.NodeName)
							actual := fmt.Sprintf("core=%q, base url=%q, node name=%q", replica.Core, replica.BaseUrl, replica.NodeName)
							problems = append(problems, shardLeaderProblem(ProblemMismatchLeader, collName, shard, expected, actual, zkLeaderName, ""))
						}
					}
				} else {
					if zkLeader != nil && coreNodeName == zkLeader.CoreNodeName {
						problems = append(problems, shardLeaderProblem(ProblemWrongLeader, collName, shard, zkLeaderName, "", zkLeaderName, ""))
					}
				}
			}

			if len(leaders) != 1 {
				extraDetails := ""
				if len(leaders) > 1 {
					// enumerate all leaders (make sure they are sorted for deterministic output)
					sort.Strings(leaders)
					extraDetails = strings.Join(leaders, ", ")
				}
				problems = append(problems, shardLeaderProblem(ProblemLeaderCount, collName, shard, "1", fmt.Sprintf("%d", len(leaders)), zkLeaderName, extraDetails))
			}
		}

		// Report on hash ranges
		missing := hashRangeSet{}
		ambiguous := hashRangeSet{}
		if len(hashRanges) == 0 {
			missing[hashRange{math.MinInt32, math.MaxInt32}] = struct{}{}
		} else {
			sort.Sort(byLoTerm(hashRanges))
			lastEnd := int32(math.MinInt32)
			for i, hr := range hashRanges {
				if i == 0 {
					if hr.lo > lastEnd {
						missing[hashRange{lastEnd, hr.lo-1}] = struct{}{}
					}
				} else if hr.lo <= lastEnd {
					ambiguous[hashRange{hr.lo, lastEnd}] = struct{}{}
				} else if hr.lo > lastEnd+1 {
					missing[hashRange{lastEnd+1, hr.lo-1}] = struct{}{}
				}
				if hr.hi > lastEnd {
					lastEnd = hr.hi
				}
			}
			if lastEnd < math.MaxInt32 {
				missing[hashRange{lastEnd+1, math.MaxInt32}] = struct{}{}
			}
		}

		if len(missing) > 0 {
			problems = append(problems, collProblem(ProblemMissingHashRange, collName, missing.String()))
		}
		if len(ambiguous) > 0 {
			problems = append(problems, collProblem(ProblemAmbiguousHashRange, collName, ambiguous.String()))
		}
	}

	return problems
}

type zkLeaderNode struct {
	Core         string `json:"core"`
	CoreNodeName string `json:"core_node_name"`
	BaseUrl      string `json:"base_url"`
	NodeName     string `json:"node_name"`
}

type zkGetter interface {
	Get(path string) ([]byte, *zk.Stat, error)
}

func queryElectedLeaderFromZk(zkConn zkGetter, coll, shard string) (*zkLeaderNode, error) {
	// TODO(jh): cache/watch these in solrmonitor instead of issuing GETs for every shard
	leaderPath := fmt.Sprintf("/solr/collections/%s/leaders/%s/leader", coll, shard)
	data, _, err := zkConn.Get(leaderPath)
	if err != nil {
		return nil, err
	}
	var leader zkLeaderNode
	if err := json.Unmarshal(data, &leader); err != nil {
		return nil, err
	}
	return &leader, nil
}

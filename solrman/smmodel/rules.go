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

package smmodel

type Rule interface {
	GetWeight() float64
	Score(*Model) float64
}

// Compute an optimal cost as if value appeared N times.
func costOptimal(value float64, n int) float64 {
	return value * value * float64(n)
}

// Score based on core count being balanced across nodes.
type RuleCoreBalance struct {
}

func (r *RuleCoreBalance) GetWeight() float64 {
	return 2.0
}

func (r *RuleCoreBalance) Score(m *Model) float64 {
	coreCount := len(m.cores)
	nodeCount := len(m.Nodes)

	// An optimal cost would be evenly dividing the cores across the nodes.
	optimalCoresPerNode := float64(coreCount) / float64(nodeCount)

	var costActual float64
	for _, node := range m.Nodes {
		nodeCost := float64(node.coreCount)
		nodeCost *= nodeCost
		costActual += nodeCost
	}
	return costOptimal(optimalCoresPerNode, nodeCount) / costActual
}

// Score based on doc count being balanced across nodes.
type RuleDocBalance struct {
}

func (r *RuleDocBalance) GetWeight() float64 {
	return 1.0
}

func (r *RuleDocBalance) Score(m *Model) float64 {
	nodeCount := len(m.Nodes)

	// An optimal cost would be evenly dividing the docs across the nodes.
	optimalDocsPerNode := m.Docs / float64(nodeCount)

	var costActual float64
	for _, node := range m.Nodes {
		nodeCost := node.Docs
		nodeCost *= nodeCost
		costActual += nodeCost
	}
	return costOptimal(optimalDocsPerNode, nodeCount) / costActual
}

// Score based on byte size being balanced across nodes.
type RuleSizeBalance struct {
}

func (r *RuleSizeBalance) GetWeight() float64 {
	return 1.0
}

func (r *RuleSizeBalance) Score(m *Model) float64 {
	nodeCount := len(m.Nodes)

	// An optimal cost would be evenly dividing the size across the nodes.
	optimalSizePerNode := m.Size / float64(nodeCount)

	var costActual float64
	for _, node := range m.Nodes {
		nodeCost := node.Size
		nodeCost *= nodeCost
		costActual += nodeCost
	}
	return costOptimal(optimalSizePerNode, nodeCount) / costActual
}

// Score based on collection-level core count being balanced across nodes.
type RuleCollectionCoreBalance struct {
}

func (r *RuleCollectionCoreBalance) GetWeight() float64 {
	// We want each collection's balance to be worth 1/10 of the global core balance to prioritize org balance.
	// So we give this rule a 0.2 weight, but the score (below) adds together the balance for every collection.
	return 1.0
}

func (r *RuleCollectionCoreBalance) Score(m *Model) float64 {
	var totalScore float64
	for _, collection := range m.Collections {
		if len(collection.cores) > 1 {
			totalScore += r.scoreCollection(collection, m)
		} else {
			totalScore += 1
		}
	}
	return totalScore
}

func (r *RuleCollectionCoreBalance) scoreCollection(collection *Collection, m *Model) float64 {
	coreCount := len(collection.cores)
	nodeCount := len(m.Nodes)

	// An optimal cost would be evenly dividing the cores across the nodes.
	optimalCoresPerNode := float64(coreCount) / float64(nodeCount)

	var costActual float64
	for _, node := range m.Nodes {
		count := 0
		for _, core := range collection.cores {
			if node.Contains(core) {
				count++
			}
		}
		nodeCost := float64(count)
		nodeCost *= nodeCost
		costActual += nodeCost
	}

	return costOptimal(optimalCoresPerNode, nodeCount) / costActual
}

var (
	Rules = []Rule{
		&RuleCoreBalance{},
		&RuleDocBalance{},
		&RuleSizeBalance{},
		&RuleCollectionCoreBalance{},
	}
)

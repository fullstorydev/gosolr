package smmodel

import "fmt"

// Compute an optimal cost as if value appeared N times.
func costOptimal(value float64, n int) float64 {
	return value * value * float64(n)
}

// Create a composite cost for the model, based on hard-coded rules. Lower cost is better.
func (m *Model) Cost() float64 {
	if len(m.cores) == 0 {
		return 0
	}

	if m.cost == 0 {
		coreCount := len(m.cores)
		nodeCount := len(m.Nodes)
		activeCount := nodeCount
		// don't consider nodes we are evacuating when calculating the optimal cores/docs/size per node
		for _, node := range m.Nodes {
			if node.Evacuating {
				activeCount--
			}
		}

		// An optimal cost would be evenly dividing the cores, docs, and size across the nodes.
		optimalCoresPerNode := float64(coreCount) / float64(activeCount)
		sqOptCores := optimalCoresPerNode * optimalCoresPerNode

		optimalDocsPerNode := m.Docs / float64(activeCount)
		sqOptDocs := optimalDocsPerNode * optimalDocsPerNode

		optimalSizePerNode := m.Size / float64(activeCount)
		sqOptSize := optimalSizePerNode * optimalSizePerNode

		for _, node := range m.Nodes {
			nodeCost := node.Cost(sqOptCores, sqOptDocs, sqOptSize)
			m.cost += nodeCost
			if debug >= 2 {
				fmt.Printf("node %s cost %f: %d %f %f vs %f %f %f\n", node.Address, nodeCost,
					node.coreCount, node.Docs, node.Size,
					optimalCoresPerNode, optimalDocsPerNode, optimalSizePerNode,
				)
			}
		}

		for _, coll := range m.Collections {
			collCost := coll.Cost(nodeCount, activeCount)
			m.cost += collCost
			if debug >= 3 {
				fmt.Printf("coll %s cost %f\n", coll.Name, collCost)
			}
		}
	}
	return m.cost
}

// Create a composite cost for the node, based on hard-coded rules. Lower cost is better.
func (n *Node) Cost(optimalCoresPerNode, optimalDocsPerNode, optimalSizePerNode float64) float64 {
	if n.coreCount == 0 {
		return 0
	}

	if n.Evacuating {
		// Cores on an evacuating node should generate a super bad score.
		optimalCoresPerNode, optimalDocsPerNode, optimalSizePerNode = 0.01, 0.01, 0.01
	}

	if n.cost == 0 {
		n.cost = float64(2*n.coreCount*n.coreCount) / optimalCoresPerNode
		n.cost += n.Docs * n.Docs / optimalDocsPerNode
		n.cost += n.Size * n.Size / optimalSizePerNode
	}
	return n.cost
}

// Create a composite cost for the collection, based on hard-coded rules. Lower cost is better.
func (c *Collection) Cost(nodeCount, activeCount int) float64 {
	coreCount := len(c.cores)
	if coreCount == 0 {
		return 0
	}

	if coreCount == 1 {
		return 1
	}

	if c.cost == 0 {
		scores := make([]int, nodeCount)
		for _, core := range c.cores {
			scores[core.nodeId]++
		}

		for _, score := range scores {
			c.cost += float64(score * score)
		}

		optimalCoresPerNode := float64(coreCount) / float64(activeCount)
		c.cost = c.cost / costOptimal(optimalCoresPerNode, activeCount)

	}
	return c.cost
}

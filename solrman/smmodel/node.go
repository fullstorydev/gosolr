package smmodel

import "fmt"

// unique id of a node for internal tracking, 0-based and contiguous so we can store in slices by id
type nodeId int

type Node struct {
	Name       string `json:"name"`
	Address    string `json:"address"`
	Evacuating bool   `json:"evacuating"`
	Size       int64  `json:"size"` // in bytes

	id nodeId

	coreCount int
}

func (n *Node) Add(core *Core) {
	n.Size += core.Size
	n.coreCount += 1
	core.nodeId = n.id
}

func (n *Node) Contains(core *Core) bool {
	return core.nodeId == n.id
}

func (n *Node) With(core *Core) *Node {
	if !n.Contains(core) {
		panic(fmt.Sprintf("core %s assigned to %d cannot be added to %d", core.Name, core.nodeId, n.id))
	}

	nCopy := *n
	nCopy.Size += core.Size
	nCopy.coreCount++
	return &nCopy
}

func (n *Node) Without(core *Core) *Node {
	if !n.Contains(core) {
		return n
	}

	nCopy := *n
	nCopy.Size -= core.Size
	nCopy.coreCount--
	return &nCopy
}

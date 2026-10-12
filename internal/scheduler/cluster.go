// Package scheduler places Pending instances on nodes using a two-phase
// filter (hard constraints) then score (soft preferences) pipeline.
package scheduler

import (
	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// NodeInfo is a node plus what is currently placed on it.
type NodeInfo struct {
	Node      api.Node
	Requested api.Resources
	// Workloads counts replicas per workload on this node.
	Workloads map[string]int
}

// Free returns allocatable minus requested.
func (n *NodeInfo) Free() api.Resources { return n.Node.Allocatable.Sub(n.Requested) }

// Cluster is a point-in-time view of the fleet used for one scheduling pass.
type Cluster struct {
	Nodes   map[string]*NodeInfo
	Order   []string // node names, sorted, for deterministic iteration
	Volumes map[string]api.Volume
	// ZoneCount counts replicas per workload per zone.
	ZoneCount map[string]map[string]int
}

// NewCluster builds a Cluster view from store contents.
func NewCluster(nodes []api.Node, instances []api.Instance, volumes []api.Volume) *Cluster {
	c := &Cluster{
		Nodes:     map[string]*NodeInfo{},
		Volumes:   map[string]api.Volume{},
		ZoneCount: map[string]map[string]int{},
	}
	for _, n := range nodes {
		c.Nodes[n.Name] = &NodeInfo{Node: n, Workloads: map[string]int{}}
		c.Order = append(c.Order, n.Name)
	}
	for _, v := range volumes {
		c.Volumes[v.Name] = v
	}
	for i := range instances {
		if instances[i].Holds() {
			c.Assume(&instances[i], instances[i].Node)
		}
	}
	return c
}

// Assume records an instance as placed on node, so later decisions in the
// same pass see the capacity as used.
func (c *Cluster) Assume(inst *api.Instance, node string) {
	ni, ok := c.Nodes[node]
	if !ok {
		return
	}
	ni.Requested = ni.Requested.Add(inst.Request())
	ni.Workloads[inst.Workload]++
	z := c.ZoneCount[inst.Workload]
	if z == nil {
		z = map[string]int{}
		c.ZoneCount[inst.Workload] = z
	}
	z[ni.Node.Zone]++
}

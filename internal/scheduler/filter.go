package scheduler

import (
	"fmt"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// Filter is a hard constraint. It returns "" if the node is feasible, or a
// short reason it is not (aggregated into the instance's status message).
type Filter interface {
	Name() string
	Filter(inst *api.Instance, n *NodeInfo, c *Cluster) string
}

// DefaultFilters is the filter chain in evaluation order (cheapest first).
func DefaultFilters() []Filter {
	return []Filter{nodeReady{}, nodeSelector{}, volumeLocality{}, fitsResources{}, antiAffinity{}}
}

type nodeReady struct{}

func (nodeReady) Name() string { return "NodeReady" }
func (nodeReady) Filter(_ *api.Instance, n *NodeInfo, _ *Cluster) string {
	if n.Node.Status != api.NodeReady {
		return "node not ready"
	}
	return ""
}

type nodeSelector struct{}

func (nodeSelector) Name() string { return "NodeSelector" }
func (nodeSelector) Filter(inst *api.Instance, n *NodeInfo, _ *Cluster) string {
	for k, v := range inst.Spec.Placement.NodeSelector {
		if n.Node.Labels[k] != v {
			return "node selector mismatch"
		}
	}
	return ""
}

type fitsResources struct{}

func (fitsResources) Name() string { return "FitsResources" }
func (fitsResources) Filter(inst *api.Instance, n *NodeInfo, _ *Cluster) string {
	free, req := n.Free(), inst.Request()
	switch {
	case req.MilliCPU > free.MilliCPU && req.MemoryMB > free.MemoryMB:
		return "insufficient cpu and memory"
	case req.MilliCPU > free.MilliCPU:
		return "insufficient cpu"
	case req.MemoryMB > free.MemoryMB:
		return "insufficient memory"
	}
	return ""
}

type antiAffinity struct{}

func (antiAffinity) Name() string { return "AntiAffinity" }
func (antiAffinity) Filter(inst *api.Instance, n *NodeInfo, _ *Cluster) string {
	if inst.Spec.Placement.AntiAffinity && n.Workloads[inst.Workload] > 0 {
		return "anti-affinity: replica already on node"
	}
	return ""
}

// volumeLocality pins a stateful replica to the node its volume is bound to,
// as long as that node is alive. Volumes bound to a dead node are released by
// the reconciler (after fencing) before the instance is rescheduled, so this
// filter never strands a replica on a dead machine.
type volumeLocality struct{}

func (volumeLocality) Name() string { return "VolumeLocality" }
func (volumeLocality) Filter(inst *api.Instance, n *NodeInfo, c *Cluster) string {
	for _, vn := range inst.Volumes {
		v, ok := c.Volumes[vn]
		if !ok || v.BoundNode == "" || v.BoundNode == n.Node.Name {
			continue
		}
		if bn, ok := c.Nodes[v.BoundNode]; ok && bn.Node.Status == api.NodeReady {
			return fmt.Sprintf("volume %s bound to %s", vn, v.BoundNode)
		}
	}
	return ""
}

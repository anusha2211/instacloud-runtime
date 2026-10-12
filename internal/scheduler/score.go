package scheduler

import (
	"math"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// Scorer is a soft preference returning a score in [0, 100]; higher is better.
type Scorer interface {
	Name() string
	Score(inst *api.Instance, n *NodeInfo, c *Cluster) float64
}

// WeightedScorer pairs a scorer with its weight.
type WeightedScorer struct {
	Scorer
	Weight float64
}

// DefaultScorers encode "pack across workloads, spread within a workload".
func DefaultScorers() []WeightedScorer {
	return []WeightedScorer{
		{binPack{}, 1},
		{spread{}, 2},
		{balance{}, 0.5},
	}
}

func utilisation(used, total int64) float64 {
	if total <= 0 {
		return 1
	}
	return float64(used) / float64(total)
}

// binPack prefers nodes that are fuller after placement (MostAllocated), on
// the more constrained of the two dimensions.
type binPack struct{}

func (binPack) Name() string { return "BinPack" }
func (binPack) Score(inst *api.Instance, n *NodeInfo, _ *Cluster) float64 {
	after := n.Requested.Add(inst.Request())
	cpu := utilisation(after.MilliCPU, n.Node.Allocatable.MilliCPU)
	mem := utilisation(after.MemoryMB, n.Node.Allocatable.MemoryMB)
	return 100 * math.Max(cpu, mem)
}

// spread penalises nodes and zones that already host replicas of the same
// workload. Node co-location is penalised more than zone co-location.
type spread struct{}

func (spread) Name() string { return "Spread" }
func (spread) Score(inst *api.Instance, n *NodeInfo, c *Cluster) float64 {
	onNode := float64(n.Workloads[inst.Workload])
	inZone := float64(c.ZoneCount[inst.Workload][n.Node.Zone])
	return 100 / (1 + 2*onNode + inZone)
}

// balance penalises placements that leave CPU and memory very unevenly used,
// which strands capacity (e.g. a node full on memory with idle CPU).
type balance struct{}

func (balance) Name() string { return "Balance" }
func (balance) Score(inst *api.Instance, n *NodeInfo, _ *Cluster) float64 {
	after := n.Requested.Add(inst.Request())
	cpu := utilisation(after.MilliCPU, n.Node.Allocatable.MilliCPU)
	mem := utilisation(after.MemoryMB, n.Node.Allocatable.MemoryMB)
	return 100 * (1 - math.Abs(cpu-mem))
}

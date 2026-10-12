package scheduler

import (
	"fmt"
	"sort"
	"strings"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// Scheduler is a pure placement function over a Cluster view.
type Scheduler struct {
	Filters []Filter
	Scorers []WeightedScorer
}

// New returns a scheduler with the default plugin set.
func New() *Scheduler {
	return &Scheduler{Filters: DefaultFilters(), Scorers: DefaultScorers()}
}

// NodeScore is one candidate's total score (exposed for tests and debugging).
type NodeScore struct {
	Node  string
	Score float64
}

// UnschedulableError explains why no node fits.
type UnschedulableError struct {
	Reasons map[string]int // reason -> number of nodes
	Nodes   int
}

func (e *UnschedulableError) Error() string {
	if e.Nodes == 0 {
		return "no nodes registered"
	}
	parts := make([]string, 0, len(e.Reasons))
	for r, n := range e.Reasons {
		parts = append(parts, fmt.Sprintf("%d %s", n, r))
	}
	sort.Strings(parts)
	return fmt.Sprintf("0/%d nodes available: %s", e.Nodes, strings.Join(parts, ", "))
}

// Schedule picks the best node for inst, or returns *UnschedulableError.
func (s *Scheduler) Schedule(inst *api.Instance, c *Cluster) (string, []NodeScore, error) {
	reasons := map[string]int{}
	var feasible []*NodeInfo
	for _, name := range c.Order {
		n := c.Nodes[name]
		ok := true
		for _, f := range s.Filters {
			if r := f.Filter(inst, n, c); r != "" {
				reasons[r]++
				ok = false
				break
			}
		}
		if ok {
			feasible = append(feasible, n)
		}
	}
	if len(feasible) == 0 {
		return "", nil, &UnschedulableError{Reasons: reasons, Nodes: len(c.Order)}
	}
	scores := make([]NodeScore, 0, len(feasible))
	for _, n := range feasible {
		var total float64
		for _, ws := range s.Scorers {
			total += ws.Weight * ws.Score(inst, n, c)
		}
		scores = append(scores, NodeScore{Node: n.Node.Name, Score: total})
	}
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].Score != scores[j].Score {
			return scores[i].Score > scores[j].Score
		}
		return scores[i].Node < scores[j].Node
	})
	return scores[0].Node, scores, nil
}

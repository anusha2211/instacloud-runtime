// Package nodemon detects failed nodes. Each agent heartbeats every couple of
// seconds; a node whose last heartbeat is older than the grace period is
// marked NotReady and every instance on it is marked Lost, which hands them to
// the reconciler for replacement.
package nodemon

import (
	"context"
	"log/slog"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

var (
	nodeTransitions = metrics.Default.Counter("instacloud_node_status_transitions_total",
		"Node status changes made by the node monitor.", "to")
	instancesLost = metrics.Default.Counter("instacloud_instances_lost_total",
		"Instances marked Lost because their node stopped heartbeating.")
)

type Monitor struct {
	Store *store.Store
	Log   *slog.Logger
	// Grace is the heartbeat lease: no heartbeat for this long = NotReady.
	Grace    time.Duration
	Interval time.Duration
	Now      func() time.Time
}

func (m *Monitor) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Monitor) Run(ctx context.Context) {
	if m.Interval == 0 {
		m.Interval = time.Second
	}
	t := time.NewTicker(m.Interval)
	defer t.Stop()
	for {
		m.Check()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Check runs one detection pass.
func (m *Monitor) Check() {
	nodes, _ := store.List[api.Node](m.Store, api.KindNode)
	insts, _ := store.List[api.Instance](m.Store, api.KindInstance)
	now := m.now()
	for i := range nodes {
		n := &nodes[i]
		age := now.Sub(n.LastHeartbeat)
		if n.Status == api.NodeNotReady || age <= m.Grace {
			continue
		}
		n.Status = api.NodeNotReady
		n.Message = "heartbeat lease expired " + age.Round(time.Second).String() + " ago"
		if err := m.Store.Update(api.KindNode, n); err != nil {
			continue // heartbeat raced us: node is alive
		}
		nodeTransitions.Inc(string(api.NodeNotReady))
		m.Log.Warn("node not ready", "node", n.Name, "lastHeartbeat", n.LastHeartbeat, "age", age.Round(time.Millisecond))
	}

	// Mark instances on NotReady nodes Lost (also catches instances bound to
	// a node right before it died, on later passes).
	notReady := map[string]bool{}
	for _, n := range nodes {
		if n.Status == api.NodeNotReady {
			notReady[n.Name] = true
		}
	}
	for i := range insts {
		in := &insts[i]
		if !notReady[in.Node] || !in.Holds() {
			continue
		}
		if in.Status.Phase == api.PhaseTerminating {
			// Nobody can confirm the stop; treat as done.
			in.Status.Phase = api.PhaseTerminated
		} else {
			in.Status.Phase = api.PhaseLost
			instancesLost.Inc()
		}
		in.Status.Ready = false
		in.Status.Message = "node " + in.Node + " not ready"
		in.Status.UpdatedAt = now.UTC()
		if err := m.Store.Update(api.KindInstance, in); err == nil {
			m.Log.Warn("instance lost", "instance", in.Name, "workload", in.Workload, "node", in.Node)
		}
	}
}

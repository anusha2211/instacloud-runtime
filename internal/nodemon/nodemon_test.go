package nodemon

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

func TestNodeFailureMarksInstancesLost(t *testing.T) {
	s := store.New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := &Monitor{Store: s, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Grace: 10 * time.Second,
		Now: func() time.Time { return now }}

	_ = s.Create(api.KindNode, &api.Node{Meta: api.Meta{Name: "alive"}, Status: api.NodeReady, LastHeartbeat: now})
	_ = s.Create(api.KindNode, &api.Node{Meta: api.Meta{Name: "dead"}, Status: api.NodeReady, LastHeartbeat: now})
	mk := func(name, node string, p api.Phase) {
		_ = s.Create(api.KindInstance, &api.Instance{Meta: api.Meta{Name: name}, Node: node, Ordinal: -1,
			Status: api.InstanceStatus{Phase: p, Ready: true}})
	}
	mk("a", "alive", api.PhaseRunning)
	mk("b", "dead", api.PhaseRunning)
	mk("c", "dead", api.PhaseTerminating)

	now = now.Add(5 * time.Second)
	m.Check()
	var n api.Node
	_ = s.Get(api.KindNode, "dead", &n)
	if n.Status != api.NodeReady {
		t.Fatal("marked not ready within grace")
	}

	// "alive" heartbeats, "dead" does not.
	_ = store.Mutate(s, api.KindNode, "alive", func(n *api.Node) bool { n.LastHeartbeat = now; return true })
	now = now.Add(7 * time.Second)
	m.Check()

	_ = s.Get(api.KindNode, "dead", &n)
	if n.Status != api.NodeNotReady {
		t.Fatalf("dead node status %s", n.Status)
	}
	want := map[string]api.Phase{"a": api.PhaseRunning, "b": api.PhaseLost, "c": api.PhaseTerminated}
	for name, p := range want {
		var in api.Instance
		_ = s.Get(api.KindInstance, name, &in)
		if in.Status.Phase != p {
			t.Errorf("%s: phase %s want %s", name, in.Status.Phase, p)
		}
	}
}

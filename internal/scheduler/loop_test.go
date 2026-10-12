package scheduler

import (
	"io"
	"log/slog"
	"testing"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

func TestLoopBindsPendingAndVolumes(t *testing.T) {
	s := store.New()
	n := mkNode("a", "z1", 2000, 2048)
	_ = s.Create(api.KindNode, &n)
	_ = s.Create(api.KindVolume, &api.Volume{Meta: api.Meta{Name: "db-data-0"}, Phase: api.VolumeAvailable})
	p := pending("db", 500, 512)
	p.Name, p.Ordinal, p.Volumes = "db-0", 0, []string{"db-data-0"}
	_ = s.Create(api.KindInstance, p)
	big := pending("huge", 9000, 512)
	big.Name = "huge-x"
	_ = s.Create(api.KindInstance, big)

	l := &Loop{Store: s, Scheduler: New(), Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if got := l.Pass(); got != 1 {
		t.Fatalf("bound %d, want 1", got)
	}
	var inst api.Instance
	_ = s.Get(api.KindInstance, "db-0", &inst)
	if inst.Node != "a" || inst.Status.Phase != api.PhaseScheduled {
		t.Fatalf("not bound: %+v", inst.Status)
	}
	var v api.Volume
	_ = s.Get(api.KindVolume, "db-data-0", &v)
	if v.BoundNode != "a" || v.Instance != "db-0" {
		t.Fatalf("volume not bound: %+v", v)
	}
	var h api.Instance
	_ = s.Get(api.KindInstance, "huge-x", &h)
	if h.Status.Phase != api.PhasePending || h.Status.Message == "" {
		t.Fatalf("unschedulable instance should carry a reason: %+v", h.Status)
	}
}

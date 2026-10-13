package controller

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

type env struct {
	t     *testing.T
	s     *store.Store
	c     *Controller
	clock time.Time
}

func newEnv(t *testing.T) *env {
	s := store.New()
	e := &env{t: t, s: s, clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	e.c = &Controller{Store: s, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return e.clock }}
	e.c.StatefulFenceDelay = 5 * time.Second
	return e
}

func (e *env) apply(name string, kind api.WorkloadKind, replicas int, mutate ...func(*api.WorkloadSpec)) {
	spec := api.WorkloadSpec{Kind: kind, Replicas: replicas, Command: []string{"x"},
		Resources: api.Resources{MilliCPU: 100, MemoryMB: 64}}
	if kind == api.KindStateful {
		spec.Volumes = []api.VolumeSpec{{Name: "data", SizeMB: 10}}
	}
	for _, m := range mutate {
		m(&spec)
	}
	var cur api.Workload
	if err := e.s.Get(api.KindWorkload, name, &cur); err != nil {
		w := &api.Workload{Meta: api.Meta{Name: name}, Spec: spec}
		if err := e.s.Create(api.KindWorkload, w); err != nil {
			e.t.Fatal(err)
		}
		return
	}
	if api.SpecChanged(cur.Spec, spec) {
		cur.Generation++
	}
	cur.Spec = spec
	if err := e.s.Update(api.KindWorkload, &cur); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) instances(wl string) []api.Instance {
	all, _ := store.List[api.Instance](e.s, api.KindInstance)
	var out []api.Instance
	for _, in := range all {
		if in.Workload == wl {
			out = append(out, in)
		}
	}
	return out
}

func (e *env) phases(wl string) map[api.Phase]int {
	m := map[api.Phase]int{}
	for _, in := range e.instances(wl) {
		m[in.Status.Phase]++
	}
	return m
}

// agentTick plays the scheduler + agent: binds pending instances to node n1,
// marks them running/ready, and completes terminations.
func (e *env) agentTick() {
	all, _ := store.List[api.Instance](e.s, api.KindInstance)
	for i := range all {
		in := &all[i]
		switch in.Status.Phase {
		case api.PhasePending:
			in.Node = "n1"
			in.Status.Phase = api.PhaseScheduled
		case api.PhaseScheduled, api.PhaseStarting:
			in.Status.Phase, in.Status.Ready = api.PhaseRunning, true
		case api.PhaseTerminating:
			in.Status.Phase = api.PhaseTerminated
		default:
			continue
		}
		_ = e.s.Update(api.KindInstance, in)
	}
}

func (e *env) converge(rounds int) {
	for i := 0; i < rounds; i++ {
		e.c.Reconcile()
		e.agentTick()
	}
	e.c.Reconcile()
}

func TestServiceScaleUpAndDown(t *testing.T) {
	e := newEnv(t)
	e.apply("web", api.KindService, 3)
	e.c.Reconcile()
	if got := e.phases("web")[api.PhasePending]; got != 3 {
		t.Fatalf("want 3 pending, got %v", e.phases("web"))
	}
	e.converge(3)
	if got := e.phases("web")[api.PhaseRunning]; got != 3 {
		t.Fatalf("want 3 running, got %v", e.phases("web"))
	}
	var w api.Workload
	_ = e.s.Get(api.KindWorkload, "web", &w)
	if w.Status.ReadyReplicas != 3 {
		t.Fatalf("status %+v", w.Status)
	}

	e.apply("web", api.KindService, 1)
	e.converge(3)
	if got := len(e.instances("web")); got != 1 {
		t.Fatalf("want 1 instance after scale down, got %v", e.phases("web"))
	}
}

func TestStatefulOrderedCreateAndScaleDown(t *testing.T) {
	e := newEnv(t)
	e.apply("db", api.KindStateful, 3)
	e.c.Reconcile()
	insts := e.instances("db")
	if len(insts) != 1 || insts[0].Name != "db-0" {
		t.Fatalf("want only db-0 first, got %+v", insts)
	}
	vols, _ := store.List[api.Volume](e.s, api.KindVolume)
	if len(vols) != 3 {
		t.Fatalf("want 3 volumes pre-created, got %d", len(vols))
	}
	e.converge(10)
	names := map[string]bool{}
	for _, in := range e.instances("db") {
		names[in.Name] = true
		if len(in.Volumes) != 1 || in.Volumes[0] != api.VolumeName("db", "data", in.Ordinal) {
			t.Fatalf("%s has volumes %v", in.Name, in.Volumes)
		}
	}
	if !names["db-0"] || !names["db-1"] || !names["db-2"] {
		t.Fatalf("got %v", names)
	}

	e.apply("db", api.KindStateful, 1)
	e.c.Reconcile()
	for _, in := range e.instances("db") {
		if in.Status.Phase == api.PhaseTerminating && in.Name != "db-2" {
			t.Fatalf("scale down must start with highest ordinal, got %s", in.Name)
		}
	}
	e.converge(10)
	if got := e.instances("db"); len(got) != 1 || got[0].Name != "db-0" {
		t.Fatalf("got %+v", got)
	}
	vols, _ = store.List[api.Volume](e.s, api.KindVolume)
	if len(vols) != 3 {
		t.Fatalf("volumes must be retained on scale down, got %d", len(vols))
	}
}

func TestLostInstances(t *testing.T) {
	e := newEnv(t)
	e.apply("web", api.KindService, 2)
	e.apply("db", api.KindStateful, 1)
	e.converge(5)

	// Node monitor marks everything on n1 lost.
	all, _ := store.List[api.Instance](e.s, api.KindInstance)
	for i := range all {
		all[i].Status.Phase, all[i].Status.Ready = api.PhaseLost, false
		all[i].Status.UpdatedAt = e.clock
		_ = e.s.Update(api.KindInstance, &all[i])
	}
	_ = store.Mutate(e.s, api.KindVolume, "db-data-0", func(v *api.Volume) bool { v.BoundNode = "n1"; return true })

	e.c.Reconcile()
	e.c.Reconcile()
	// Stateless: replaced right away with new names.
	if p := e.phases("web"); p[api.PhasePending] != 2 {
		t.Fatalf("web should have 2 fresh pending replicas, got %v", p)
	}
	// Stateful: still Lost until the fence delay passes.
	var db api.Instance
	_ = e.s.Get(api.KindInstance, "db-0", &db)
	if db.Status.Phase != api.PhaseLost {
		t.Fatalf("db-0 moved before fence delay: %s", db.Status.Phase)
	}

	e.clock = e.clock.Add(6 * time.Second)
	e.c.Reconcile()
	db = api.Instance{}
	_ = e.s.Get(api.KindInstance, "db-0", &db)
	if db.Status.Phase != api.PhasePending || db.Node != "" || !strings.Contains(db.Status.Message, "n1") {
		t.Fatalf("db-0 not requeued: %+v node=%q", db.Status, db.Node)
	}
	var v api.Volume
	_ = e.s.Get(api.KindVolume, "db-data-0", &v)
	if v.BoundNode != "" {
		t.Fatalf("volume should be released, bound to %q", v.BoundNode)
	}
	if db.Name != "db-0" || db.Volumes[0] != "db-data-0" {
		t.Fatal("identity must be preserved")
	}
}

func TestWorkloadDeletionGC(t *testing.T) {
	e := newEnv(t)
	e.apply("db", api.KindStateful, 2)
	e.converge(8)
	_ = e.s.Delete(api.KindWorkload, "db", 0)
	e.converge(4)
	if n := len(e.instances("db")); n != 0 {
		t.Fatalf("instances left: %d", n)
	}
	vols, _ := store.List[api.Volume](e.s, api.KindVolume)
	if len(vols) != 0 {
		t.Fatalf("volumes left: %d", len(vols))
	}
}

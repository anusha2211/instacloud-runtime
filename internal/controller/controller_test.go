package controller

import (
	"io"
	"log/slog"
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

package agent

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/apiserver"
	"github.com/anusha2211/instacloud-runtime/internal/client"
	"github.com/anusha2211/instacloud-runtime/internal/runtime"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

type harness struct {
	t     *testing.T
	store *store.Store
	rt    *runtime.Fake
	agent *Agent
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := store.New()
	srv := httptest.NewServer(apiserver.New(s, log))
	t.Cleanup(srv.Close)
	rt := runtime.NewFake()
	a := New(Config{
		NodeName: "n1", Address: "127.0.0.1", Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192},
		PortBase: 20000, VolumeRoot: t.TempDir(),
	}, client.New(srv.URL), rt, log)
	if err := a.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, store: s, rt: rt, agent: a}
	return h
}

func (h *harness) addInstance(name string, hc *api.HealthCheck) {
	spec := api.WorkloadSpec{Kind: api.KindService, Command: []string{"x"}, Port: true, HealthCheck: hc,
		Resources: api.Resources{MilliCPU: 100, MemoryMB: 64}, TerminationGraceSeconds: 1}
	in := &api.Instance{Meta: api.Meta{Name: name}, Workload: "web", Ordinal: -1, Spec: spec, Node: "n1",
		Status: api.InstanceStatus{Phase: api.PhaseScheduled}}
	if err := h.store.Create(api.KindInstance, in); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) inst(name string) api.Instance {
	var in api.Instance
	_ = h.store.Get(api.KindInstance, name, &in)
	return in
}

// syncUntil runs agent syncs until cond holds.
func (h *harness) syncUntil(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		h.agent.Sync(context.Background())
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s", what)
}

func TestAgentStartsAndReportsRunning(t *testing.T) {
	h := newHarness(t)
	h.addInstance("web-a", nil)
	h.syncUntil("running", func() bool {
		in := h.inst("web-a")
		return in.Status.Phase == api.PhaseRunning && in.Status.Ready
	})
	if ep := h.inst("web-a").Status.Endpoint; ep != "127.0.0.1:20000" {
		t.Fatalf("endpoint %q", ep)
	}
	spec, _ := h.rt.Spec("web-a")
	if spec.Port != 20000 {
		t.Fatalf("port not passed to runtime: %+v", spec)
	}
}

func TestAgentRestartsWithBackoff(t *testing.T) {
	h := newHarness(t)
	h.addInstance("web-a", nil)
	h.syncUntil("running", func() bool { return h.inst("web-a").Status.Phase == api.PhaseRunning })

	h.rt.Crash("web-a", 1, "Exited")
	h.syncUntil("failed", func() bool {
		in := h.inst("web-a")
		return in.Status.Phase == api.PhaseFailed && in.Status.Restarts == 1
	})
	h.syncUntil("restarted", func() bool { return h.inst("web-a").Status.Phase == api.PhaseRunning })
	if h.rt.Starts["web-a"] != 2 {
		t.Fatalf("starts=%d", h.rt.Starts["web-a"])
	}
	if got := h.inst("web-a").Status.Restarts; got != 1 {
		t.Fatalf("restarts=%d", got)
	}
}

func TestAgentTerminatesAndStopsOrphans(t *testing.T) {
	h := newHarness(t)
	h.addInstance("web-a", nil)
	h.addInstance("web-b", nil)
	h.syncUntil("running", func() bool {
		return h.inst("web-a").Status.Phase == api.PhaseRunning && h.inst("web-b").Status.Phase == api.PhaseRunning
	})

	// Control plane asks web-a to terminate.
	_ = store.Mutate(h.store, api.KindInstance, "web-a", func(i *api.Instance) bool {
		i.Status.Phase = api.PhaseTerminating
		return true
	})
	// web-b is rescheduled to another node behind our back.
	_ = store.Mutate(h.store, api.KindInstance, "web-b", func(i *api.Instance) bool {
		i.Node = "n2"
		return true
	})
	h.syncUntil("terminated", func() bool { return h.inst("web-a").Status.Phase == api.PhaseTerminated })
	h.syncUntil("orphan stopped", func() bool {
		st, err := h.rt.Status("web-b")
		return err != nil || !st.Running
	})
}

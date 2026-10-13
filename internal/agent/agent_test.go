package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/apiserver"
	"github.com/anusha2211/instacloud-runtime/internal/client"
	"github.com/anusha2211/instacloud-runtime/internal/runtime"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

type fakeProber struct {
	mu   sync.Mutex
	fail bool
}

func (f *fakeProber) Probe(context.Context, string, *api.HealthCheck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("connection refused")
	}
	return nil
}

func (f *fakeProber) set(fail bool) { f.mu.Lock(); f.fail = fail; f.mu.Unlock() }

type harness struct {
	t     *testing.T
	store *store.Store
	rt    *runtime.Fake
	agent *Agent
	probe *fakeProber
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
		StartupGrace: time.Nanosecond,
		Backoff:      Backoff{Base: 20 * time.Millisecond, Max: 100 * time.Millisecond, ResetAfter: time.Hour},
	}, client.New(srv.URL), rt, log)
	if err := a.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, store: s, rt: rt, agent: a}
	h.probe = &fakeProber{}
	a.SetProber(h.probe)
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

func TestAgentHealthCheckFailureRestarts(t *testing.T) {
	h := newHarness(t)
	hc := &api.HealthCheck{Path: "/healthz", IntervalSeconds: 0, TimeoutSeconds: 1, FailureThreshold: 2}
	h.addInstance("web-a", hc)
	h.syncUntil("ready", func() bool { in := h.inst("web-a"); return in.Status.Ready })

	h.probe.set(true)
	h.syncUntil("unhealthy restart", func() bool { return h.inst("web-a").Status.Restarts >= 1 })
	h.probe.set(false)
	h.syncUntil("ready again", func() bool { in := h.inst("web-a"); return in.Status.Ready && in.Status.Phase == api.PhaseRunning })
	if h.rt.Starts["web-a"] < 2 {
		t.Fatalf("expected restart, starts=%d", h.rt.Starts["web-a"])
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

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 10 * time.Second}
	want := []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for n, w := range want {
		if got := b.Delay(n); got != w {
			t.Errorf("Delay(%d)=%v want %v", n, got, w)
		}
	}
}

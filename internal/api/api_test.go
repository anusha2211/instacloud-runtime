package api

import (
	"strings"
	"testing"
)

func validWorkload() *Workload {
	w := &Workload{Meta: Meta{Name: "web"}, Spec: WorkloadSpec{
		Replicas: 2, Command: []string{"./app"}, Port: true,
		Resources:   Resources{MilliCPU: 250, MemoryMB: 128},
		HealthCheck: &HealthCheck{Path: "/healthz"},
	}}
	w.Spec.Defaults()
	return w
}

func TestValidate(t *testing.T) {
	if err := validWorkload().Validate(); err != nil {
		t.Fatalf("valid workload rejected: %v", err)
	}
	cases := map[string]func(*Workload){
		"invalid name":                   func(w *Workload) { w.Name = "Web_1" },
		"spec.command is required":       func(w *Workload) { w.Spec.Command = nil },
		"must be > 0":                    func(w *Workload) { w.Spec.Resources.MemoryMB = 0 },
		"only allowed for kind=stateful": func(w *Workload) { w.Spec.Volumes = []VolumeSpec{{Name: "d", SizeMB: 1}} },
		"requires spec.port":             func(w *Workload) { w.Spec.Port = false },
		"minReplicas <= maxReplicas": func(w *Workload) {
			w.Spec.Autoscale = &AutoscaleSpec{MinReplicas: 3, MaxReplicas: 1, TargetRPSPerReplica: 1}
		},
	}
	for want, mutate := range cases {
		w := validWorkload()
		mutate(w)
		err := w.Validate()
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v", want, err)
		}
	}
}

func TestDefaults(t *testing.T) {
	w := validWorkload()
	if w.Spec.Kind != KindService || w.Spec.HealthCheck.FailureThreshold != 3 || w.Spec.TerminationGraceSeconds != 10 {
		t.Fatalf("defaults not applied: %+v", w.Spec)
	}
}

func TestSpecChangedIgnoresReplicas(t *testing.T) {
	a := validWorkload().Spec
	b := a
	b.Replicas = 10
	if SpecChanged(a, b) {
		t.Fatal("replica change must not trigger a rollout")
	}
	b.Env = map[string]string{"V": "2"}
	if !SpecChanged(a, b) {
		t.Fatal("env change must trigger a rollout")
	}
}

func TestLifecycleTransitions(t *testing.T) {
	allowed := [][2]Phase{
		{PhasePending, PhaseScheduled}, {PhaseScheduled, PhaseStarting}, {PhaseStarting, PhaseRunning},
		{PhaseRunning, PhaseUnhealthy}, {PhaseUnhealthy, PhaseFailed}, {PhaseFailed, PhaseStarting},
		{PhaseRunning, PhaseLost}, {PhaseLost, PhasePending}, {PhaseRunning, PhaseTerminating},
		{PhaseTerminating, PhaseTerminated}, {PhaseRunning, PhaseRunning},
	}
	for _, tr := range allowed {
		if !CanTransition(tr[0], tr[1]) {
			t.Errorf("%s -> %s should be allowed", tr[0], tr[1])
		}
	}
	denied := [][2]Phase{
		{PhaseTerminated, PhaseRunning}, {PhaseTerminating, PhaseRunning}, {PhasePending, PhaseRunning},
		{PhaseLost, PhaseRunning},
	}
	for _, tr := range denied {
		if CanTransition(tr[0], tr[1]) {
			t.Errorf("%s -> %s should be rejected", tr[0], tr[1])
		}
	}
}

func TestResources(t *testing.T) {
	a := Resources{MilliCPU: 500, MemoryMB: 256}
	if !a.Fits(Resources{MilliCPU: 500, MemoryMB: 256}) || a.Fits(Resources{MilliCPU: 499, MemoryMB: 1024}) {
		t.Fatal("Fits")
	}
	if got := a.Add(a).Sub(a); got != a {
		t.Fatalf("Add/Sub: %v", got)
	}
}

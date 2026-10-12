package apiserver

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, r))
	return rec
}

func newServer() (*Server, *store.Store) {
	s := store.New()
	return New(s, slog.New(slog.NewTextHandler(io.Discard, nil))), s
}

func spec() api.WorkloadSpec {
	return api.WorkloadSpec{Replicas: 2, Command: []string{"./app"}, Resources: api.Resources{MilliCPU: 100, MemoryMB: 64}}
}

func TestApplyBumpsGenerationOnlyOnSpecChange(t *testing.T) {
	srv, s := newServer()
	if rec := do(t, srv, "PUT", "/v1/workloads/web", api.Workload{Spec: spec()}); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	sp := spec()
	sp.Replicas = 5
	do(t, srv, "PUT", "/v1/workloads/web", api.Workload{Spec: sp})
	var w api.Workload
	_ = s.Get(api.KindWorkload, "web", &w)
	if w.Generation != 1 || w.Spec.Replicas != 5 {
		t.Fatalf("replica change bumped generation: gen=%d replicas=%d", w.Generation, w.Spec.Replicas)
	}
	sp.Env = map[string]string{"V": "2"}
	do(t, srv, "PUT", "/v1/workloads/web", api.Workload{Spec: sp})
	w = api.Workload{}
	_ = s.Get(api.KindWorkload, "web", &w)
	if w.Generation != 2 {
		t.Fatalf("spec change should bump generation, got %d", w.Generation)
	}
}

func TestApplyRejectsInvalid(t *testing.T) {
	srv, _ := newServer()
	bad := spec()
	bad.Command = nil
	if rec := do(t, srv, "PUT", "/v1/workloads/web", api.Workload{Spec: bad}); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", rec.Code)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("PUT", "/v1/workloads/web", bytes.NewReader([]byte(`{"spec":{"bogus":1}}`))))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown fields must be rejected, got %d", rec.Code)
	}
}

func TestStatusUpdateGuards(t *testing.T) {
	srv, s := newServer()
	_ = s.Create(api.KindInstance, &api.Instance{Meta: api.Meta{Name: "web-1"}, Node: "n1", Ordinal: -1,
		Status: api.InstanceStatus{Phase: api.PhaseScheduled}})

	ok := StatusUpdate{Node: "n1", Status: api.InstanceStatus{Phase: api.PhaseRunning, Ready: true}}
	if rec := do(t, srv, "PUT", "/v1/instances/web-1/status", ok); rec.Code != http.StatusNoContent {
		t.Fatalf("valid update: %d %s", rec.Code, rec.Body)
	}
	wrongNode := StatusUpdate{Node: "n2", Status: api.InstanceStatus{Phase: api.PhaseRunning}}
	if rec := do(t, srv, "PUT", "/v1/instances/web-1/status", wrongNode); rec.Code != http.StatusConflict {
		t.Fatalf("report from wrong node must be rejected, got %d", rec.Code)
	}
	_ = store.Mutate(s, api.KindInstance, "web-1", func(i *api.Instance) bool {
		i.Status.Phase = api.PhaseTerminating
		return true
	})
	if rec := do(t, srv, "PUT", "/v1/instances/web-1/status", ok); rec.Code != http.StatusConflict {
		t.Fatalf("agent must not resurrect a terminating instance, got %d", rec.Code)
	}
}

func TestRegisterHeartbeatAssignments(t *testing.T) {
	srv, s := newServer()
	reg := RegisterRequest{Address: "10.0.0.1", Zone: "a",
		Capacity: api.Resources{MilliCPU: 4000, MemoryMB: 8192}, Reserved: api.Resources{MilliCPU: 500, MemoryMB: 512}}
	if rec := do(t, srv, "POST", "/v1/nodes/n1/register", reg); rec.Code != http.StatusOK {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	var n api.Node
	_ = s.Get(api.KindNode, "n1", &n)
	if n.Allocatable != (api.Resources{MilliCPU: 3500, MemoryMB: 7680}) || n.Status != api.NodeReady {
		t.Fatalf("node %+v", n)
	}
	_ = store.Mutate(s, api.KindNode, "n1", func(n *api.Node) bool { n.Status = api.NodeNotReady; return true })
	if rec := do(t, srv, "POST", "/v1/nodes/n1/heartbeat", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("heartbeat: %d", rec.Code)
	}
	n = api.Node{}
	_ = s.Get(api.KindNode, "n1", &n)
	if n.Status != api.NodeReady {
		t.Fatal("heartbeat should mark node ready again")
	}

	_ = s.Create(api.KindVolume, &api.Volume{Meta: api.Meta{Name: "db-data-0"}})
	_ = s.Create(api.KindInstance, &api.Instance{Meta: api.Meta{Name: "db-0"}, Node: "n1", Volumes: []string{"db-data-0"},
		Status: api.InstanceStatus{Phase: api.PhaseScheduled}})
	_ = s.Create(api.KindInstance, &api.Instance{Meta: api.Meta{Name: "other"}, Node: "n2",
		Status: api.InstanceStatus{Phase: api.PhaseScheduled}})
	rec := do(t, srv, "GET", "/v1/nodes/n1/assignments", nil)
	var as []Assignment
	_ = json.Unmarshal(rec.Body.Bytes(), &as)
	if len(as) != 1 || as[0].Instance.Name != "db-0" || len(as[0].Volumes) != 1 {
		t.Fatalf("assignments %+v", as)
	}
}

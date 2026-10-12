// Package apiserver exposes the state store over HTTP/JSON. It is the only
// entry point for users (via icctl) and node agents, and the place where
// specs are validated and generations are assigned.
package apiserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

var (
	reqTotal = metrics.Default.Counter("instacloud_api_requests_total",
		"API requests by route and status code.", "route", "code")
	reqLatency = metrics.Default.Histogram("instacloud_api_request_duration_seconds",
		"API request latency.", nil, "route")
)

// Server is the control-plane HTTP API.
type Server struct {
	store *store.Store
	log   *slog.Logger
	mux   *http.ServeMux
	now   func() time.Time
}

func New(s *store.Store, log *slog.Logger) *Server {
	srv := &Server{store: s, log: log, mux: http.NewServeMux(), now: time.Now}
	srv.routes()
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) routes() {
	s.handle("GET /healthz", func(w http.ResponseWriter, r *http.Request) error {
		_, err := w.Write([]byte("ok\n"))
		return err
	})
	s.mux.Handle("GET /metrics", metrics.Default.Handler())

	// Workloads (desired state).
	s.handle("GET /v1/workloads", s.list(api.KindWorkload))
	s.handle("GET /v1/workloads/{name}", s.get(api.KindWorkload))
	s.handle("PUT /v1/workloads/{name}", s.applyWorkload)
	s.handle("POST /v1/workloads/{name}/scale", s.scaleWorkload)
	s.handle("DELETE /v1/workloads/{name}", s.deleteWorkload)

	// Read-only views of actual state.
	s.handle("GET /v1/instances", s.list(api.KindInstance))
	s.handle("GET /v1/instances/{name}", s.get(api.KindInstance))
	s.handle("GET /v1/nodes", s.list(api.KindNode))
	s.handle("GET /v1/nodes/{name}", s.get(api.KindNode))
	s.handle("GET /v1/volumes", s.list(api.KindVolume))

	// Node agent endpoints.
	s.handle("POST /v1/nodes/{name}/register", s.registerNode)
	s.handle("POST /v1/nodes/{name}/heartbeat", s.heartbeat)
	s.handle("GET /v1/nodes/{name}/assignments", s.assignments)
	s.handle("PUT /v1/instances/{name}/status", s.instanceStatus)

	// Change stream.
	s.handle("GET /v1/watch", s.watch)
}

// httpError carries a status code.
type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func errf(code int, f string, a ...any) error { return &httpError{code, fmt.Sprintf(f, a...)} }

func (s *Server) handle(pattern string, h func(http.ResponseWriter, *http.Request) error) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		code := http.StatusOK
		if err := h(w, r); err != nil {
			code = statusFor(err)
			if code >= 500 {
				s.log.Error("request failed", "route", pattern, "err", err)
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
		}
		reqTotal.Inc(pattern, strconv.Itoa(code))
		reqLatency.Since(start, pattern)
	})
}

func statusFor(err error) int {
	var he *httpError
	switch {
	case errors.As(err, &he):
		return he.code
	case store.IsNotFound(err):
		return http.StatusNotFound
	case store.IsConflict(err), store.IsExists(err):
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errf(http.StatusBadRequest, "invalid body: %v", err)
	}
	return nil
}

func (s *Server) list(kind string) func(http.ResponseWriter, *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		items, rev := s.store.ListRaw(kind)
		writeJSON(w, http.StatusOK, map[string]any{"items": items, "revision": rev})
		return nil
	}
}

func (s *Server) get(kind string) func(http.ResponseWriter, *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		raw, err := s.store.GetRaw(kind, r.PathValue("name"))
		if err != nil {
			return err
		}
		writeJSON(w, http.StatusOK, raw)
		return nil
	}
}

// applyWorkload creates or updates a workload (declarative apply). A change to
// anything other than replicas bumps the generation, which triggers a rollout.
func (s *Server) applyWorkload(w http.ResponseWriter, r *http.Request) error {
	var in api.Workload
	if err := decode(r, &in); err != nil {
		return err
	}
	in.Name = r.PathValue("name")
	in.Spec.Defaults()
	if err := in.Validate(); err != nil {
		return errf(http.StatusUnprocessableEntity, "%v", err)
	}
	var cur api.Workload
	err := s.store.Get(api.KindWorkload, in.Name, &cur)
	switch {
	case store.IsNotFound(err):
		wl := api.Workload{Meta: api.Meta{Name: in.Name, Labels: in.Labels}, Spec: in.Spec}
		if err := s.store.Create(api.KindWorkload, &wl); err != nil {
			return err
		}
		s.log.Info("workload created", "workload", wl.Name, "kind", wl.Spec.Kind, "replicas", wl.Spec.Replicas)
		writeJSON(w, http.StatusCreated, wl)
		return nil
	case err != nil:
		return err
	}
	if cur.Spec.Kind != in.Spec.Kind {
		return errf(http.StatusUnprocessableEntity, "spec.kind is immutable")
	}
	err = store.Mutate(s.store, api.KindWorkload, in.Name, func(wl *api.Workload) bool {
		if api.SpecChanged(wl.Spec, in.Spec) {
			wl.Generation++
		}
		wl.Spec = in.Spec
		wl.Labels = in.Labels
		cur = *wl
		return true
	})
	if err != nil {
		return err
	}
	s.log.Info("workload updated", "workload", cur.Name, "generation", cur.Generation, "replicas", cur.Spec.Replicas)
	writeJSON(w, http.StatusOK, cur)
	return nil
}

func (s *Server) scaleWorkload(w http.ResponseWriter, r *http.Request) error {
	var in struct {
		Replicas int `json:"replicas"`
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	if in.Replicas < 0 || in.Replicas > 1000 {
		return errf(http.StatusUnprocessableEntity, "replicas must be in [0,1000]")
	}
	var out api.Workload
	err := store.Mutate(s.store, api.KindWorkload, r.PathValue("name"), func(wl *api.Workload) bool {
		wl.Spec.Replicas = in.Replicas
		out = *wl
		return true
	})
	if err != nil {
		return err
	}
	s.log.Info("workload scaled", "workload", out.Name, "replicas", in.Replicas)
	writeJSON(w, http.StatusOK, out)
	return nil
}

// deleteWorkload removes the desired state; the reconciler then terminates
// the instances and garbage-collects volumes.
func (s *Server) deleteWorkload(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("name")
	if err := s.store.Delete(api.KindWorkload, name, 0); err != nil {
		return err
	}
	s.log.Info("workload deleted", "workload", name)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// RegisterRequest is sent by an agent on startup.
type RegisterRequest struct {
	Address  string            `json:"address"`
	Zone     string            `json:"zone"`
	Capacity api.Resources     `json:"capacity"`
	Reserved api.Resources     `json:"reserved"`
	Runtime  string            `json:"runtime"`
	Labels   map[string]string `json:"labels,omitempty"`
}

func (s *Server) registerNode(w http.ResponseWriter, r *http.Request) error {
	var in RegisterRequest
	if err := decode(r, &in); err != nil {
		return err
	}
	name := r.PathValue("name")
	if err := api.ValidName(name); err != nil {
		return errf(http.StatusUnprocessableEntity, "%v", err)
	}
	if !in.Reserved.Fits(in.Capacity) || in.Capacity.MilliCPU <= 0 || in.Capacity.MemoryMB <= 0 {
		return errf(http.StatusUnprocessableEntity, "invalid capacity/reserved")
	}
	n := api.Node{
		Meta:          api.Meta{Name: name, Labels: in.Labels},
		Address:       in.Address,
		Zone:          in.Zone,
		Capacity:      in.Capacity,
		Allocatable:   in.Capacity.Sub(in.Reserved),
		Runtime:       in.Runtime,
		Status:        api.NodeReady,
		LastHeartbeat: s.now().UTC(),
	}
	err := s.store.Create(api.KindNode, &n)
	if store.IsExists(err) {
		err = store.Mutate(s.store, api.KindNode, name, func(cur *api.Node) bool {
			rv, created := cur.ResourceVersion, cur.CreatedAt
			*cur = n
			cur.ResourceVersion, cur.CreatedAt = rv, created
			return true
		})
	}
	if err != nil {
		return err
	}
	s.log.Info("node registered", "node", name, "zone", in.Zone, "allocatable", n.Allocatable.String(), "runtime", in.Runtime)
	writeJSON(w, http.StatusOK, n)
	return nil
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("name")
	err := store.Mutate(s.store, api.KindNode, name, func(n *api.Node) bool {
		n.LastHeartbeat = s.now().UTC()
		if n.Status == api.NodeNotReady {
			s.log.Info("node recovered", "node", name)
			n.Status = api.NodeReady
			n.Message = ""
		}
		return true
	})
	if err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// Assignment is what an agent needs to run one instance.
type Assignment struct {
	Instance api.Instance `json:"instance"`
	Volumes  []api.Volume `json:"volumes,omitempty"`
}

func (s *Server) assignments(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("name")
	insts, err := store.List[api.Instance](s.store, api.KindInstance)
	if err != nil {
		return err
	}
	vols, err := store.List[api.Volume](s.store, api.KindVolume)
	if err != nil {
		return err
	}
	byName := map[string]api.Volume{}
	for _, v := range vols {
		byName[v.Name] = v
	}
	out := []Assignment{}
	for _, in := range insts {
		if in.Node != name || in.Status.Phase == api.PhaseTerminated || in.Status.Phase == api.PhaseLost {
			continue
		}
		a := Assignment{Instance: in}
		for _, vn := range in.Volumes {
			if v, ok := byName[vn]; ok {
				a.Volumes = append(a.Volumes, v)
			}
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, out)
	return nil
}

// StatusUpdate is reported by the agent that runs an instance.
type StatusUpdate struct {
	Node   string             `json:"node"`
	Status api.InstanceStatus `json:"status"`
}

// instanceStatus applies an agent's status report. Reports from a node the
// instance is no longer bound to (e.g. a partitioned agent coming back after
// the instance was rescheduled) and illegal phase transitions are rejected.
func (s *Server) instanceStatus(w http.ResponseWriter, r *http.Request) error {
	var in StatusUpdate
	if err := decode(r, &in); err != nil {
		return err
	}
	name := r.PathValue("name")
	var reject error
	err := store.Mutate(s.store, api.KindInstance, name, func(inst *api.Instance) bool {
		reject = nil
		if inst.Node != in.Node {
			reject = errf(http.StatusConflict, "instance %s is bound to %q, not %q", name, inst.Node, in.Node)
			return false
		}
		from, to := inst.Status.Phase, in.Status.Phase
		if !api.CanTransition(from, to) {
			reject = errf(http.StatusConflict, "illegal transition %s -> %s", from, to)
			return false
		}
		// The reconciler owns Terminating: an agent may not resurrect it.
		if from == api.PhaseTerminating && to != api.PhaseTerminated {
			reject = errf(http.StatusConflict, "instance is terminating")
			return false
		}
		if from != to {
			s.log.Info("instance phase", "instance", name, "workload", inst.Workload, "node", in.Node,
				"from", from, "to", to, "msg", in.Status.Message)
		}
		in.Status.UpdatedAt = s.now().UTC()
		inst.Status = in.Status
		return true
	})
	if err != nil {
		return err
	}
	if reject != nil {
		return reject
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// watch streams store events as newline-delimited JSON.
func (s *Server) watch(w http.ResponseWriter, r *http.Request) error {
	fl, ok := w.(http.Flusher)
	if !ok {
		return errf(http.StatusInternalServerError, "streaming unsupported")
	}
	ch, stop := s.store.Watch(r.URL.Query().Get("kind"))
	defer stop()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	fl.Flush()
	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return nil
		case e, ok := <-ch:
			if !ok {
				return nil // dropped as slow consumer; client re-lists
			}
			if err := enc.Encode(e); err != nil {
				return nil
			}
			fl.Flush()
		}
	}
}

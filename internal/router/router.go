// Package router is the L7 entry point for workloads. It maps
//
//	/w/<workload>/<path>          -> any ready replica of <workload>
//	/w/<workload>-<ordinal>/<path> -> that specific stateful replica
//
// Only Running && ready instances get traffic. The endpoint table is cached so
// we keep serving the last known set if the control plane goes away.
package router

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
)

var (
	routerRequests = metrics.Default.Counter("instacloud_router_requests_total",
		"Requests proxied by workload and status code.", "workload", "code")
	routerLatency = metrics.Default.Histogram("instacloud_router_request_duration_seconds",
		"End-to-end proxied request latency.", nil, "workload")
	routerUpstreamErrors = metrics.Default.Counter("instacloud_router_upstream_errors_total",
		"Failed upstream attempts (connection errors), by workload.", "workload")
	routerEndpoints = metrics.Default.Gauge("instacloud_router_endpoints",
		"Ready endpoints per workload.", "workload")
)

// Endpoint is one routable replica.
type Endpoint struct {
	Instance string
	Address  string // host:port
}

// Source provides the current instances (in-process store or API client).
type Source func(ctx context.Context) ([]api.Instance, error)

// Router is an http.Handler.
type Router struct {
	log       *slog.Logger
	transport http.RoundTripper

	mu      sync.RWMutex
	byWL    map[string][]Endpoint
	byName  map[string]Endpoint
	rr      map[string]*atomic.Uint64
	ejected map[string]time.Time // address -> until
	rates   *rateTracker
}

func New(log *slog.Logger) *Router {
	return &Router{
		log: log,
		transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     30 * time.Second,
		},
		byWL:    map[string][]Endpoint{},
		byName:  map[string]Endpoint{},
		rr:      map[string]*atomic.Uint64{},
		ejected: map[string]time.Time{},
		rates:   newRateTracker(10 * time.Second),
	}
}

// Update replaces the endpoint table from a list of instances.
func (r *Router) Update(insts []api.Instance) {
	byWL := map[string][]Endpoint{}
	byName := map[string]Endpoint{}
	for _, in := range insts {
		if in.Status.Phase != api.PhaseRunning || !in.Status.Ready || in.Status.Endpoint == "" {
			continue
		}
		ep := Endpoint{Instance: in.Name, Address: in.Status.Endpoint}
		byWL[in.Workload] = append(byWL[in.Workload], ep)
		if in.Stateful() {
			byName[in.Name] = ep
		}
	}
	for _, eps := range byWL {
		sort.Slice(eps, func(i, j int) bool { return eps[i].Instance < eps[j].Instance })
	}
	r.mu.Lock()
	for wl := range r.byWL {
		if _, ok := byWL[wl]; !ok {
			routerEndpoints.Set(0, wl)
		}
	}
	for wl, eps := range byWL {
		routerEndpoints.Set(float64(len(eps)), wl)
		if r.rr[wl] == nil {
			r.rr[wl] = &atomic.Uint64{}
		}
	}
	r.byWL, r.byName = byWL, byName
	r.mu.Unlock()
}

// Sync keeps the endpoint table fresh by polling src until ctx is done.
// (Polling keeps the router decoupled from the store; a watch stream is a
// drop-in replacement.)
func (r *Router) Sync(ctx context.Context, src Source, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if insts, err := src(ctx); err == nil {
			r.Update(insts)
		} else {
			r.log.Warn("endpoint refresh failed; serving last known endpoints", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Endpoints returns the ready endpoints of a workload (for tests / debug).
func (r *Router) Endpoints(workload string) []Endpoint {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Endpoint(nil), r.byWL[workload]...)
}

// RPS returns the recent request rate for a workload; used by the autoscaler.
func (r *Router) RPS(workload string) float64 { return r.rates.rate(workload) }

// pick returns endpoints to try, in order: round-robin start, ejected last.
func (r *Router) pick(target string) (string, []Endpoint) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ep, ok := r.byName[target]; ok {
		wl := target[:strings.LastIndex(target, "-")]
		return wl, []Endpoint{ep}
	}
	eps := r.byWL[target]
	if len(eps) == 0 {
		return target, nil
	}
	start := int(r.rr[target].Add(1)-1) % len(eps)
	now := time.Now()
	var healthy, ejected []Endpoint
	for i := range eps {
		ep := eps[(start+i)%len(eps)]
		if until, ok := r.ejected[ep.Address]; ok && now.Before(until) {
			ejected = append(ejected, ep)
		} else {
			healthy = append(healthy, ep)
		}
	}
	return target, append(healthy, ejected...)
}

func (r *Router) eject(addr string) {
	r.mu.Lock()
	r.ejected[addr] = time.Now().Add(5 * time.Second)
	r.mu.Unlock()
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }

var errUpstream = errors.New("upstream connection failed")

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/healthz" {
		_, _ = w.Write([]byte("ok\n"))
		return
	}
	rest, ok := strings.CutPrefix(req.URL.Path, "/w/")
	if !ok || rest == "" {
		http.Error(w, "route: expected /w/<workload>/...", http.StatusNotFound)
		return
	}
	target, path, _ := strings.Cut(rest, "/")
	path = "/" + path
	start := time.Now()
	workload, eps := r.pick(target)
	r.rates.add(workload)
	rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
	defer func() {
		routerRequests.Inc(workload, strconv.Itoa(rec.code))
		routerLatency.Since(start, workload)
	}()
	if len(eps) == 0 {
		rec.code = http.StatusServiceUnavailable
		http.Error(w, "no ready endpoints for "+target, http.StatusServiceUnavailable)
		return
	}

	// Retry once on connection failure for idempotent requests. The request
	// never reached the replica, so retrying is safe even for others, but we
	// keep to the conservative rule.
	attempts := 1
	if req.Method == http.MethodGet || req.Method == http.MethodHead {
		attempts = 2
	}
	for i := 0; i < attempts && i < len(eps); i++ {
		ep := eps[i]
		failed := false
		proxy := &httputil.ReverseProxy{
			Transport: r.transport,
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(&url.URL{Scheme: "http", Host: ep.Address})
				pr.Out.URL.Path = path
				pr.Out.URL.RawPath = ""
				pr.Out.Header.Set("X-Instacloud-Instance", ep.Instance)
				pr.SetXForwarded()
			},
			ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
				failed = true
				routerUpstreamErrors.Inc(workload)
				r.eject(ep.Address)
				r.log.Warn("upstream failed", "workload", workload, "instance", ep.Instance, "err", err)
			},
		}
		proxy.ServeHTTP(rec, req)
		if !failed {
			return
		}
	}
	rec.code = http.StatusBadGateway
	http.Error(w, errUpstream.Error(), http.StatusBadGateway)
}

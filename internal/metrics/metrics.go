// Package metrics is a tiny Prometheus text-format registry (counters, gauges,
// histograms). Enough for our needs without pulling in client_golang.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type kind string

const (
	counterKind   kind = "counter"
	gaugeKind     kind = "gauge"
	histogramKind kind = "histogram"
)

// Registry holds metric families.
type Registry struct {
	mu         sync.Mutex
	families   map[string]*family
	collectors []func()
}

func NewRegistry() *Registry { return &Registry{families: map[string]*family{}} }

// Default is the process-wide registry.
var Default = NewRegistry()

type family struct {
	name, help string
	kind       kind
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*series
}

type series struct {
	labelValues []string
	value       float64
	counts      []uint64 // histogram buckets (cumulative at render time)
	sum         float64
	count       uint64
}

func (r *Registry) family(name, help string, k kind, labels []string, buckets []float64) *family {
	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.families[name]; ok {
		return f
	}
	f := &family{name: name, help: help, kind: k, labels: labels, buckets: buckets, series: map[string]*series{}}
	r.families[name] = f
	return f
}

func (f *family) get(lv []string) *series {
	if len(lv) != len(f.labels) {
		panic(fmt.Sprintf("metric %s: want %d label values, got %d", f.name, len(f.labels), len(lv)))
	}
	key := strings.Join(lv, "\xff")
	s, ok := f.series[key]
	if !ok {
		s = &series{labelValues: append([]string(nil), lv...)}
		if f.kind == histogramKind {
			s.counts = make([]uint64, len(f.buckets))
		}
		f.series[key] = s
	}
	return s
}

// CounterVec is a monotonically increasing labelled counter.
type CounterVec struct{ f *family }

func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	return &CounterVec{r.family(name, help, counterKind, labels, nil)}
}

func (c *CounterVec) Add(v float64, lv ...string) {
	c.f.mu.Lock()
	c.f.get(lv).value += v
	c.f.mu.Unlock()
}

func (c *CounterVec) Inc(lv ...string) { c.Add(1, lv...) }

// Value returns the current value (for tests and in-process consumers).
func (c *CounterVec) Value(lv ...string) float64 {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	return c.f.get(lv).value
}

// GaugeVec is a labelled value that can go up and down.
type GaugeVec struct{ f *family }

func (r *Registry) Gauge(name, help string, labels ...string) *GaugeVec {
	return &GaugeVec{r.family(name, help, gaugeKind, labels, nil)}
}

func (g *GaugeVec) Set(v float64, lv ...string) {
	g.f.mu.Lock()
	g.f.get(lv).value = v
	g.f.mu.Unlock()
}

// Reset drops all series (used by collectors that recompute a gauge).
func (g *GaugeVec) Reset() {
	g.f.mu.Lock()
	g.f.series = map[string]*series{}
	g.f.mu.Unlock()
}

// HistogramVec observes distributions.
type HistogramVec struct{ f *family }

// DefBuckets suit latencies in seconds.
var DefBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if buckets == nil {
		buckets = DefBuckets
	}
	return &HistogramVec{r.family(name, help, histogramKind, labels, buckets)}
}

func (h *HistogramVec) Observe(v float64, lv ...string) {
	h.f.mu.Lock()
	defer h.f.mu.Unlock()
	s := h.f.get(lv)
	for i, b := range h.f.buckets {
		if v <= b {
			s.counts[i]++
			break
		}
	}
	s.sum += v
	s.count++
}

// Since observes the seconds elapsed since t.
func (h *HistogramVec) Since(t time.Time, lv ...string) {
	h.Observe(time.Since(t).Seconds(), lv...)
}

// OnCollect registers a function run before every scrape, used to refresh
// gauges derived from state (e.g. instances by phase).
func (r *Registry) OnCollect(fn func()) {
	r.mu.Lock()
	r.collectors = append(r.collectors, fn)
	r.mu.Unlock()
}

// Write renders all families in Prometheus text exposition format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	cols := append([]func(){}, r.collectors...)
	r.mu.Unlock()
	for _, c := range cols {
		c()
	}
	r.mu.Lock()
	names := make([]string, 0, len(r.families))
	for n := range r.families {
		names = append(names, n)
	}
	fams := r.families
	r.mu.Unlock()
	sort.Strings(names)
	for _, n := range names {
		fams[n].write(w)
	}
}

func (f *family) write(w io.Writer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
	keys := make([]string, 0, len(f.series))
	for k := range f.series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := f.series[k]
		switch f.kind {
		case histogramKind:
			var cum uint64
			for i, b := range f.buckets {
				cum += s.counts[i]
				fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, f.labelStr(s.labelValues, "le", formatFloat(b)), cum)
			}
			fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, f.labelStr(s.labelValues, "le", "+Inf"), s.count)
			fmt.Fprintf(w, "%s_sum%s %s\n", f.name, f.labelStr(s.labelValues, "", ""), formatFloat(s.sum))
			fmt.Fprintf(w, "%s_count%s %d\n", f.name, f.labelStr(s.labelValues, "", ""), s.count)
		default:
			fmt.Fprintf(w, "%s%s %s\n", f.name, f.labelStr(s.labelValues, "", ""), formatFloat(s.value))
		}
	}
}

func (f *family) labelStr(lv []string, extraK, extraV string) string {
	var parts []string
	for i, l := range f.labels {
		parts = append(parts, fmt.Sprintf("%s=%q", l, lv[i]))
	}
	if extraK != "" {
		parts = append(parts, fmt.Sprintf("%s=%q", extraK, extraV))
	}
	if len(parts) == 0 {
		return ""
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e15 {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}

// Handler serves the registry at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}

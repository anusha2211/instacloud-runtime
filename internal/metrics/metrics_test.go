package metrics

import (
	"strings"
	"testing"
)

func TestTextFormat(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("reqs_total", "Requests.", "code")
	c.Inc("200")
	c.Add(2, "200")
	c.Inc("500")
	g := r.Gauge("up", "Up.")
	g.Set(1)
	h := r.Histogram("lat_seconds", "Latency.", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(3)

	var sb strings.Builder
	r.Write(&sb)
	out := sb.String()
	for _, want := range []string{
		"# TYPE reqs_total counter",
		`reqs_total{code="200"} 3`,
		`reqs_total{code="500"} 1`,
		"up 1",
		`lat_seconds_bucket{le="0.1"} 1`,
		`lat_seconds_bucket{le="1"} 2`,
		`lat_seconds_bucket{le="+Inf"} 3`,
		"lat_seconds_count 3",
		"lat_seconds_sum 3.55",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestCollectorRunsBeforeScrape(t *testing.T) {
	r := NewRegistry()
	g := r.Gauge("phase", "x", "p")
	n := 0
	r.OnCollect(func() { n++; g.Reset(); g.Set(float64(n), "Running") })
	var sb strings.Builder
	r.Write(&sb)
	r.Write(&sb)
	if !strings.Contains(sb.String(), `phase{p="Running"} 2`) {
		t.Fatal(sb.String())
	}
}

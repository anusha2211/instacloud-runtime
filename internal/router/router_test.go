package router

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

func backend(t *testing.T, name string) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, name+" "+r.URL.Path)
	}))
	t.Cleanup(s.Close)
	return s
}

func inst(name, wl string, ord int, addr string, ready bool) api.Instance {
	return api.Instance{Meta: api.Meta{Name: name}, Workload: wl, Ordinal: ord,
		Status: api.InstanceStatus{Phase: api.PhaseRunning, Ready: ready, Endpoint: addr}}
}

func get(t *testing.T, h http.Handler, path string) (int, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func addr(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

func TestRoundRobinOnlyReady(t *testing.T) {
	a, b, c := backend(t, "a"), backend(t, "b"), backend(t, "c")
	r := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.Update([]api.Instance{
		inst("web-a", "web", -1, addr(a), true),
		inst("web-b", "web", -1, addr(b), true),
		inst("web-c", "web", -1, addr(c), false), // not ready: never routed
	})
	seen := map[string]int{}
	for i := 0; i < 10; i++ {
		code, body := get(t, r, "/w/web/hello")
		if code != 200 || !strings.HasSuffix(body, " /hello") {
			t.Fatalf("code=%d body=%q", code, body)
		}
		seen[strings.Fields(body)[0]]++
	}
	if seen["a"] != 5 || seen["b"] != 5 || seen["c"] != 0 {
		t.Fatalf("distribution %v", seen)
	}
}

func TestRetriesOnDeadEndpoint(t *testing.T) {
	a := backend(t, "a")
	dead := httptest.NewServer(http.NotFoundHandler())
	deadAddr := addr(dead)
	dead.Close()
	r := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.Update([]api.Instance{inst("web-a", "web", -1, addr(a), true), inst("web-b", "web", -1, deadAddr, true)})
	for i := 0; i < 6; i++ {
		if code, body := get(t, r, "/w/web/"); code != 200 || !strings.HasPrefix(body, "a") {
			t.Fatalf("request %d: code=%d body=%q", i, code, body)
		}
	}
}

func TestStatefulOrdinalAddressAndNoEndpoints(t *testing.T) {
	a, b := backend(t, "kv0"), backend(t, "kv1")
	r := New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.Update([]api.Instance{inst("kv-0", "kv", 0, addr(a), true), inst("kv-1", "kv", 1, addr(b), true)})
	for i := 0; i < 3; i++ {
		if _, body := get(t, r, "/w/kv-1/get"); !strings.HasPrefix(body, "kv1 /get") {
			t.Fatalf("ordinal routing broken: %q", body)
		}
	}
	if code, _ := get(t, r, "/w/nope/"); code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", code)
	}
}

func TestRateTracker(t *testing.T) {
	now := time.Unix(1000, 0)
	rt := newRateTracker(10 * time.Second)
	rt.now = func() time.Time { return now }
	for s := 0; s < 10; s++ {
		for i := 0; i < 5; i++ {
			rt.add("web")
		}
		now = now.Add(time.Second)
	}
	if got := rt.rate("web"); got != 5 {
		t.Fatalf("rate=%v want 5", got)
	}
}

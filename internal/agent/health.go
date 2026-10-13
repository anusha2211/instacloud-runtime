package agent

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// Prober runs one health check. It is an interface so tests can fake it.
type Prober interface {
	Probe(ctx context.Context, endpoint string, hc *api.HealthCheck) error
}

// HTTPProber performs HTTP GET probes; any 2xx/3xx is healthy.
type HTTPProber struct{ Client *http.Client }

func (p HTTPProber) Probe(ctx context.Context, endpoint string, hc *api.HealthCheck) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(hc.TimeoutSeconds)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+hc.Path, nil)
	if err != nil {
		return err
	}
	c := p.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// healthState tracks probe results for one instance.
type healthState struct {
	lastProbe   time.Time
	successes   int // since start
	consecFails int
	lastErr     error
}

// due reports whether a probe should run now.
func (h *healthState) due(now time.Time, hc *api.HealthCheck) bool {
	return now.Sub(h.lastProbe) >= time.Duration(hc.IntervalSeconds)*time.Second
}

// record updates counters and returns (ready, failed) where failed means the
// failure threshold has been crossed.
func (h *healthState) record(now time.Time, err error, hc *api.HealthCheck, startedAt time.Time, startupGrace time.Duration) (ready, failed bool) {
	h.lastProbe = now
	h.lastErr = err
	if err == nil {
		h.successes++
		h.consecFails = 0
		return true, false
	}
	h.consecFails++
	// Before the first success, give the process a startup grace period.
	if h.successes == 0 && now.Sub(startedAt) < startupGrace {
		return false, false
	}
	return false, h.consecFails >= hc.FailureThreshold
}

// Package autoscaler adjusts the replica count of workloads that declare an
// autoscale block, based on request rate observed at the router.
//
//	desired = ceil(observedRPS / targetRPSPerReplica), clamped to [min, max]
//
// Two guards prevent flapping: changes inside a tolerance band are ignored,
// and scale-down uses the highest recommendation seen during a stabilisation
// window (scale-up is immediate). The autoscaler only writes spec.replicas,
// so it never triggers a rollout.
package autoscaler

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

var (
	decisions = metrics.Default.Counter("instacloud_autoscaler_decisions_total",
		"Scaling decisions by workload and direction.", "workload", "direction")
	desiredGauge = metrics.Default.Gauge("instacloud_autoscaler_desired_replicas",
		"Latest autoscaler recommendation.", "workload")
	observedGauge = metrics.Default.Gauge("instacloud_autoscaler_observed_rps",
		"Request rate used for the latest decision.", "workload")
)

// Signal returns the current load (requests/second) for a workload.
type Signal func(workload string) float64

type sample struct {
	at       time.Time
	replicas int
}

type Autoscaler struct {
	Store  *store.Store
	Log    *slog.Logger
	Signal Signal
	// Interval between evaluations.
	Interval time.Duration
	// Tolerance: ignore a change if |observed/target - current| / current <= Tolerance.
	Tolerance float64
	// ScaleDownWindow: recommendation history used before scaling down.
	ScaleDownWindow time.Duration
	Now             func() time.Time

	history map[string][]sample
}

func (a *Autoscaler) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *Autoscaler) Run(ctx context.Context) {
	if a.Interval == 0 {
		a.Interval = 5 * time.Second
	}
	t := time.NewTicker(a.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.Evaluate()
		}
	}
}

// Recommend is the pure scaling rule.
func Recommend(current int, rps float64, spec api.AutoscaleSpec, tolerance float64) int {
	raw := rps / spec.TargetRPSPerReplica
	if current > 0 && math.Abs(raw-float64(current))/float64(current) <= tolerance {
		return clamp(current, spec)
	}
	return clamp(int(math.Ceil(raw)), spec)
}

func clamp(n int, s api.AutoscaleSpec) int {
	if n < s.MinReplicas {
		return s.MinReplicas
	}
	if n > s.MaxReplicas {
		return s.MaxReplicas
	}
	return n
}

// Evaluate runs one pass over all autoscaled workloads.
func (a *Autoscaler) Evaluate() {
	if a.history == nil {
		a.history = map[string][]sample{}
	}
	wls, _ := store.List[api.Workload](a.Store, api.KindWorkload)
	now := a.now()
	for _, wl := range wls {
		as := wl.Spec.Autoscale
		if as == nil {
			continue
		}
		cur := wl.Spec.Replicas
		rps := a.Signal(wl.Name)
		rec := Recommend(cur, rps, *as, a.Tolerance)
		observedGauge.Set(rps, wl.Name)

		// Stabilise scale-down: take the max recommendation in the window.
		h := append(a.history[wl.Name], sample{at: now, replicas: rec})
		cut := 0
		for cut < len(h) && now.Sub(h[cut].at) > a.ScaleDownWindow {
			cut++
		}
		h = h[cut:]
		a.history[wl.Name] = h
		desired := rec
		if rec < cur {
			for _, s := range h {
				if s.replicas > desired {
					desired = s.replicas
				}
			}
		}
		desiredGauge.Set(float64(desired), wl.Name)
		if desired == cur {
			continue
		}
		dir := "up"
		if desired < cur {
			dir = "down"
		}
		err := store.Mutate(a.Store, api.KindWorkload, wl.Name, func(w *api.Workload) bool {
			if w.Spec.Replicas != cur {
				return false // someone else changed it; re-evaluate next pass
			}
			w.Spec.Replicas = desired
			return true
		})
		if err != nil {
			continue
		}
		decisions.Inc(wl.Name, dir)
		a.Log.Info("autoscaled", "workload", wl.Name, "from", cur, "to", desired, "rps", math.Round(rps*10)/10,
			"targetPerReplica", as.TargetRPSPerReplica)
	}
}

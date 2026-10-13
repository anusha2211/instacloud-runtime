package autoscaler

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

func TestRecommend(t *testing.T) {
	spec := api.AutoscaleSpec{MinReplicas: 1, MaxReplicas: 10, TargetRPSPerReplica: 10}
	cases := []struct {
		cur  int
		rps  float64
		want int
	}{
		{2, 0, 1},     // idle -> min
		{2, 21, 2},    // within 10% tolerance of 2.1 -> stay
		{2, 45, 5},    // ceil(4.5)
		{2, 1000, 10}, // capped at max
		{5, 30, 3},
	}
	for _, c := range cases {
		if got := Recommend(c.cur, c.rps, spec, 0.1); got != c.want {
			t.Errorf("Recommend(%d, %v)=%d want %d", c.cur, c.rps, got, c.want)
		}
	}
}

func TestScaleUpImmediatelyScaleDownStabilised(t *testing.T) {
	s := store.New()
	_ = s.Create(api.KindWorkload, &api.Workload{Meta: api.Meta{Name: "web"}, Spec: api.WorkloadSpec{
		Replicas: 1, Autoscale: &api.AutoscaleSpec{MinReplicas: 1, MaxReplicas: 8, TargetRPSPerReplica: 10}}})
	rps := 50.0
	now := time.Unix(0, 0)
	a := &Autoscaler{Store: s, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Tolerance: 0.1,
		ScaleDownWindow: 30 * time.Second, Signal: func(string) float64 { return rps }, Now: func() time.Time { return now }}
	replicas := func() int {
		var w api.Workload
		_ = s.Get(api.KindWorkload, "web", &w)
		return w.Spec.Replicas
	}

	a.Evaluate()
	if replicas() != 5 {
		t.Fatalf("want 5 after burst, got %d", replicas())
	}
	rps = 10
	for i := 0; i < 5; i++ { // 25s of low traffic: still inside the window
		now = now.Add(5 * time.Second)
		a.Evaluate()
	}
	if replicas() != 5 {
		t.Fatalf("scaled down inside stabilisation window: %d", replicas())
	}
	now = now.Add(10 * time.Second)
	a.Evaluate()
	if replicas() != 1 {
		t.Fatalf("want 1 after window, got %d", replicas())
	}
}

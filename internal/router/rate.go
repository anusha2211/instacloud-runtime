package router

import (
	"sync"
	"time"
)

// rateTracker counts requests per workload in 1s buckets over a sliding
// window and reports the average rate.
type rateTracker struct {
	mu      sync.Mutex
	window  time.Duration
	buckets map[string]map[int64]int
	now     func() time.Time
}

func newRateTracker(window time.Duration) *rateTracker {
	return &rateTracker{window: window, buckets: map[string]map[int64]int{}, now: time.Now}
}

func (t *rateTracker) add(workload string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sec := t.now().Unix()
	b := t.buckets[workload]
	if b == nil {
		b = map[int64]int{}
		t.buckets[workload] = b
	}
	b[sec]++
	if len(b) > int(t.window.Seconds())*2 {
		t.gc(b, sec)
	}
}

func (t *rateTracker) gc(b map[int64]int, now int64) {
	for s := range b {
		if s <= now-int64(t.window.Seconds()) {
			delete(b, s)
		}
	}
}

// rate is requests/second over the last full window (excluding the current,
// partial second).
func (t *rateTracker) rate(workload string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now().Unix()
	n := 0
	w := int64(t.window.Seconds())
	for s, c := range t.buckets[workload] {
		if s < now && s >= now-w {
			n += c
		}
	}
	return float64(n) / float64(w)
}

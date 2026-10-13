package agent

import "time"

// Backoff computes restart delays: Base * 2^(n-1), capped at Max. The counter
// resets once an instance has stayed healthy for ResetAfter, so a replica that
// crashes once a day is not penalised for yesterday's crash.
type Backoff struct {
	Base       time.Duration
	Max        time.Duration
	ResetAfter time.Duration
}

func DefaultBackoff() Backoff {
	return Backoff{Base: time.Second, Max: time.Minute, ResetAfter: time.Minute}
}

// Delay returns the wait before restart number n (n >= 1).
func (b Backoff) Delay(n int) time.Duration {
	if n <= 0 {
		return 0
	}
	d := b.Base
	for i := 1; i < n; i++ {
		d *= 2
		if d >= b.Max {
			return b.Max
		}
	}
	if d > b.Max {
		return b.Max
	}
	return d
}

package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

var (
	schedAttempts = metrics.Default.Counter("instacloud_scheduler_attempts_total",
		"Scheduling attempts by result (scheduled, unschedulable, conflict).", "result")
	schedLatency = metrics.Default.Histogram("instacloud_scheduler_pass_duration_seconds",
		"Duration of a full scheduling pass.", nil)
	pendingGauge = metrics.Default.Gauge("instacloud_scheduler_pending_instances",
		"Instances waiting for a node at the end of the last pass.")
)

// Loop binds Pending instances to nodes. It runs a pass whenever instances or
// nodes change, and on a timer as a safety net (level-triggered).
type Loop struct {
	Store     *store.Store
	Scheduler *Scheduler
	Log       *slog.Logger
	Interval  time.Duration
}

func (l *Loop) Run(ctx context.Context) {
	if l.Interval == 0 {
		l.Interval = 2 * time.Second
	}
	events, stop := l.Store.Watch("")
	defer stop()
	tick := time.NewTicker(l.Interval)
	defer tick.Stop()
	for {
		l.Pass()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case _, ok := <-events:
			if !ok { // dropped as slow consumer: re-subscribe
				events, stop = l.Store.Watch("")
			}
			drain(events)
		}
	}
}

// drain coalesces bursts of events into one pass.
func drain(ch <-chan store.Event) {
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			return
		}
	}
}

// Pass runs one scheduling pass and returns the number of instances bound.
func (l *Loop) Pass() int {
	start := time.Now()
	defer schedLatency.Since(start)
	nodes, _ := store.List[api.Node](l.Store, api.KindNode)
	insts, _ := store.List[api.Instance](l.Store, api.KindInstance)
	vols, _ := store.List[api.Volume](l.Store, api.KindVolume)
	c := NewCluster(nodes, insts, vols)

	var pending []*api.Instance
	for i := range insts {
		if insts[i].Status.Phase == api.PhasePending && insts[i].Node == "" {
			pending = append(pending, &insts[i])
		}
	}
	// Larger requests first (first-fit decreasing packs better), then by age.
	sort.SliceStable(pending, func(i, j int) bool {
		a, b := pending[i].Request(), pending[j].Request()
		if a.MemoryMB != b.MemoryMB {
			return a.MemoryMB > b.MemoryMB
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})

	bound := 0
	for _, inst := range pending {
		node, _, err := l.Scheduler.Schedule(inst, c)
		if err != nil {
			schedAttempts.Inc("unschedulable")
			l.markUnschedulable(inst, err)
			continue
		}
		if err := l.bind(inst, node); err != nil {
			if store.IsConflict(err) || errors.Is(err, errSkip) {
				schedAttempts.Inc("conflict")
				continue
			}
			l.Log.Error("bind failed", "instance", inst.Name, "node", node, "err", err)
			continue
		}
		c.Assume(inst, node)
		schedAttempts.Inc("scheduled")
		bound++
		l.Log.Info("instance scheduled", "instance", inst.Name, "workload", inst.Workload, "node", node)
	}
	pendingGauge.Set(float64(len(pending) - bound))
	return bound
}

var errSkip = errors.New("instance changed")

// bind is a CAS on the instance read in this pass: if anything changed it
// (deleted, already bound by another scheduler) the write fails and the
// instance is reconsidered in the next pass.
func (l *Loop) bind(inst *api.Instance, node string) error {
	inst.Node = node
	inst.Status.Phase = api.PhaseScheduled
	inst.Status.Message = ""
	inst.Status.UpdatedAt = time.Now().UTC()
	if err := l.Store.Update(api.KindInstance, inst); err != nil {
		return err
	}
	for _, vn := range inst.Volumes {
		err := store.Mutate(l.Store, api.KindVolume, vn, func(v *api.Volume) bool {
			v.BoundNode = node
			v.Instance = inst.Name
			v.Phase = api.VolumeBound
			return true
		})
		if err != nil {
			l.Log.Error("volume bind failed", "volume", vn, "err", err)
		}
	}
	return nil
}

func (l *Loop) markUnschedulable(inst *api.Instance, err error) {
	msg := err.Error()
	if inst.Status.Message == msg {
		return
	}
	inst.Status.Message = msg
	if uerr := l.Store.Update(api.KindInstance, inst); uerr == nil {
		l.Log.Warn("instance unschedulable", "instance", inst.Name, "reason", msg)
	}
}

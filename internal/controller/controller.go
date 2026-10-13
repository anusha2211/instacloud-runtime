// Package controller contains the workload reconciler: the control loop that
// turns Workload specs (desired state) into Instance and Volume objects and
// keeps them converged through scaling, rollouts, crashes and node loss.
//
// The reconciler is level-triggered: every pass recomputes what should exist
// from a fresh read of the store, so it can crash or miss events at any point
// and the next pass repairs the state.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/store"
)

var (
	reconcileDuration = metrics.Default.Histogram("instacloud_reconcile_duration_seconds",
		"Duration of a full reconcile pass.", nil)
	instanceActions = metrics.Default.Counter("instacloud_reconcile_actions_total",
		"Actions taken by the reconciler.", "action")
)

// Controller reconciles all workloads.
type Controller struct {
	Store    *store.Store
	Log      *slog.Logger
	Interval time.Duration
	// StatefulFenceDelay is how long a stateful instance on a lost node waits
	// before its identity and volume are moved to another node. It must exceed
	// the agent's self-fencing lease so the old copy has stopped writing.
	StatefulFenceDelay time.Duration
	Now                func() time.Time
}

func (c *Controller) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Run reconciles on every store change (coalesced) and on a timer.
func (c *Controller) Run(ctx context.Context) {
	if c.Interval == 0 {
		c.Interval = time.Second
	}
	events, stop := c.Store.Watch("")
	defer func() { stop() }()
	tick := time.NewTicker(c.Interval)
	defer tick.Stop()
	for {
		c.Reconcile()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case _, ok := <-events:
			if !ok {
				events, stop = c.Store.Watch("")
			}
			coalesce(events)
		}
	}
}

func coalesce(ch <-chan store.Event) {
	t := time.NewTimer(25 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-t.C:
			return
		}
	}
}

// state is one consistent-enough read of the store for a pass.
type state struct {
	workloads map[string]*api.Workload
	instances []api.Instance
	volumes   map[string]api.Volume
	byWL      map[string][]*api.Instance
}

func (c *Controller) read() *state {
	wls, _ := store.List[api.Workload](c.Store, api.KindWorkload)
	insts, _ := store.List[api.Instance](c.Store, api.KindInstance)
	vols, _ := store.List[api.Volume](c.Store, api.KindVolume)
	s := &state{workloads: map[string]*api.Workload{}, instances: insts,
		volumes: map[string]api.Volume{}, byWL: map[string][]*api.Instance{}}
	for i := range wls {
		s.workloads[wls[i].Name] = &wls[i]
	}
	for _, v := range vols {
		s.volumes[v.Name] = v
	}
	for i := range s.instances {
		in := &s.instances[i]
		s.byWL[in.Workload] = append(s.byWL[in.Workload], in)
	}
	return s
}

// Reconcile runs one full pass.
func (c *Controller) Reconcile() {
	start := time.Now()
	defer reconcileDuration.Since(start)

	s := c.read()
	c.collectGarbage(s)
	c.handleLost(s)

	s = c.read() // GC and lost handling change the picture
	names := make([]string, 0, len(s.workloads))
	for n := range s.workloads {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		wl := s.workloads[n]
		insts := s.byWL[n]
		c.reconcileService(wl, insts)
		c.updateStatus(wl, insts)
	}
}

// collectGarbage deletes terminated instances, terminates instances of
// deleted workloads, and removes volumes of deleted workloads once nothing
// uses them.
func (c *Controller) collectGarbage(s *state) {
	used := map[string]bool{}
	for i := range s.instances {
		in := &s.instances[i]
		if in.Status.Phase == api.PhaseTerminated {
			if err := c.Store.Delete(api.KindInstance, in.Name, in.ResourceVersion); err == nil {
				instanceActions.Inc("delete")
			}
			continue
		}
		for _, v := range in.Volumes {
			used[v] = true
		}
		if _, ok := s.workloads[in.Workload]; !ok {
			c.terminate(in, "workload deleted")
		}
	}
	for name, v := range s.volumes {
		if _, ok := s.workloads[v.Workload]; !ok && !used[name] {
			if err := c.Store.Delete(api.KindVolume, name, 0); err == nil {
				instanceActions.Inc("delete_volume")
				c.Log.Info("volume deleted", "volume", name, "workload", v.Workload)
			}
		}
	}
}

// handleLost repairs instances on nodes that stopped heartbeating (marked
// Lost by the node monitor). Stateless replicas are dropped immediately and
// replaced by the normal replica-count logic: a short-lived duplicate, if the
// node was only partitioned, is harmless. Stateful replicas keep their
// identity and volume and are re-queued for scheduling only after the fence
// delay, so two copies never write the same volume.
func (c *Controller) handleLost(s *state) {
	for i := range s.instances {
		in := &s.instances[i]
		if in.Status.Phase != api.PhaseLost {
			continue
		}
		if !in.Stateful() {
			c.setPhase(in, api.PhaseTerminated, "node lost; replaced")
			continue
		}
		if c.now().Sub(in.Status.UpdatedAt) < c.StatefulFenceDelay {
			continue
		}
		old := in.Node
		in.Node = ""
		in.Status = api.InstanceStatus{Phase: api.PhasePending, Restarts: in.Status.Restarts,
			Message: "rescheduling after loss of node " + old, UpdatedAt: c.now().UTC()}
		if err := c.Store.Update(api.KindInstance, in); err != nil {
			continue
		}
		for _, vn := range in.Volumes {
			_ = store.Mutate(c.Store, api.KindVolume, vn, func(v *api.Volume) bool {
				v.BoundNode, v.Instance, v.Phase = "", "", api.VolumeAvailable
				return true
			})
		}
		instanceActions.Inc("reschedule_stateful")
		c.Log.Info("stateful instance rescheduling", "instance", in.Name, "from", old, "volumes", in.Volumes)
	}
}

func (c *Controller) newInstance(wl *api.Workload, name string, ordinal int, volumes []string) error {
	in := &api.Instance{
		Meta:       api.Meta{Name: name, Labels: map[string]string{"workload": wl.Name}},
		Workload:   wl.Name,
		Ordinal:    ordinal,
		Generation: wl.Generation,
		Spec:       wl.Spec,
		Volumes:    volumes,
		Status:     api.InstanceStatus{Phase: api.PhasePending, UpdatedAt: c.now().UTC()},
	}
	if err := c.Store.Create(api.KindInstance, in); err != nil {
		return err
	}
	instanceActions.Inc("create")
	c.Log.Info("instance created", "instance", name, "workload", wl.Name, "generation", wl.Generation)
	return nil
}

// terminate moves an instance to Terminating (agent stops it) or straight to
// Terminated if no agent is responsible for it.
func (c *Controller) terminate(in *api.Instance, why string) {
	switch in.Status.Phase {
	case api.PhaseTerminating, api.PhaseTerminated:
		return
	case api.PhasePending, api.PhaseLost:
		c.setPhase(in, api.PhaseTerminated, why)
		return
	}
	c.setPhase(in, api.PhaseTerminating, why)
}

func (c *Controller) setPhase(in *api.Instance, p api.Phase, why string) {
	from := in.Status.Phase
	in.Status.Phase = p
	in.Status.Ready = false
	in.Status.Message = why
	in.Status.UpdatedAt = c.now().UTC()
	if err := c.Store.Update(api.KindInstance, in); err != nil {
		return // conflict: next pass sees the new state
	}
	instanceActions.Inc(string(p))
	c.Log.Info("instance phase", "instance", in.Name, "workload", in.Workload, "from", from, "to", p, "reason", why)
}

func (c *Controller) updateStatus(wl *api.Workload, insts []*api.Instance) {
	st := api.WorkloadStatus{ObservedGeneration: wl.Generation}
	for _, in := range insts {
		if !in.Status.Phase.Live() {
			continue
		}
		st.Replicas++
		if in.Status.Ready {
			st.ReadyReplicas++
		}
		if in.Generation == wl.Generation {
			st.UpdatedReplicas++
		}
	}
	for _, in := range insts {
		if in.Status.Phase == api.PhasePending && in.Status.Message != "" {
			st.Message = in.Name + ": " + in.Status.Message
			break
		}
	}
	if st == wl.Status {
		return
	}
	_ = store.Mutate(c.Store, api.KindWorkload, wl.Name, func(w *api.Workload) bool {
		if w.Generation != wl.Generation {
			return false // changed under us; next pass
		}
		w.Status = st
		return true
	})
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func randSuffix() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Package agent is the node agent. It registers the node, heartbeats, pulls
// assignments, runs them via a Runtime and reports status. Only outbound
// calls; restarts are decided locally so they still happen if the control
// plane is down.
package agent

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
	"github.com/anusha2211/instacloud-runtime/internal/apiserver"
	"github.com/anusha2211/instacloud-runtime/internal/client"
	"github.com/anusha2211/instacloud-runtime/internal/metrics"
	"github.com/anusha2211/instacloud-runtime/internal/runtime"
)

var (
	agentStarts = metrics.Default.Counter("instacloud_agent_instance_starts_total",
		"Instance starts (including restarts).", "workload")
	agentExits = metrics.Default.Counter("instacloud_agent_instance_exits_total",
		"Instance exits by reason.", "workload", "reason")
	agentProbes = metrics.Default.Counter("instacloud_agent_health_probes_total",
		"Health probes by result.", "workload", "result")
	agentRunning = metrics.Default.Gauge("instacloud_agent_instances",
		"Instances managed by this agent.")
	agentEnforced = metrics.Default.Gauge("instacloud_agent_cgroup_enforced",
		"1 if cgroup resource limits are enforced on this node.")
	agentHeartbeatFail = metrics.Default.Counter("instacloud_agent_heartbeat_failures_total",
		"Failed heartbeats to the control plane.")
)

// Config configures an agent.
type Config struct {
	NodeName          string
	Address           string // address the router uses to reach instances
	Zone              string
	Labels            map[string]string
	Capacity          api.Resources
	Reserved          api.Resources
	PortBase          int // first port of this node's range
	PortCount         int
	VolumeRoot        string // where volume directories live
	HeartbeatInterval time.Duration
	SyncInterval      time.Duration
	// LeaseDuration: if the agent cannot heartbeat for this long it assumes it
	// has been declared dead and fences itself by stopping stateful instances,
	// so a partitioned node never keeps writing to a volume that has moved.
	LeaseDuration time.Duration
}

// Agent manages instances on one node.
type Agent struct {
	cfg Config
	api *client.Client
	rt  runtime.Runtime
	log *slog.Logger
	now func() time.Time

	mu       sync.Mutex
	insts    map[string]*local
	ports    map[int]string
	lastBeat time.Time
	fenced   bool
}

// local is the agent's bookkeeping for one instance.
type local struct {
	inst      api.Instance
	volumes   map[string]string
	port      int
	started   bool
	startedAt time.Time
	restarts  int
	nextStart time.Time
	status    api.InstanceStatus // desired report
	reported  api.InstanceStatus // last successfully reported
	stopping  bool
}

func New(cfg Config, c *client.Client, rt runtime.Runtime, log *slog.Logger) *Agent {
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 2 * time.Second
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = time.Second
	}
	if cfg.LeaseDuration == 0 {
		cfg.LeaseDuration = 10 * time.Second
	}
	if abs, err := filepath.Abs(cfg.VolumeRoot); err == nil {
		cfg.VolumeRoot = abs
	}
	if cfg.PortCount == 0 {
		cfg.PortCount = 1000
	}
	a := &Agent{cfg: cfg, api: c, rt: rt, log: log.With("node", cfg.NodeName),
		now: time.Now, insts: map[string]*local{}, ports: map[int]string{}}
	return a
}

// Register announces the node to the control plane, retrying until it works.
func (a *Agent) Register(ctx context.Context) error {
	req := apiserver.RegisterRequest{Address: a.cfg.Address, Zone: a.cfg.Zone, Capacity: a.cfg.Capacity,
		Reserved: a.cfg.Reserved, Runtime: a.rt.Name(), Labels: a.cfg.Labels}
	for {
		err := a.api.Register(ctx, a.cfg.NodeName, req)
		if err == nil {
			a.mu.Lock()
			a.lastBeat = a.now()
			a.mu.Unlock()
			a.log.Info("registered", "capacity", a.cfg.Capacity.String(), "runtime", a.rt.Name())
			return nil
		}
		a.log.Warn("register failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Run registers and then heartbeats and syncs until ctx is cancelled. On
// shutdown all instances are stopped (node drain).
func (a *Agent) Run(ctx context.Context) error {
	if p, ok := a.rt.(interface{ Enforced() bool }); ok && p.Enforced() {
		agentEnforced.Set(1)
	} else {
		agentEnforced.Set(0)
	}
	if err := a.Register(ctx); err != nil {
		return err
	}
	hb := time.NewTicker(a.cfg.HeartbeatInterval)
	sync := time.NewTicker(a.cfg.SyncInterval)
	defer hb.Stop()
	defer sync.Stop()
	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return nil
		case <-hb.C:
			a.heartbeat(ctx)
		case <-sync.C:
			a.Sync(ctx)
		}
	}
}

func (a *Agent) heartbeat(ctx context.Context) {
	err := a.api.Heartbeat(ctx, a.cfg.NodeName)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil {
		if a.fenced {
			a.log.Info("control plane reachable again; lifting self-fence")
		}
		a.lastBeat, a.fenced = a.now(), false
		return
	}
	agentHeartbeatFail.Inc()
	if client.IsNotFound(err) {
		go func() { _ = a.Register(context.Background()) }()
	}
	if !a.fenced && a.now().Sub(a.lastBeat) > a.cfg.LeaseDuration {
		a.fenced = true
		a.log.Warn("lease expired without contact; fencing stateful instances", "since", a.lastBeat)
		for _, l := range a.insts {
			if l.inst.Stateful() && l.started {
				go a.stop(l, "self-fenced: lost contact with control plane")
			}
		}
	}
}

// Sync reconciles local instances with the control plane's assignments.
func (a *Agent) Sync(ctx context.Context) {
	assigns, err := a.api.Assignments(ctx, a.cfg.NodeName)
	if err != nil {
		// Control plane unavailable: keep running what we have, keep
		// health-checking and restarting locally.
		a.log.Debug("assignments unavailable", "err", err)
		a.tickLocal(ctx)
		return
	}
	want := map[string]apiserver.Assignment{}
	for _, as := range assigns {
		want[as.Instance.Name] = as
	}

	a.mu.Lock()
	fenced := a.fenced
	// New assignments.
	for name, as := range want {
		l, ok := a.insts[name]
		if !ok {
			l = &local{inst: as.Instance}
			a.insts[name] = l
		}
		l.inst = as.Instance
		l.volumes = map[string]string{}
		for _, v := range as.Volumes {
			l.volumes[v.Name] = filepath.Join(a.cfg.VolumeRoot, v.Name)
		}
	}
	// Instances no longer assigned to us (deleted, or rescheduled elsewhere
	// while we were partitioned): stop them.
	var orphans []*local
	for name, l := range a.insts {
		if _, ok := want[name]; !ok {
			orphans = append(orphans, l)
		}
	}
	a.mu.Unlock()

	for _, l := range orphans {
		a.log.Info("instance no longer assigned here; stopping", "instance", l.inst.Name)
		a.stop(l, "unassigned")
		a.forget(l)
	}
	if fenced {
		return
	}
	a.tickLocal(ctx)
}

// tickLocal drives each local instance one step and reports changes.
func (a *Agent) tickLocal(ctx context.Context) {
	a.mu.Lock()
	list := make([]*local, 0, len(a.insts))
	for _, l := range a.insts {
		list = append(list, l)
	}
	agentRunning.Set(float64(len(list)))
	a.mu.Unlock()

	var wg sync.WaitGroup
	for _, l := range list {
		wg.Add(1)
		go func(l *local) {
			defer wg.Done()
			a.step(ctx, l)
			a.report(ctx, l)
		}(l)
	}
	wg.Wait()
}

// step is the per-instance state machine on the node.
func (a *Agent) step(ctx context.Context, l *local) {
	now := a.now()

	if l.inst.Status.Phase == api.PhaseTerminating {
		if !l.stopping {
			a.stop(l, "terminated by control plane")
			l.status = api.InstanceStatus{Phase: api.PhaseTerminated, Restarts: l.restarts, Message: "stopped"}
		}
		return
	}
	if l.stopping {
		return
	}

	if !l.started {
		if now.Before(l.nextStart) {
			return
		}
		a.start(ctx, l)
		return
	}

	st, err := a.rt.Status(l.inst.Name)
	if err != nil || !st.Running {
		// Process exited: schedule a restart with backoff.
		reason, code := "Exited", -1
		if err == nil {
			reason, code = st.Reason, st.ExitCode
		}
		agentExits.Inc(l.inst.Workload, reason)
		a.scheduleRestart(l, fmt.Sprintf("%s (code %d)", reason, code))
		return
	}

	l.status.Phase, l.status.Ready = api.PhaseRunning, true
}

func (a *Agent) start(ctx context.Context, l *local) {
	spec := l.inst.Spec
	port := 0
	if spec.Port {
		var err error
		if port, err = a.allocPort(l.inst.Name); err != nil {
			l.status = api.InstanceStatus{Phase: api.PhaseFailed, Restarts: l.restarts, Message: err.Error()}
			return
		}
	}
	for _, dir := range l.volumes {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			a.scheduleRestart(l, "volume: "+err.Error())
			return
		}
	}
	_ = a.rt.Remove(l.inst.Name) // clear a previous exited run
	rs := runtime.InstanceSpec{ID: l.inst.Name, Workload: l.inst.Workload, Ordinal: l.inst.Ordinal,
		Command: spec.Command, Env: spec.Env, Resources: spec.Resources, Port: port, Volumes: l.volumes}
	if err := a.rt.Start(ctx, rs); err != nil {
		a.scheduleRestart(l, "start failed: "+err.Error())
		return
	}
	agentStarts.Inc(l.inst.Workload)
	l.port, l.started, l.startedAt = port, true, a.now()
	st, _ := a.rt.Status(l.inst.Name)
	l.status = api.InstanceStatus{Phase: api.PhaseStarting, Restarts: l.restarts, PID: st.PID}
	if port > 0 {
		l.status.Endpoint = fmt.Sprintf("%s:%d", a.cfg.Address, port)
	}
	l.status.Phase, l.status.Ready = api.PhaseRunning, true
	a.log.Info("instance started", "instance", l.inst.Name, "workload", l.inst.Workload,
		"pid", st.PID, "port", port, "restarts", l.restarts, "enforced", st.Enforced)
}

func (a *Agent) scheduleRestart(l *local, why string) {
	l.started = false
	l.restarts++
	d := time.Second // TODO: exponential backoff
	l.nextStart = a.now().Add(d)
	l.status = api.InstanceStatus{Phase: api.PhaseFailed, Restarts: l.restarts, Endpoint: "",
		Message: fmt.Sprintf("%s; restarting in %s", why, d.Round(time.Millisecond))}
	a.releasePort(l.inst.Name)
	a.log.Warn("instance down; backing off", "instance", l.inst.Name, "reason", why, "restarts", l.restarts, "delay", d)
}

func (a *Agent) stop(l *local, why string) {
	a.mu.Lock()
	if l.stopping {
		a.mu.Unlock()
		return
	}
	l.stopping = true
	a.mu.Unlock()
	_ = a.rt.Stop(context.Background(), l.inst.Name, grace(l.inst.Spec))
	_ = a.rt.Remove(l.inst.Name)
	a.releasePort(l.inst.Name)
	a.mu.Lock()
	l.started = false
	l.stopping = !a.fenced || !l.inst.Stateful() // fenced instances may restart later
	a.mu.Unlock()
	a.log.Info("instance stopped", "instance", l.inst.Name, "reason", why)
}

func (a *Agent) forget(l *local) {
	a.mu.Lock()
	delete(a.insts, l.inst.Name)
	a.mu.Unlock()
}

// report pushes status if it changed since the last successful report.
func (a *Agent) report(ctx context.Context, l *local) {
	if l.status.Phase == "" || l.status == l.reported {
		return
	}
	err := a.api.ReportStatus(ctx, l.inst.Name, a.cfg.NodeName, l.status)
	switch {
	case err == nil:
		l.reported = l.status
		if l.status.Phase == api.PhaseTerminated {
			a.forget(l)
		}
	case client.IsConflict(err):
		// Rejected: the instance moved or is terminating. The next sync
		// re-reads assignments and stops it if needed.
		a.log.Debug("status rejected", "instance", l.inst.Name, "err", err)
		l.reported = l.status
	case client.IsNotFound(err):
		a.stop(l, "instance deleted")
		a.forget(l)
	default:
		a.log.Debug("status report failed", "instance", l.inst.Name, "err", err)
	}
}

func (a *Agent) shutdown() {
	a.mu.Lock()
	list := make([]*local, 0, len(a.insts))
	for _, l := range a.insts {
		list = append(list, l)
	}
	a.mu.Unlock()
	a.log.Info("agent shutting down; stopping instances", "count", len(list))
	var wg sync.WaitGroup
	for _, l := range list {
		wg.Add(1)
		go func(l *local) { defer wg.Done(); a.stop(l, "agent shutdown") }(l)
	}
	wg.Wait()
}

func (a *Agent) allocPort(owner string) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for p, o := range a.ports {
		if o == owner {
			return p, nil
		}
	}
	for p := a.cfg.PortBase; p < a.cfg.PortBase+a.cfg.PortCount; p++ {
		if _, used := a.ports[p]; !used {
			a.ports[p] = owner
			return p, nil
		}
	}
	return 0, fmt.Errorf("no free ports in [%d,%d)", a.cfg.PortBase, a.cfg.PortBase+a.cfg.PortCount)
}

func (a *Agent) releasePort(owner string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for p, o := range a.ports {
		if o == owner {
			delete(a.ports, p)
		}
	}
}

func grace(s api.WorkloadSpec) time.Duration {
	return time.Duration(s.TerminationGraceSeconds) * time.Second
}

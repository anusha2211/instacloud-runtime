package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Process runs each instance as a host process in its own process group, and
// in its own cgroup v2 leaf when we can create one. Resource isolation only,
// not a security boundary - fine for dev and trusted pools.
type Process struct {
	LogDir  string // per-instance stdout/stderr files
	cgroups *Cgroups
	log     *slog.Logger

	mu    sync.Mutex
	procs map[string]*proc
}

type proc struct {
	cmd   *exec.Cmd
	state State
	done  chan struct{}
	logf  *os.File
}

// NewProcess creates a process runtime. If cgroups cannot be set up it logs
// why and runs without enforcement (State.Enforced = false).
func NewProcess(logDir string, cgroupName string, log *slog.Logger) *Process {
	p := &Process{LogDir: logDir, log: log, procs: map[string]*proc{}}
	cg, err := DetectCgroups("", cgroupName)
	if err != nil {
		log.Warn("cgroup v2 limits disabled; running without resource enforcement", "reason", err)
	} else {
		log.Info("cgroup v2 limits enabled", "root", cg.Root)
		p.cgroups = cg
	}
	return p
}

func (p *Process) Name() string { return "process" }

// Enforced reports whether cgroup limits are applied.
func (p *Process) Enforced() bool { return p.cgroups != nil }

func (p *Process) Start(_ context.Context, spec InstanceSpec) error {
	if len(spec.Command) == 0 {
		return errors.New("empty command")
	}
	p.mu.Lock()
	if old, ok := p.procs[spec.ID]; ok && old.state.Running {
		p.mu.Unlock()
		return fmt.Errorf("%s already running", spec.ID)
	}
	p.mu.Unlock()

	if err := os.MkdirAll(p.LogDir, 0o755); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(p.LogDir, spec.ID+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	bin := spec.Command[0]
	if strings.Contains(bin, "/") && !filepath.IsAbs(bin) {
		// The working directory becomes the volume, so resolve relative
		// binaries against the agent's directory first.
		if abs, err := filepath.Abs(bin); err == nil {
			bin = abs
		}
	}
	cmd := exec.Command(bin, spec.Command[1:]...)
	cmd.Env = buildEnv(spec)
	cmd.Stdout, cmd.Stderr = logf, logf
	// Own process group so Stop can signal the whole tree. Pdeathsig makes
	// instances share fate with the agent, the way VMs die with their host:
	// killing an agent is an honest simulation of losing the machine.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if dir := firstVolume(spec); dir != "" {
		cmd.Dir = dir
	}

	enforced := false
	if p.cgroups != nil {
		if _, err := p.cgroups.Create(spec.ID, spec.Resources); err != nil {
			p.log.Warn("cgroup create failed; starting unenforced", "instance", spec.ID, "err", err)
		} else {
			enforced = true
		}
	}
	fmt.Fprintf(logf, "--- %s starting %v\n", time.Now().Format(time.RFC3339), spec.Command)
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("start %s: %w", spec.ID, err)
	}
	if enforced {
		// Small window where the child runs outside its cgroup; acceptable for
		// this runtime (a production runtime uses clone3(CLONE_INTO_CGROUP)).
		if err := p.cgroups.AddProc(spec.ID, cmd.Process.Pid); err != nil {
			p.log.Warn("cgroup attach failed", "instance", spec.ID, "err", err)
			enforced = false
		}
	}
	pr := &proc{cmd: cmd, done: make(chan struct{}), logf: logf,
		state: State{Running: true, PID: cmd.Process.Pid, StartedAt: time.Now(), Enforced: enforced}}
	p.mu.Lock()
	p.procs[spec.ID] = pr
	p.mu.Unlock()
	go p.wait(spec.ID, pr)
	return nil
}

func (p *Process) wait(id string, pr *proc) {
	err := pr.cmd.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	pr.state.Running = false
	pr.state.ExitedAt = time.Now()
	pr.state.Reason = "Exited"
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			pr.state.ExitCode = 128 + int(ws.Signal())
			pr.state.Reason = "Killed"
		} else {
			pr.state.ExitCode = ee.ExitCode()
		}
	}
	if p.cgroups != nil && p.cgroups.OOMKilled(id) {
		pr.state.Reason = "OOMKilled"
	}
	fmt.Fprintf(pr.logf, "--- %s exited code=%d reason=%s\n", time.Now().Format(time.RFC3339), pr.state.ExitCode, pr.state.Reason)
	pr.logf.Close()
	close(pr.done)
}

func (p *Process) Stop(_ context.Context, id string, grace time.Duration) error {
	p.mu.Lock()
	pr, ok := p.procs[id]
	p.mu.Unlock()
	if !ok {
		return nil
	}
	select {
	case <-pr.done:
		return nil
	default:
	}
	pgid := -pr.cmd.Process.Pid
	_ = syscall.Kill(pgid, syscall.SIGTERM)
	select {
	case <-pr.done:
	case <-time.After(grace):
		_ = syscall.Kill(pgid, syscall.SIGKILL)
		<-pr.done
	}
	return nil
}

func (p *Process) Status(id string) (State, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pr, ok := p.procs[id]
	if !ok {
		return State{}, ErrNotFound
	}
	return pr.state, nil
}

func (p *Process) Remove(id string) error {
	p.mu.Lock()
	pr, ok := p.procs[id]
	if ok && pr.state.Running {
		p.mu.Unlock()
		return fmt.Errorf("%s still running", id)
	}
	delete(p.procs, id)
	p.mu.Unlock()
	if p.cgroups != nil {
		return p.cgroups.Remove(id)
	}
	return nil
}

func buildEnv(spec InstanceSpec) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"INSTANCE_ID=" + spec.ID,
		"WORKLOAD=" + spec.Workload,
	}
	if spec.Ordinal >= 0 {
		env = append(env, "INSTANCE_ORDINAL="+strconv.Itoa(spec.Ordinal))
	}
	if spec.Port > 0 {
		env = append(env, "PORT="+strconv.Itoa(spec.Port))
	}
	for name, path := range spec.Volumes {
		env = append(env, "VOLUME_"+envName(name)+"="+path)
	}
	if dir := firstVolume(spec); dir != "" {
		env = append(env, "DATA_DIR="+dir)
	}
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	return env
}

func firstVolume(spec InstanceSpec) string {
	for _, path := range spec.Volumes {
		return path
	}
	return ""
}

func envName(s string) string {
	b := []byte(s)
	for i, c := range b {
		switch {
		case c >= 'a' && c <= 'z':
			b[i] = c - 32
		case c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		default:
			b[i] = '_'
		}
	}
	return string(b)
}

var _ Runtime = (*Process)(nil)
var _ Runtime = (*Fake)(nil)

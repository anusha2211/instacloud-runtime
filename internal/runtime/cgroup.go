package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// cgroupPeriod is the CFS period used for cpu.max (100ms, the kernel default).
const cgroupPeriod = 100000

// Cgroups manages one cgroup v2 subtree for all instances on this node.
type Cgroups struct {
	Root string // e.g. /sys/fs/cgroup/instacloud
}

// DetectCgroups returns a manager if cgroup v2 is mounted and we can create a
// subtree with the cpu and memory controllers enabled, or an error explaining
// why not (no v2, read-only, no delegation).
func DetectCgroups(base, name string) (*Cgroups, error) {
	if base == "" {
		base = "/sys/fs/cgroup"
	}
	if _, err := os.Stat(filepath.Join(base, "cgroup.controllers")); err != nil {
		return nil, fmt.Errorf("cgroup v2 not mounted at %s", base)
	}
	ctrl, err := os.ReadFile(filepath.Join(base, "cgroup.controllers"))
	if err != nil {
		return nil, err
	}
	if !strings.Contains(string(ctrl), "cpu") || !strings.Contains(string(ctrl), "memory") {
		return nil, fmt.Errorf("cpu/memory controllers not available (have %q)", strings.TrimSpace(string(ctrl)))
	}
	root := filepath.Join(base, name)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create %s: %w", root, err)
	}
	// Enable controllers for children of our subtree. In cgroup v2 a cgroup with
	// processes cannot also have controller-enabled children, so instances get
	// leaf cgroups and our root holds no processes.
	if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+cpu +memory"), 0o644); err != nil {
		return nil, fmt.Errorf("enable controllers in %s: %w", root, err)
	}
	return &Cgroups{Root: root}, nil
}

// Create makes a leaf cgroup for an instance and writes its limits.
func (c *Cgroups) Create(id string, r api.Resources) (string, error) {
	dir := filepath.Join(c.Root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, "cpu.max"), []byte(CPUMax(r.MilliCPU)), 0o644); err != nil {
		return "", fmt.Errorf("cpu.max: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "memory.max"), []byte(strconv.FormatInt(r.MemoryMB*1024*1024, 10)), 0o644); err != nil {
		return "", fmt.Errorf("memory.max: %w", err)
	}
	// Avoid swapping instead of OOM-killing, so memory limits behave predictably.
	_ = os.WriteFile(filepath.Join(dir, "memory.swap.max"), []byte("0"), 0o644)
	return dir, nil
}

// AddProc moves a pid into the instance cgroup.
func (c *Cgroups) AddProc(id string, pid int) error {
	return os.WriteFile(filepath.Join(c.Root, id, "cgroup.procs"), []byte(strconv.Itoa(pid)), 0o644)
}

// OOMKilled reports whether the kernel OOM-killed anything in the cgroup.
func (c *Cgroups) OOMKilled(id string) bool {
	b, err := os.ReadFile(filepath.Join(c.Root, id, "memory.events"))
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 && f[0] == "oom_kill" && f[1] != "0" {
			return true
		}
	}
	return false
}

// Remove deletes an instance cgroup (must be empty).
func (c *Cgroups) Remove(id string) error {
	err := os.Remove(filepath.Join(c.Root, id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// CPUMax renders a cgroup v2 cpu.max value for a millicore request:
// 500m -> "50000 100000" (half a CPU per period).
func CPUMax(milli int64) string {
	quota := milli * cgroupPeriod / 1000
	if quota < 1000 {
		quota = 1000 // kernel minimum is 1ms
	}
	return fmt.Sprintf("%d %d", quota, cgroupPeriod)
}

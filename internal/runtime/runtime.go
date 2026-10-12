// Package runtime defines the interface between the node agent and the
// technology that actually isolates and runs a replica (microVM, container,
// process). The agent depends only on this interface.
package runtime

import (
	"context"
	"errors"
	"time"

	"github.com/anusha2211/instacloud-runtime/internal/api"
)

// ErrNotFound is returned for unknown instance ids.
var ErrNotFound = errors.New("runtime: instance not found")

// InstanceSpec is everything a runtime needs to start one replica.
type InstanceSpec struct {
	ID        string
	Workload  string
	Ordinal   int
	Command   []string
	Env       map[string]string
	Resources api.Resources
	Port      int               // 0 if the workload does not serve
	Volumes   map[string]string // volume name -> host path
}

// State is the runtime's view of an instance.
type State struct {
	Running   bool
	PID       int
	ExitCode  int
	Reason    string // e.g. "Exited", "OOMKilled", "Killed"
	StartedAt time.Time
	ExitedAt  time.Time
	// Enforced reports whether resource limits are actually applied.
	Enforced bool
}

// Runtime starts and stops isolated replicas on one machine.
//
// Implementations: Process (this package; cgroup v2 limits), Fake (tests).
// A Firecracker implementation would boot a jailed microVM per instance with
// vCPUs/memory from Resources, volumes as virtio-block devices and a tap
// device on the tenant overlay.
type Runtime interface {
	Name() string
	// Start launches the instance. Starting an id that is already running is
	// an error; the agent stops before restarting.
	Start(ctx context.Context, spec InstanceSpec) error
	// Stop asks the instance to exit (SIGTERM / ACPI shutdown), and kills it
	// after grace. Stopping an unknown or exited instance is not an error.
	Stop(ctx context.Context, id string, grace time.Duration) error
	// Status reports the current state; ErrNotFound if never started.
	Status(id string) (State, error)
	// Remove forgets an exited instance and releases its resources.
	Remove(id string) error
}

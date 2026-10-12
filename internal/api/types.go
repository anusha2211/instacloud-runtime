// Package api defines the object model shared by the control plane, the node
// agent and the CLI. Objects are plain structs serialised as JSON.
package api

import (
	"fmt"
	"time"
)

// Kinds of objects kept in the state store.
const (
	KindWorkload = "workloads"
	KindInstance = "instances"
	KindNode     = "nodes"
	KindVolume   = "volumes"
)

// Meta is common object metadata.
type Meta struct {
	Name string `json:"name"`
	// ResourceVersion is assigned by the store on every write and used for
	// optimistic concurrency (compare-and-swap).
	ResourceVersion int64 `json:"resourceVersion,omitempty"`
	// Generation is bumped when the desired spec changes (not on status or
	// replica-count changes).
	Generation int64             `json:"generation,omitempty"`
	CreatedAt  time.Time         `json:"createdAt,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// Object is implemented by everything stored in the state store.
type Object interface {
	GetMeta() *Meta
}

func (m *Meta) GetMeta() *Meta { return m }

// Resources are a replica's requests, or a node's capacity.
type Resources struct {
	MilliCPU int64 `json:"milliCPU"`
	MemoryMB int64 `json:"memoryMB"`
}

func (r Resources) Add(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU + o.MilliCPU, MemoryMB: r.MemoryMB + o.MemoryMB}
}

func (r Resources) Sub(o Resources) Resources {
	return Resources{MilliCPU: r.MilliCPU - o.MilliCPU, MemoryMB: r.MemoryMB - o.MemoryMB}
}

// Fits reports whether r fits inside capacity c.
func (r Resources) Fits(c Resources) bool {
	return r.MilliCPU <= c.MilliCPU && r.MemoryMB <= c.MemoryMB
}

func (r Resources) String() string {
	return fmt.Sprintf("%dm/%dMi", r.MilliCPU, r.MemoryMB)
}

// WorkloadKind distinguishes stateless from stateful workloads.
type WorkloadKind string

const (
	KindService  WorkloadKind = "service"
	KindStateful WorkloadKind = "stateful"
)

// HealthCheck is an HTTP probe run by the node agent against each replica.
type HealthCheck struct {
	Path             string `json:"path"`
	IntervalSeconds  int    `json:"intervalSeconds,omitempty"`
	TimeoutSeconds   int    `json:"timeoutSeconds,omitempty"`
	FailureThreshold int    `json:"failureThreshold,omitempty"`
}

// VolumeSpec requests a persistent volume per replica (stateful only).
type VolumeSpec struct {
	Name      string `json:"name"`
	SizeMB    int64  `json:"sizeMB"`
	MountPath string `json:"mountPath,omitempty"`
}

// Placement holds scheduling preferences and constraints.
type Placement struct {
	// AntiAffinity forbids two replicas of this workload on the same node.
	AntiAffinity bool `json:"antiAffinity,omitempty"`
	// NodeSelector restricts placement to nodes carrying all these labels.
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
}

// AutoscaleSpec enables horizontal autoscaling on request rate.
type AutoscaleSpec struct {
	MinReplicas         int     `json:"minReplicas"`
	MaxReplicas         int     `json:"maxReplicas"`
	TargetRPSPerReplica float64 `json:"targetRPSPerReplica"`
}

// WorkloadSpec is the user-declared desired state.
type WorkloadSpec struct {
	Kind        WorkloadKind      `json:"kind"`
	Replicas    int               `json:"replicas"`
	Command     []string          `json:"command"`
	Env         map[string]string `json:"env,omitempty"`
	Resources   Resources         `json:"resources"`
	Port        bool              `json:"port,omitempty"` // allocate a serving port
	HealthCheck *HealthCheck      `json:"healthCheck,omitempty"`
	Volumes     []VolumeSpec      `json:"volumes,omitempty"`
	Placement   Placement         `json:"placement,omitempty"`
	Autoscale   *AutoscaleSpec    `json:"autoscale,omitempty"`
	// TerminationGraceSeconds between SIGTERM and SIGKILL.
	TerminationGraceSeconds int `json:"terminationGraceSeconds,omitempty"`
}

// WorkloadStatus is maintained by the reconciler.
type WorkloadStatus struct {
	ObservedGeneration int64  `json:"observedGeneration"`
	Replicas           int    `json:"replicas"`
	ReadyReplicas      int    `json:"readyReplicas"`
	UpdatedReplicas    int    `json:"updatedReplicas"`
	Message            string `json:"message,omitempty"`
}

type Workload struct {
	Meta   `json:"metadata"`
	Spec   WorkloadSpec   `json:"spec"`
	Status WorkloadStatus `json:"status"`
}

// Phase is an instance lifecycle phase. See lifecycle.go for transitions.
type Phase string

const (
	PhasePending     Phase = "Pending"
	PhaseScheduled   Phase = "Scheduled"
	PhaseStarting    Phase = "Starting"
	PhaseRunning     Phase = "Running"
	PhaseUnhealthy   Phase = "Unhealthy"
	PhaseFailed      Phase = "Failed"
	PhaseLost        Phase = "Lost"
	PhaseTerminating Phase = "Terminating"
	PhaseTerminated  Phase = "Terminated"
)

// Instance is a single replica of a workload.
type Instance struct {
	Meta       `json:"metadata"`
	Workload   string         `json:"workload"`
	Ordinal    int            `json:"ordinal"` // -1 for stateless replicas
	Generation int64          `json:"workloadGeneration"`
	Spec       WorkloadSpec   `json:"spec"` // copy of the spec it was built from
	Node       string         `json:"node,omitempty"`
	Volumes    []string       `json:"volumes,omitempty"` // bound volume names
	Status     InstanceStatus `json:"status"`
}

type InstanceStatus struct {
	Phase     Phase     `json:"phase"`
	Ready     bool      `json:"ready"`
	Restarts  int       `json:"restarts"`
	Endpoint  string    `json:"endpoint,omitempty"` // host:port
	PID       int       `json:"pid,omitempty"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
}

// Stateful reports whether the instance has a stable identity.
func (i *Instance) Stateful() bool { return i.Ordinal >= 0 }

// Request returns the resources reserved by this instance.
func (i *Instance) Request() Resources { return i.Spec.Resources }

// Holds reports whether the instance currently holds resources on its node.
func (i *Instance) Holds() bool {
	if i.Node == "" {
		return false
	}
	switch i.Status.Phase {
	case PhaseTerminated, PhaseLost, PhasePending:
		return false
	}
	return true
}

type NodeStatus string

const (
	NodeReady    NodeStatus = "Ready"
	NodeNotReady NodeStatus = "NotReady"
	NodeDraining NodeStatus = "Draining"
)

// Node is a machine running the node agent.
type Node struct {
	Meta          `json:"metadata"`
	Address       string     `json:"address"` // host reachable by router
	Zone          string     `json:"zone,omitempty"`
	Capacity      Resources  `json:"capacity"`
	Allocatable   Resources  `json:"allocatable"`
	Runtime       string     `json:"runtime,omitempty"`
	Status        NodeStatus `json:"status"`
	LastHeartbeat time.Time  `json:"lastHeartbeat"`
	Message       string     `json:"message,omitempty"`
}

type VolumePhase string

const (
	VolumeAvailable VolumePhase = "Available"
	VolumeBound     VolumePhase = "Bound"
)

// Volume is a persistent volume owned by one ordinal of a stateful workload.
type Volume struct {
	Meta      `json:"metadata"`
	Workload  string      `json:"workload"`
	Ordinal   int         `json:"ordinal"`
	SizeMB    int64       `json:"sizeMB"`
	MountPath string      `json:"mountPath,omitempty"`
	BoundNode string      `json:"boundNode,omitempty"`
	Instance  string      `json:"instance,omitempty"`
	Phase     VolumePhase `json:"phase"`
}

// InstanceName returns the stable name for a stateful ordinal.
func InstanceName(workload string, ordinal int) string {
	return fmt.Sprintf("%s-%d", workload, ordinal)
}

// VolumeName returns the name of a stateful ordinal's volume.
func VolumeName(workload, vol string, ordinal int) string {
	return fmt.Sprintf("%s-%s-%d", workload, vol, ordinal)
}

# InstaCloud Compute Runtime - Design

_Draft - still filling in sections._

How a workload goes from a submitted spec to isolated, running, reachable replicas,
and how it stays that way through load changes, deploys and failures.
[REQUIREMENTS.md](REQUIREMENTS.md) has the problem breakdown; this is about the decisions.

Anything marked (built) has working code in this repo. The rest is design only.


## 1. Goals and non-goals

Goals:

- One declarative API for stateless and stateful workloads.
- Strong isolation between tenants on shared hardware.
- Level-triggered control loops: any component can crash and restart and the system
  converges again from stored state.
- The data plane keeps serving when the control plane is degraded.
- Every decision (placement, restart, reschedule, scale) is observable.

Not trying to do (yet):

- Be a general Kubernetes replacement - no CRDs, no arbitrary controllers.
- Multi-region control planes - a control plane owns one region / cell.
- Batch / gang scheduling, GPUs, preemption by priority (noted in evolution).

## 2. Architecture overview

See [diagrams/architecture.md](diagrams/architecture.md).

The system is split into a **control plane** that decides and a **data plane** that
executes. The split is deliberate: the control plane can be slow, restarted or briefly
unavailable without user traffic noticing.

| Component | Responsibility | Writes | Reads |
|---|---|---|---|
| API server (built) | Validate and admit specs, bump generation, expose state | `Workload` | all |
| State store (built) | Versioned KV with compare-and-swap and watch | - | - |
| Scheduler (built) | Bind `Pending` instances to nodes | `Instance.node` | nodes, instances, volumes |
| Workload reconciler (built) | Make instances match spec: create, replace, delete, order stateful replicas | `Instance`, `Volume` | workloads, instances, nodes |
| Node monitor (built) | Mark nodes `NotReady` when heartbeat lease expires | `Node.status` | nodes |
| Autoscaler (built) | Adjust `replicas` from load signals | `Workload.replicas` | metrics, workloads |
| Node agent (built) | Run assigned instances via a runtime, health-check, restart, report | `Instance.status`, `Node` | its assignments |
| Router (built) | Stable entry point per workload, LB across healthy endpoints | metrics | instances |

Controllers coordinate only through the store. Each one
runs the same loop: *watch -> diff desired vs actual -> act -> write*. Watches are an
optimisation; every loop also does a periodic full resync so a missed event can only
delay convergence, never break it.

Nodes pull, the control plane never pushes. Agents poll (long-poll in production) for their
assignments and report status. The control plane never initiates connections into the
fleet, so a compromised control-plane component cannot be used to reach nodes directly
and the node firewall only needs egress.

## 3. Isolation and runtime

Tenants run untrusted code next to each other. The options considered:

| Option | Isolation | Startup | Density | Verdict |
|---|---|---|---|---|
| Plain processes / containers (namespaces + cgroups) | Shared kernel; one kernel bug = escape | ~50 ms | highest | OK for trusted / single-tenant pools only |
| gVisor (user-space kernel) | Syscall interception, smaller attack surface | ~150 ms | high | Good, but syscall overhead hurts I/O-heavy apps, compatibility gaps |
| **Firecracker microVMs** | Hardware virtualization (KVM), minimal device model, jailer | ~125 ms | high (~5 MiB overhead/VM) | **Chosen** for multi-tenant pools |
| Full VMs (QEMU) | Strong | seconds | low | Too slow and heavy per replica |

Going with Firecracker: production nodes run each replica in a **Firecracker microVM** launched
by the jailer (chroot, seccomp, dropped privileges, own cgroup). Inside the VM a tiny
init starts the workload. The VM boundary is the security boundary; cgroups on the
VM process are the resource boundary. Single-tenant / trusted pools can use the
cheaper container runtime.

The agent only knows about this interface (built):

```go
type Runtime interface {
    Name() string
    Start(ctx context.Context, spec InstanceSpec) (Handle, error)
    Stop(ctx context.Context, id string, grace time.Duration) error
    Status(id string) (State, error)
}
```

This repo ships a **process runtime** (fork/exec with cgroup v2 `cpu.max` and
`memory.max`, own process group, graceful SIGTERM -> SIGKILL) and a fake runtime for
tests. A Firecracker runtime implements the same interface: `Start` = create jailed
VM with vCPU/memory from the request, attach rootfs + volume block devices and a tap
device, boot; `Stop` = send Ctrl-Alt-Del, wait grace, kill VMM.

## 4. Object model

All objects carry `metadata { name, resourceVersion, generation, createdAt }`.

- **Workload** - desired state. `kind` (`service` | `stateful`), `replicas`, `command`,
  `env`, `resources {milliCPU, memoryMB}`, `port`, `healthCheck`, `volumes`,
  `placement {antiAffinity, spreadByZone}`, `autoscale {min, max, targetRPSPerReplica}`.
  Updating anything but `replicas` bumps `generation` (triggers a rollout).
- **Instance** - one replica. `workload`, `ordinal` (stateful), `generation` it was
  built from, `node` binding, `phase`, `ready`, `restarts`, `endpoint`, `message`.
- **Node** - a machine. `capacity`, `allocatable`, `zone`, `labels`, `address`,
  `lastHeartbeat`, `status` (`Ready` | `NotReady` | `Draining`).
- **Volume** - persistent volume for a stateful replica: `workload`, `ordinal`, `sizeMB`,
  `boundNode`, `phase`. Named `<workload>-<volume>-<ordinal>`; it outlives instances.

Desired state (Workload) and observed state (Instance status, Node heartbeats) are
separate objects with separate writers, so a heartbeat storm never contends with a
user's deploy.

## 5. Scheduling and placement

The scheduler binds each `Pending` instance to a node in two phases, the same shape
as the Kubernetes scheduler framework and Borg:

First filter out nodes that:

1. are not `Ready` (or are draining),
2. do not have `allocatable - sum(requests of instances bound there)` >= request
   on both CPU and memory,
3. already run a replica of the same workload when `antiAffinity` is set,
4. are not the node a stateful replica's volume is pinned to, when the volume is
   node-local and that node is still alive (see [section 10](#10-stateful-workloads-and-persistent-volumes)).

Then score what's left (weighted sum):

- **Bin-packing** (`MostAllocated`): prefer nodes that will be fuller after placement,
  using the max of CPU and memory utilisation so neither dimension is wasted. Packing
  leaves whole machines free for large requests and for draining/maintenance, and
  lets the fleet scale down.
- **Spread**: penalise nodes and zones already running replicas of the same workload,
  so one machine or zone loss does not take out a whole service.
- **Balance**: penalise placements that leave CPU and memory very unevenly used on a
  node (stranded capacity).

Ties break on node name so results are deterministic (makes tests and debugging much easier).

TODO: scoring every feasible node is O(nodes) per instance. Fine here, needs sampling
at a few thousand nodes (see evolution section).

Packing and spreading pull in opposite directions; the weights encode the policy
"pack across workloads, spread within a workload". Requests (not live usage) drive
placement, so placement is predictable and over-commit is an explicit decision rather
than an accident.

Binding is a compare-and-swap on the instance's `resourceVersion`, so two scheduler
replicas (or a scheduler racing a delete) can never double-bind; the loser just
retries on the next pass.

## 6. CPU and memory management

- Requests are reservations: the scheduler never places more requested CPU or
  memory on a node than it has allocatable (capacity minus a system reserve for the
  agent, VMM and kernel).
- CPU is compressible: enforced with cgroup v2 `cpu.max` (quota/period) so a replica
  that asks for 500m gets at most half a core under contention. In microVMs this maps
  to vCPU count plus the same quota on the VMM thread.
- Memory is not: `memory.max` is a hard limit; exceeding it OOM-kills the
  replica, which the agent reports as `OOMKilled` and restarts with backoff. VM memory
  is sized to the request plus a small guest overhead.
- Over-commit should be a per-pool policy knob (e.g. 1.5x CPU on burstable pools, never
  for memory) rather than default behaviour.
- The process runtime in this repo writes real cgroup v2 limits when it can create a
  cgroup under `/sys/fs/cgroup`, and falls back to unenforced execution with a warning
  (and the `instacloud_agent_cgroup_enforced` gauge at 0) on laptops / containers where
  it cannot.

## 7. Networking and service routing

Per replica: in production each microVM gets a tap device attached to a per-tenant
overlay (VXLAN/Geneve or an eBPF-based fabric) and its own IP; tenant isolation is
enforced at the overlay. In the demo each replica binds a host port allocated by the
agent and published as `endpoint = <node address>:<port>`.

Per workload, every workload gets a stable name; the **router** (L7 proxy) maps
`/w/<workload>/...` (or a `Host` header in production) to the set of endpoints of
instances that are `Running` **and** `ready`. It:

- discovers endpoints by watching instance state (cached, so it keeps serving if the
  control plane goes away),
- round-robins across endpoints, skipping ones that failed recently (passive outlier
  detection) and retrying idempotent requests once on connection failure,
- records per-workload request counts and latency, which feed the autoscaler.

Readiness, not liveness, gates traffic: a replica that is up but failing health checks
is removed from rotation before it is restarted.

Stateful replicas also get stable per-ordinal addresses (`/w/<workload>-<ordinal>/`)
so clients and peers can address a specific member.

# InstaCloud Compute Runtime - Design

_Last updated Oct 12, 2026._

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

## 8. Workload lifecycle

See [diagrams/lifecycle.md](diagrams/lifecycle.md) for the state machine and
[diagrams/dataflow.md](diagrams/dataflow.md) for the end-to-end sequence.

```
Pending -> Scheduled -> Starting -> Running -> Terminating -> Terminated
              \            \           \ 
               \            -> Failed   -> Unhealthy -> (restart) -> Starting
                -> Lost (node died)        -> Failed (restart budget exceeded) 
```

| Phase | Owner | Meaning |
|---|---|---|
| `Pending` | reconciler | instance object exists, no node |
| `Scheduled` | scheduler | bound to a node, agent has not started it yet |
| `Starting` | agent | runtime started, waiting for first successful health check |
| `Running` | agent | process alive; `ready` reflects the latest health check |
| `Unhealthy` | agent | health check failure threshold exceeded, restart pending |
| `Failed` | agent | exited / crash-looping; waiting on backoff timer |
| `Lost` | node monitor | node stopped heartbeating; status unknown |
| `Terminating` | reconciler | asked to stop (scale down, rollout, delete) |
| `Terminated` | agent | stopped, resources released; object then garbage-collected |

Transitions are validated in one place (`api.CanTransition`), so an out-of-order
status report (e.g. an agent reporting `Running` for an instance the reconciler has
already marked `Terminating`) is rejected instead of resurrecting it.

Graceful termination: the reconciler sets `Terminating`; the router stops sending
traffic as soon as it sees it; the agent sends SIGTERM (or ACPI shutdown to the VM),
waits the grace period (default 10s), then SIGKILL, releases the port and cgroup, and
reports `Terminated`. Volumes are retained.

## 9. Failure detection and recovery

| Failure | Detected by | Response |
|---|---|---|
| Process exits / crashes | agent (wait on child) | restart in place with exponential backoff (1s, 2s, 4s ... cap 60s, reset after 60s healthy) |
| Process hung / unhealthy | agent HTTP health probe, N consecutive failures | mark not ready (router drains), then restart with backoff |
| OOM | runtime (cgroup `memory.events`) | as crash, reason `OOMKilled` |
| Node dies / partitions | node monitor: heartbeat lease (default 10s) expired | node `NotReady`; its instances `Lost`; reconciler replaces them elsewhere |
| Agent restarts | agent on boot | re-adopts or restarts its assignments from the control plane (assignments are the source of truth) |
| Control plane down | agents, router | data plane keeps running last known state; reconcile resumes on recovery |
| Scheduler/reconciler crash | - | stateless; resync from store on restart |

### Lost vs. dead

The tricky part. A node that stops heartbeating may just be partitioned.
For **stateless** replicas we replace immediately - a temporary duplicate is harmless.
For **stateful** replicas a duplicate is dangerous (two writers on one volume), so the
replacement for ordinal *k* is only started once the old one is fenced: in production by
revoking the volume attachment at the storage layer (and the VM is killed by the node's
own watchdog when it loses its lease); in this repo the reconciler waits an extra
`statefulFenceDelay` after `NotReady` before re-binding the volume. When a partitioned
agent comes back it sees its instances are no longer assigned to it and stops them.

## 10. Stateful workloads and persistent volumes

Stateful workloads get, per ordinal:

- a stable **identity**: instance name `<workload>-<ordinal>` and env `INSTANCE_ORDINAL`,
  kept across restarts and rescheduling;
- a **volume** `<workload>-<vol>-<ordinal>` created before the instance and never
  deleted on scale-down (deleted only with the workload, if `retainVolumes=false`);
- **ordered** operations: ordinal *k* is created only once *k-1* is `Running` and ready;
  scale-down removes the highest ordinal first; rollouts go highest to lowest.

Volumes follow the replica. In production volumes are network block devices
(Ceph RBD / EBS-style) attached to whichever node runs the replica, with
single-attachment enforced by the storage service - which is also the fencing
mechanism. Node-local NVMe volumes are an option for latency-sensitive databases;
then the scheduler pins the replica to that node and recovery means restoring from
replication/backup rather than re-attaching.

In this repo a volume is a directory under a shared data root (standing in for a
network volume). The scheduler records `boundNode` on attach; when the instance moves,
the reconciler re-binds the volume to the new node and the same data shows up there -
the demo writes a key on `kv-1`, kills its node, and reads the key back after `kv-1`
lands elsewhere.

## 11. Deployments, restarts and scaling

### Rolling updates (built)

Changing the spec bumps `generation`. The reconciler
replaces instances whose generation is stale one at a time (`maxSurge=1,
maxUnavailable=0` for services): create a new instance, wait until it is ready, then
terminate one old one. A rollout never reduces ready capacity below `replicas`. If the
new generation does not become ready within a deadline the rollout stalls (and is
visible in status) instead of continuing to replace healthy old replicas. Stateful
workloads roll one ordinal at a time, highest first, reusing the same volume.

### Restarts

Restart policy is `Always` for services and stateful replicas.
Backoff state lives on the agent (so it survives control-plane outages) and is
reported in instance status (`restarts`, `message`).

### Autoscaling (built)

The autoscaler periodically computes, per workload with an
`autoscale` block:

```
desired = ceil(observedRPS / targetRPSPerReplica)   clamped to [min, max]
```

with two guards against flapping: a **tolerance band** (ignore changes within 10%) and
a **scale-down stabilisation window** (use the max recommendation over the last N
minutes before scaling down; scale up immediately). Scaling writes `replicas` only, so
it never triggers a rollout. Using RPS from the router since it's closest to actual demand. CPU from agents is the
obvious second signal - open question whether to combine them (max of the two
recommendations, like HPA does) or let the workload pick one.

### Cluster autoscaling (not built)

When pending instances cannot be placed for longer than
a threshold, a fleet autoscaler requests machines from the provisioning layer;
nodes that stay under a utilisation floor are drained and returned. Bin-packing in
the scheduler is what makes the scale-down side effective.

## 12. Observability

- Metrics: every binary exposes `/metrics` in Prometheus text format.
  Control plane: API request counts/latency, scheduling attempts/latency/failures,
  reconcile loop duration, instances by phase, nodes by status, autoscaler decisions.
  Agent: instance starts/restarts/exits, health probe results, cgroup enforcement.
  Router: requests, upstream errors, latency per workload.
- Logs: JSON logs via `log/slog` with consistent keys
  (`workload`, `instance`, `node`, `phase`) so one replica's life can be traced across
  components with a single query.
- Events (minimal for now): instance `message` carries the last reason
  (`OOMKilled`, `health check failed 3x`, `node lost`, `no node fits: insufficient memory`).
- Still missing for production: a proper event stream object type, per-replica log shipping from the
  VM serial console / stdout to a log pipeline, distributed tracing through the router,
  SLO dashboards (scheduling latency, time-to-ready, recovery time after node loss).

## 13. Key tradeoffs

| Decision | Chosen | Alternative | Why |
|---|---|---|---|
| Isolation | Firecracker microVMs | containers, gVisor | Untrusted multi-tenant code needs a hardware boundary; Firecracker keeps startup and density close to containers. Cost: more complex networking/storage plumbing (tap devices, block devices instead of bind mounts). |
| Control model | Declarative, level-triggered reconcilers over a shared store | Imperative workflows / RPC between services | Self-healing after any crash or missed event; each controller testable alone. Cost: eventual consistency, harder to reason about latency of a single action. |
| Coordination | All state in one store, controllers communicate via objects | Direct service-to-service calls | One source of truth, trivial audit, components can be restarted independently. Cost: store becomes the scaling bottleneck (see evolution). |
| Node communication | Agents pull assignments | Control plane pushes to nodes | One-directional trust boundary, natural backpressure, works through NAT. Cost: assignment latency = poll interval (mitigated with long-poll/watch). |
| Placement signal | Requests (reservations) | Live usage | Predictable, no feedback loops. Cost: lower utilisation; addressed later with usage-aware over-commit pools. |
| Placement policy | Pack across workloads, spread within | Pure spread / pure pack | Keeps whole machines free (scale-down, big jobs, maintenance) while one node/zone loss only hurts a fraction of any service. |
| Node failure | Lease timeout, replace stateless immediately, fence stateful first | Replace everything immediately | Duplicates are harmless for stateless, corrupting for stateful. Cost: slower recovery for stateful replicas. |
| Restart handling | On the agent with backoff | Control plane decides every restart | Fast, works during control-plane outage, avoids thundering herd on the API. |
| Autoscaling signal | Router RPS with stabilisation | CPU only | RPS tracks demand directly and is available for every service; CPU lags and is noisy for I/O-bound apps. |
| Store in this repo | In-memory with snapshot file | etcd / Raft | Keeps the repo self-contained. The `Store` interface is the seam where etcd or a Raft log plugs in. |

## What is left out and why

Cut on purpose so the time goes into the core orchestration logic actually working
end to end:

- Firecracker runtime - needs KVM and root; the runtime interface and the design
  above are what matter, and the process runtime exercises the same agent code paths.
- Overlay networking / per-replica IPs - host ports are enough to demonstrate
  endpoint discovery, readiness-gated routing and failover.
- Network block storage - simulated by a shared directory; the binding, fencing and
  re-attach logic in the reconciler is the interesting part and is implemented.
- HA control plane / Raft - single process with an in-memory store and periodic
  snapshot; HA is an operational property that would not change the controller design.
- AuthN/Z, tenants, quotas - essential for production, orthogonal to orchestration.
- Image distribution, log pipeline, tracing - standard infrastructure with
  well-known solutions.
- Preemption, priorities, GPUs, batch/gang scheduling - additive scheduler plugins.

## 15. Evolution with scale

**Now (this repo, tens of nodes):** Single control-plane process,
in-memory store, polling agents, one router.

**Production v1, hundreds of nodes in one region:**
- Store -> etcd (or Raft-replicated log) with 3-5 members; controllers run 2+ replicas
  with leader election via store leases.
- Agents use watch/long-poll instead of polling; heartbeats move to lightweight lease
  objects so they do not churn the main object.
- Firecracker runtime, overlay network, network block volumes with attachment fencing.
- Router becomes a horizontally scaled fleet behind L4 load balancers, fed by an
  endpoint-discovery service rather than reading the store.
- AuthN/Z, per-tenant quotas enforced at admission.

**Thousands of nodes per region:**
- **Cells:** shard the fleet into independent cells (~2-5k nodes each) with their own
  control plane; a thin global layer places workloads into cells. Blast radius of any
  control-plane bug is one cell.
- **Scheduler throughput:** optimistic concurrency with multiple schedulers over shared
  state (Omega-style), sampling a subset of feasible nodes per decision instead of
  scoring all, and caching node aggregates.
- **Observability at scale:** metrics aggregated per cell, sampled tracing, event
  retention policies.
- Over-commit pools driven by live usage, priority and preemption, fleet autoscaling
  tied to pending-instance pressure.

**Multi-region** (furthest out, least thought through):
- Global API with per-region control planes; workloads declare region placement.
- Global anycast/edge routing with health-aware failover between regions.
- Stateful workloads replicate across regions at the application or storage layer;
  the orchestrator handles placement and fencing, not data replication.

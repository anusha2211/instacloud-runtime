# Requirements / problem breakdown

Notes from breaking down the InstaCloud compute ask before picking a design.

The short version: take a workload definition from a customer and keep it running,
correctly sized, reachable and recoverable on a fleet of machines we don't fully
trust (tenant code is untrusted, machines fail, the network partitions).

## Who's involved

- **Tenants / app developers** - want to say "run N copies of this with X cpu / Y mem,
  on a port, with a disk" and get a stable address, status, logs and metrics back.
- **Operators** - add/drain machines, see utilization, survive a machine dying without
  getting paged.
- **The system itself** - placement, failure detection, converging actual state to desired.

## Lifecycle, broken into sub-problems

Each stage fails in its own way, which is mostly what drives the design:

1. Deploy / admission - validate, persist durably, assign a generation. Fails on bad specs
   and concurrent writers.
2. Scheduling - find a machine with room that satisfies constraints (anti-affinity,
   spread, volume locality). Fragmentation, hot spots, two schedulers racing.
3. Execution - isolated process with enforced cpu/mem. Crash loops, OOM, noisy
   neighbours, and (the scary one) escaping isolation.
4. Networking - address per replica, stable entry point per workload, only route to
   healthy replicas. Stale endpoints are the classic bug here.
5. Storage - disks that survive restarts and follow the replica if it moves. Two writers
   on one volume = corruption.
6. Scaling - change replica counts with load without flapping.
7. Updates / restarts - roll out without downtime, restart crashes without hammering.
8. Termination - SIGTERM, drain, SIGKILL; release resources; keep volumes.
9. Failure recovery - dead processes and dead machines. False positives during a
   partition can create duplicates.
10. Observability - all of the above has to be explainable after the fact.

## Functional requirements

- Declarative workload API: create / update / delete / scale, plus status.
- Two kinds of workload:
  - `service`: stateless, interchangeable replicas
  - `stateful`: stable ordinal identity (`db-0`, `db-1`, ...), one persistent volume
    each, created/replaced in order
- Per-replica cpu (millicores) and memory (MiB) requests, enforced on the node.
- Placement that respects capacity, node health, anti-affinity, zone spread, volume locality.
- Health checks, restart with exponential backoff.
- Detect machine failure and reschedule what was on it.
- Stable endpoint per workload, LB across healthy replicas.
- Horizontal autoscaling between min/max.
- Rolling updates.
- Metrics + structured logs everywhere.

## Non-functional

Production targets vs. what's realistic for this repo:

| | production | this repo |
|---|---|---|
| scale | 10k+ machines/region | one control plane, tens of nodes |
| control plane HA | replicated store, 99.95% | single process + snapshots |
| data plane keeps running if CP is down | yes | yes |
| node failure detection | < 30s | ~10s, configurable |
| isolation | hardware virtualization | process + cgroup v2 (pluggable) |

Assumptions:

- Tenant code is untrusted, so isolation is a security boundary, not just resource accounting.
- Machines fail routinely. Everything should be level-triggered (reconcile from observed
  state) rather than relying on seeing every event.
- Desired state is small (specs); actual state is large and noisy (heartbeats, status).
  Keep them as separate objects with separate writers.
- Artifacts: an OCI image / rootfs in production; a command line in the demo runtime.

Open question: do we need batch/cron-style workloads in v1? Leaving them out for now,
nothing in the object model should block adding them later.

## Scope for this repo

Build: API, versioned store with watch, scheduler, reconciler + lifecycle state machine,
node agent with pluggable runtime, process runtime with cgroup limits, health checks +
backoff, heartbeat-based node failure handling, stateful workloads with volumes that follow
the replica, autoscaler, L7 router, metrics/logs, CLI, tests, e2e demo, CI.

Design only: Firecracker runtime, overlay networking, network block storage, HA store
(Raft/etcd), multi-region, authn/z + quotas, image distribution, log pipeline.

Reasoning for the cuts is in [DESIGN.md](DESIGN.md#what-is-left-out-and-why).

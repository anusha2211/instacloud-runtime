# Instance lifecycle

The state machine enforced by `api.CanTransition` and driven by the
reconciler, scheduler, node monitor and agent. Labels say who makes each move.

```mermaid
stateDiagram-v2
    [*] --> Pending: reconciler creates instance
    Pending --> Scheduled: scheduler binds node (CAS)
    Scheduled --> Starting: agent starts runtime
    Starting --> Running: first health check passes
    Running --> Unhealthy: N consecutive probe failures
    Unhealthy --> Running: probe recovers
    Unhealthy --> Failed: agent kills it
    Running --> Failed: process exits / OOMKilled
    Starting --> Failed: start error / exits
    Failed --> Starting: backoff elapsed (1s, 2s, 4s ... 60s)

    Scheduled --> Lost: node heartbeat lease expired
    Starting --> Lost
    Running --> Lost
    Unhealthy --> Lost
    Failed --> Lost
    Lost --> Terminated: stateless - replaced immediately
    Lost --> Pending: stateful - after fence delay, same name + volume

    Pending --> Terminated: deleted before scheduling
    Scheduled --> Terminating: scale down / rollout / delete
    Starting --> Terminating
    Running --> Terminating
    Unhealthy --> Terminating
    Failed --> Terminating
    Terminating --> Terminated: agent: SIGTERM, grace, SIGKILL
    Terminated --> [*]: reconciler garbage-collects
```

## Workload-level lifecycle

```mermaid
flowchart TD
    A[icctl apply] --> B{exists?}
    B -- no --> C[create Workload gen=1]
    B -- yes --> D{spec changed<br/>besides replicas?}
    D -- yes --> E[generation++ -> rollout]
    D -- no --> F[replicas updated -> scale]
    C --> R[reconciler]
    E --> R
    F --> R
    R --> S{kind}
    S -- service --> SV[create missing replicas<br/>surge 1 during rollout<br/>retire old only while ready >= desired]
    S -- stateful --> ST[ensure volumes per ordinal<br/>create lowest missing ordinal<br/>only after lower ones ready<br/>scale down / roll out from highest]
```

## Node failure and recovery

```mermaid
sequenceDiagram
    autonumber
    participant Ag as Agent (node-2)
    participant NM as Node monitor
    participant St as Store
    participant RC as Reconciler
    participant SC as Scheduler
    participant Ag3 as Agent (node-3)

    Ag-xSt: heartbeats stop (crash / partition)
    Note over Ag: after its own lease expires,<br/>a partitioned agent stops<br/>stateful instances (self-fence)
    NM->>St: lease expired -> node NotReady
    NM->>St: instances on node-2 -> Lost
    RC->>St: stateless: Lost -> Terminated, create replacements
    RC->>St: stateful kv-1: wait fence delay, then Pending,<br/>release volume kv-data-1
    SC->>St: bind kv-1 -> node-3, volume bound to node-3
    Ag3->>St: poll assignments: kv-1 + volume
    Ag3->>Ag3: start kv-1 with same identity & data
    Ag3->>St: status Running, ready
```

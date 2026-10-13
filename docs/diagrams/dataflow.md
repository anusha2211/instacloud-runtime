# Data flow: from `apply` to serving traffic

```mermaid
sequenceDiagram
    autonumber
    actor U as Developer
    participant API as API server
    participant ST as State store
    participant RC as Reconciler
    participant SC as Scheduler
    participant AG as Node agent
    participant RT as Runtime
    participant RO as Router
    actor C as Client

    U->>API: PUT /v1/workloads/web (spec)
    API->>API: validate, default, generation
    API->>ST: create Workload (rev N)
    ST-->>RC: watch event
    RC->>ST: create Instances web-xxxx (Pending)
    ST-->>SC: watch event
    SC->>SC: filter (ready, fits, anti-affinity, volume)<br/>score (bin-pack, spread, balance)
    SC->>ST: CAS bind Instance.node = node-1 (Scheduled)
    AG->>API: GET /v1/nodes/node-1/assignments (poll)
    API-->>AG: [web-xxxx + spec + volumes]
    AG->>AG: allocate port, prepare volume dir
    AG->>RT: Start(id, cmd, env, cpu/mem limits)
    RT-->>AG: pid (cgroup cpu.max / memory.max set)
    AG->>API: status Starting, endpoint host:port
    loop every interval
        AG->>AG: HTTP GET /healthz
    end
    AG->>API: status Running, ready=true
    API->>ST: update Instance
    RO->>ST: refresh endpoints (Running && ready)
    C->>RO: GET /w/web/...
    RO->>AG: proxy to host:port (round-robin, retry on conn error)
    RO-->>RO: count request (RPS for autoscaler)
```

## Write ownership

Each field has exactly one writer, which is what makes the loops safe to run
concurrently without locks (conflicts only happen on genuine races and are
resolved by compare-and-swap + retry):

| Object / field | Writer |
|---|---|
| `Workload.spec` | API server (user), autoscaler (`replicas` only) |
| `Workload.status` | reconciler |
| `Instance` create / delete, `Terminating`, `Lost -> Pending` | reconciler |
| `Instance.node`, `Scheduled` | scheduler |
| `Instance.status` (Starting ... Terminated) | agent on the bound node (via API, transition-checked) |
| `Instance` -> `Lost`, `Node.status = NotReady` | node monitor |
| `Node.lastHeartbeat`, `Node` registration | agent (via API) |
| `Volume.boundNode` | scheduler (bind), reconciler (release after fencing) |

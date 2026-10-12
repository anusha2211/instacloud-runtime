# High-level architecture

```mermaid
flowchart LR
    subgraph Clients
        CLI[icctl / CI pipelines]
        Users[End-user traffic]
    end

    subgraph CP[Control plane]
        API[API server<br/>validation, admission,<br/>generation bump]
        Store[(State store<br/>versioned KV + watch)]
        Sched[Scheduler<br/>filter + score]
        Ctrl[Workload reconciler<br/>lifecycle state machine]
        NodeMon[Node monitor<br/>heartbeat leases]
        AS[Autoscaler]
        API <--> Store
        Sched <-->|watch / bind| Store
        Ctrl <-->|watch / write| Store
        NodeMon <--> Store
        AS -->|scale| Store
    end

    subgraph DP[Data plane]
        Router[L7 router<br/>endpoint discovery,<br/>LB + retries]
        subgraph N1[Node 1]
            A1[Node agent]
            RT1[Runtime<br/>microVM / process+cgroup]
            W1a[replica]
            W1b[replica]
            V1[(volume)]
        end
        subgraph N2[Node 2]
            A2[Node agent]
            RT2[Runtime]
            W2a[replica]
            V2[(volume)]
        end
    end

    CLI -->|HTTP/JSON| API
    Users --> Router
    Router -->|endpoints| Store
    Router --> W1a & W1b & W2a
    Router -.->|RPS samples| AS
    A1 -->|register, heartbeat,<br/>status| API
    A2 -->|register, heartbeat,<br/>status| API
    A1 -->|poll assignments| API
    A1 --> RT1 --> W1a & W1b
    W1b --- V1
    A2 --> RT2 --> W2a
    W2a --- V2
```

**Reading the diagram**

- Everything the control plane knows lives in the **state store**. Components never talk
  to each other directly; they read and write objects and react to watch events.
  This keeps each controller independently restartable and testable.
- **Desired state** (`Workload` specs) is written only by the API server and autoscaler.
  **Actual state** (`Node` heartbeats, `Instance` status) is written by agents.
  The reconciler and scheduler sit in between and close the gap.
- The **node agent** is the only thing that touches a machine. It pulls its assignments
  and pushes status - the control plane never opens connections into nodes, which
  keeps the trust boundary one-directional.
- The **router** is data plane: it caches endpoints and keeps serving with the last known
  set if the control plane is unavailable.

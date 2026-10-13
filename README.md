# instacloud-runtime

Compute runtime and orchestration layer for InstaCloud: schedules, runs, routes,
scales and heals stateless and stateful workloads across a fleet of machines.

> Work in progress. Start with [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md) for the
> problem breakdown and [docs/DESIGN.md](docs/DESIGN.md) for the architecture.

## Quick start

```bash
make test   # unit tests
make demo   # builds everything and runs the end-to-end demo locally
```

## Layout

```
cmd/         binaries (control plane, node agent, CLI)
internal/    control plane + agent packages
docs/        design doc and diagrams
examples/    sample workload specs
scripts/     demo / dev scripts
```

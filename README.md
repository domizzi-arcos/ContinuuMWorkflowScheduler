
# Distributed Workflow Scheduler

A discrete-event simulator for **tier-aware task scheduling across Edge, Fog, and Cloud** — written in Go, backed by Redis + RabbitMQ, with a live web dashboard.

The simulator is the artefact accompanying the master's thesis  
*"Distributed Workflow Scheduling Across Edge–Fog–Cloud Infrastructures"* (Pablo Cano Turnes, UC3M, 2026). It compares five scheduling policies — `round_robin`, `random`, `best_fit`, `worst_fit`, and a capacity-aware `topsis` variant — over a configurable topology of edge / fog / cloud nodes.

Based on [swarnabgarang/distributed-task-scheduler](https://github.com/swarnabgarang/distributed-task-scheduler).

---

## At a glance

| Layer | What it does | Where |
|---|---|---|
| **Scheduler core** | Polls Redis for pending executions, picks a server via the configured algorithm, atomically reserves resources, publishes to a tier queue | `internal/scheduler/` |
| **Worker simulators** | Three goroutines (edge / fog / cloud) that simulate execution by sleeping for `task.Duration + server.Latency` | `internal/worker/` |
| **Generator** | Instantiates workflow templates on a 2 s cadence; streams new exec IDs to the scheduler | `internal/generator/` |
| **State store** | Redis is the single source of truth for tasks, executions, servers, results | `internal/store/redis.go` |
| **Message bus** | RabbitMQ direct exchange → `edge_queue` / `fog_queue` / `cloud_queue`; fanout exchange → `result_queue` | `internal/broker/` |
| **Dashboard** | HTTP + WebSocket UI for live monitoring, pause/resume, and restart | `internal/web/` |

---

## Quick start

### Prerequisites

- Go 1.25 (only needed for local runs — the Docker image builds it for you)
- Docker + Docker Compose

### Run with Docker Compose

```sh
docker-compose up --build
```

Starts:
- Redis (port `6379`)
- RabbitMQ (port `5672`, management UI on `15672`)
- RedisInsight (port `8001`)
- The simulator (dashboard on `8080`)

The Compose file mounts `config.yaml` directly, so editing it and re-running `docker-compose up --build` takes effect on the next start.

Open the dashboard at **<http://localhost:8080>**.

To tear everything down (including volumes):

```sh
docker-compose down -v
```

### Run locally without Docker

```sh
# Redis + RabbitMQ must already be reachable per config.yaml
go run ./cmd
```

### Benchmark study (all algorithms back-to-back)

To compare every scheduling policy under an identical topology, run the
self-contained study in `benchmark/` — it brings up its own Redis and
RabbitMQ and writes one JSON file per algorithm plus seven comparison
PDFs:

```sh
cd benchmark
docker compose up --build benchmark
docker compose --profile plot run --rm plotter results/benchmark_<timestamp>
```

See [`benchmark/README.md`](benchmark/README.md) for details.

### Optional environment overrides

Copy `.env.example` to `.env` and edit the three knobs it exposes (generation window, progress log cadence, log directory). The Compose file picks it up automatically if present; otherwise the defaults baked into `config.yaml` win.

```sh
cp .env.example .env
```

### Run the tests

```sh
go test ./...
```

The five scheduling algorithms each have table-driven unit tests covering the empty/single-candidate paths and their selection invariants (packing, rotation, seed determinism, TOPSIS dominance, util-weight saturation fix). Seeder uniqueness rules are also covered.

---

## Configuring the topology

`config.yaml` is the single place to adjust how many servers you simulate, how powerful they are, and which scheduling algorithm runs. The blocks you'll edit most often:

| Block | What it controls |
|---|---|
| `servers.edge.count` / `fog.count` / `cloud.count` | **How many servers per tier.** The scheduler dispatches across them in proportion to their capacity. |
| `servers.{edge,fog,cloud}.cpu` / `memory` / `disk` | Per-tier CPU / memory / disk ranges. Each individual server gets a random draw from this range at startup. Use `{min, max, step}` to express a range or a bare scalar to fix a value. |
| `servers.{edge,fog,cloud}.wan_latency` | Simulated network latency added to every task running on a node of this tier (milliseconds). |
| `servers.{edge,fog,cloud}.cost_per_hour` | Hourly cost rate used to compute per-task cost. |
| `servers.{edge,fog,cloud}.power_watts` | Power-draw range used to compute per-task energy. |
| `servers.seed` | `0` = non-deterministic capacity randomisation, non-zero = reproducible. |
| `scheduler.algorithm` | One of `round_robin`, `random`, `best_fit`, `worst_fit`, `topsis`. |
| `scheduler.topsis_weights` | TOPSIS criterion weights: `time`, `cost`, `energy`, `util`. Setting `util: 0` reproduces the pre-saturation-fix behaviour. |
| `generator.task_count` | How many task templates are seeded at startup. |
| `generator.edge_max_duration_ms` / `fog_max_duration_ms` | Duration thresholds that assign each task's `TargetTier`. |
| `experiment.duration_seconds` | How long the generator runs before stopping (`0` = manual stop). The drain loop in `cmd/main.go` waits for all in-flight work to finish before exporting results. |

Example — to compare a small edge-heavy topology against a cloud-heavy one, edit:

```yaml
servers:
  edge:  { count: 8, ... }
  fog:   { count: 2, ... }
  cloud: { count: 1, ... }
```

…then `docker-compose up --build` and watch the dashboard.

---

## Architecture

```
                                +-----------+
                                | Dashboard |
                                |  (8080)   |
                                +-----+-----+
                                      |
                +---------------------+----------------------+
                |                     |                      |
       +--------v---------+   +-------v-------+    +---------v---------+
       | Generator (2 s)  |-->|  Scheduler    |--->|     Broker        |
       | seeds workflow   |   |  (500 ms tick)|    | task_exchange     |
       | instances        |   |  picks server |    |  /edge|fog|cloud  |
       +------------------+   +-------+-------+    +---------+---------+
                                      |                      |
                                      v                      v
                              +-------+-------+    +---------+---------+
                              |    Redis      |<---+    Workers       |
                              |  state +      |    |  (sleep duration |
                              |  reservations |    |   + latency)     |
                              +-------+-------+    +---------+---------+
                                      ^                      |
                                      |                      v
                              +-------+-------+    +---------+---------+
                              | Results       |<---+ result_exchange   |
                              | collector +   |    | (fanout)          |
                              | exporter      |    +-------------------+
                              +---------------+
```

- **Goroutines**: Generator, Scheduler, Workers (×3), Results collector, Web hub, optionally Workflow tracker. All coordinate via channels and a single `control.Controller` so the dashboard can pause/resume the whole pipeline coherently.
- **Concurrency safety**: every mutating Redis operation on `execution:*` and `server:*` uses WATCH/MULTI/EXEC with a 16-retry ceiling (see `internal/store/redis.go`).
- **RabbitMQ topology**: `task_exchange` (direct) routes by lowercase tier name; `result_exchange` (fanout) broadcasts to `result_queue`. QoS prefetch matches the worker concurrency limit.

---

## Scheduling algorithms

The scheduler implements five policies, all configured via `scheduler.algorithm`:

| Algorithm | Strategy |
|---|---|
| `round_robin` | Cycles through candidate servers in sorted ID order. |
| `random` | Selects a capable server uniformly at random (seeded for reproducibility). |
| `best_fit` | Packs tasks onto the server with the least remaining free capacity. |
| `worst_fit` | Spreads load by choosing the server with the most remaining free capacity. |
| `topsis` | Multi-criteria ranking (TOPSIS method) scoring each server on estimated execution time, monetary cost, energy consumption, and post-placement utilisation. Weights are configurable in `config.yaml`. |

`FreeCapacity` is computed as `(MaxCPU − UsedCPU) + (MaxMemory − UsedMemory)/100 + (MaxDisk − UsedDisk)`.

---

## Repository layout

```
cmd/
  main.go            entry point; ticker loop + signal handling
  session.go         simulationSession lifecycle (StopGeneration / StopAll)
  experiment.go      telemetry snapshots for progress + summary logging
  logging.go         slog file/console wiring
internal/
  broker/            RabbitMQ topology + publishers/consumers
  config/            YAML loader + validator
  control/           pause/resume controller polled by all hot loops
  generator/         workflow template instantiation + dataset replay
  models/            shared types (Task, Server, Execution, *Message, …)
  results/           result collector + JSON exporter (ExperimentResult schema)
  scheduler/         dispatch loop + five Algorithm implementations
  seeder/            synthetic server + task seeding
  store/             Redis store (key schema documented in package doc)
  web/               HTTP + WebSocket dashboard
  worker/            three-tier consumer goroutines
  workflow/          DAG templates + tracker
config.yaml          single configuration file — edit to tune the topology
docker-compose.yml   one-command setup of Redis + RabbitMQ + the simulator
Dockerfile           build for the app container
```

---

## Dashboard

Once the app is running, open <http://localhost:8080>. The dashboard exposes:

- task and execution lists
- server status and capacity
- per-tier load
- result history
- raw Redis key snapshot
- pause / resume controls
- restart control after a run ends

---

## Citing this work

If you use this code, please cite the thesis:

> Cano Turnes, Pablo. *Distributed Workflow Scheduling Across Edge–Fog–Cloud Infrastructures*. Master's thesis, Universidad Carlos III de Madrid, 2026.

---

## License

MIT — see [LICENSE](LICENSE).

# Distributed Workflow Scheduler

A Go-based simulation of a distributed task scheduling system across Edge, Fog, and Cloud resources.

## Overview

This repository implements a distributed workflow scheduler that:
- generates synthetic tasks and server nodes,
- schedules task executions to tiered servers using pluggable algorithms,
- simulates task execution with latency and resource consumption,
- uses Redis for state persistence and RabbitMQ for task/result messaging,
- exposes a dashboard and control API for runtime visibility.

The system is designed for experiments and evaluation of scheduling policies in a tiered infrastructure.

## Architecture

Key runtime components:

- `cmd/main.go` — application entry point and experiment orchestrator.
- `internal/config` — loads the YAML configuration file (`config.yaml`).
- `internal/generator` — generates task workloads and seeds them into Redis.
- `internal/scheduler` — dispatches task executions to servers according to the selected algorithm.
- `internal/worker` — consumes tasks from RabbitMQ queues and simulates execution.
- `internal/store` — Redis-based persistence for tasks, servers, executions, and results.
- `internal/broker` — RabbitMQ broker wrappers for task/result publish/consume.
- `internal/web` — HTTP API and dashboard endpoints for monitoring and control.
- `internal/control` — pause/resume and restart control for the running simulation.

## Dataflow

1. Application starts and loads `config.yaml`.
2. Redis server and RabbitMQ broker are connected.
3. Server nodes are generated, then tasks are generated and stored in Redis.
4. Scheduler fetches pending executions and selects a server.
5. Selected execution is assigned and published to a RabbitMQ tier queue.
6. Worker consumers read task messages and simulate execution.
7. Task results are published back through RabbitMQ and saved in Redis.
8. Web dashboard exposes current tasks, executions, servers, results, and simulation controls.

## Features

- Tier-aware scheduling across `EDGE`, `FOG`, and `CLOUD`.
- Multiple scheduling algorithms:
  - `round_robin`
  - `random`
  - `best_fit`
  - `worst_fit`
  - `topsis`
- Configurable server topology, task workload, and runtime behavior.
- Dashboard with pause/resume and restart controls.
- Redis state inspection.
- Docker Compose integration for Redis, RabbitMQ, and the app.

## Configuration

The main configuration file is `config.yaml`.

### Important sections

- `servers` — infrastructure topology and resource ranges for each tier.
- `scheduler` — scheduling algorithm and seed.
- `generator` — task generation count, seed, and tier duration thresholds.
- `redis` — Redis connection address and database index.
- `rabbitmq` — RabbitMQ connection URL.
- `web` — dashboard port.
- `experiment` — experiment duration in seconds (0 for manual stop) and progress log interval in seconds.

Example values are already provided in `config.yaml`.

## Running the application

### Using Docker Compose

From the repository root:

```sh
docker-compose up --build
```

This starts:
- `redis` on port `6379`
- `rabbitmq` on port `5672` (management UI on `15672`)
- the application on port `8080`

The app mounts `config.yaml` and reads `CONFIG_PATH=/app/config.yaml`.

### Running locally

Ensure you have:
- Go `1.25`
- Redis
- RabbitMQ

Run:

```sh
go run ./cmd
```

Set environment variables as needed:

- `CONFIG_PATH` — path to `config.yaml` (default: `config.yaml`)

## Code Layout

- `cmd/` — program entrypoint and experiment lifecycle control.
- `internal/broker/` — RabbitMQ publishing and consuming logic.
- `internal/config/` — YAML configuration loader.
- `internal/control/` — pause/resume control wrapper.
- `internal/generator/` — task creation and seeding logic.
- `internal/models/` — domain models for tasks, servers, executions, and results.
- `internal/results/` — result collector logic.
- `internal/scheduler/` — scheduling loop and algorithms.
- `internal/store/` — Redis storage operations.
- `internal/web/` — web server and HTTP handlers.
- `internal/worker/` — worker simulation consumers.
- `logs/` — local log output volume mount for Docker.

## Scheduling Algorithms

The scheduler implements five policies:

| Algorithm | Strategy |
|---|---|
| `round_robin` | Cycles through candidate servers in sorted ID order. |
| `random` | Selects a capable server uniformly at random (seeded for reproducibility). |
| `best_fit` | Packs tasks onto the server with the least remaining free capacity. |
| `worst_fit` | Spreads load by choosing the server with the most remaining free capacity. |
| `topsis` | Multi-criteria ranking (TOPSIS method) scoring each server on estimated execution time, monetary cost, and energy consumption. Weights are configurable. |

`FreeCapacity` is computed as `(MaxCPU − UsedCPU) + (MaxMemory − UsedMemory)/100 + (MaxDisk − UsedDisk)`.

The algorithm is configured in `config.yaml`.

## Dashboard

Once the app is running, open:

```sh
http://localhost:8080
```

The dashboard provides:
- task lists
- execution status
- server status and capacity
- result history
- Redis key snapshot
- pause/resume controls
- restart control after a run ends

## Testing

Unit tests cover all five scheduling algorithms in `internal/scheduler/`:

```sh
go test ./internal/scheduler/...
```

Each algorithm has a dedicated `*_test.go` file with table-driven tests for empty inputs, single-candidate passthrough, and algorithm-specific selection invariants (packing order, cycle rotation, seed determinism, TOPSIS dominance and weight bias).

The `cmd/` package contains integration-style tests for server seeding (`server_seed_test.go`).

To run the full suite:

```sh
go test ./...
```

## Development Notes

- The application uses Redis for persistence and RabbitMQ for task/result message delivery.
- Ephemeral Redis keys (`execution:*`, `result:*`, `workflow:*`) carry a 2-hour TTL; static definition keys (`task:*`, `server:*`) have no TTL.
- Tasks are generated with a target tier based on duration thresholds and available server tiers.
- Workers simulate real-world latency by adding server WAN latency to task duration.
- The app logs to stdout using Go's structured `log/slog` package. Log level defaults to `DEBUG`; each message carries structured fields (algorithm, server, execution ID, etc.).

## Dependencies

Declared in `go.mod`:
- `github.com/gorilla/mux`
- `github.com/gorilla/websocket`
- `github.com/rabbitmq/amqp091-go`
- `github.com/redis/go-redis/v9`
- `gopkg.in/yaml.v3`

## Notes

- `config.yaml` uses seeds to reproduce workloads and server capacities.
- `seed: 0` means random values on each startup.
- The dashboard remains available after a run finishes and allows restarting the simulation.

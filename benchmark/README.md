# Benchmark study

Self-contained grid runner that exercises every scheduling policy in
`internal/scheduler/` against an identical topology and exports a JSON
result file per algorithm, ready for the Python plotter.

The stack is independent from the root `docker-compose.yml` (its own
Redis and RabbitMQ), so it can run without touching the interactive
dashboard runner at the project root.

## Run the grid

```sh
cd benchmark
docker compose up --build benchmark
```

This runs the five algorithms sequentially — `round_robin`, `random`,
`best_fit`, `worst_fit`, `topsis` — and writes one
`<algorithm>_edge{N}_fog{N}_cloud{N}_<timestamp>.json` per run into
`results/benchmark_<timestamp>/`. The container exits once all five
runs and the post-run drain have finished.

Tune the grid via the `command:` line in `docker-compose.yml`:

| Flag        | Default | What it does                                             |
|-------------|---------|----------------------------------------------------------|
| `-duration` | `120`   | Seconds of task generation per algorithm                 |
| `-servers`  | `5`     | Number of servers per tier (EDGE, FOG, CLOUD)            |
| `-config`   | `/app/config.yaml` | Path to the shared config file                  |

Everything else — capacity ranges, TOPSIS weights, dataset path —
lives in `benchmark/config.yaml`.

## Generate the figures

```sh
docker compose --profile plot run --rm plotter results/benchmark_<timestamp>
```

The plotter scans the given directory (or `.json` files) and writes
seven PDFs into `figures/`:

| File                              | What it shows                                  |
|-----------------------------------|------------------------------------------------|
| `01_makespan_cdf.pdf`             | CDF of workflow makespans per algorithm        |
| `02_makespan_per_template.pdf`    | Boxplots per workflow template                 |
| `03_cost_energy.pdf`              | Total cost and energy bars per algorithm       |
| `04_throughput_timeline.pdf`      | Tasks/min over the run                         |
| `05_tier_distribution.pdf`        | Stacked task counts per tier (EDGE/FOG/CLOUD)  |
| `06_multiobjective.pdf`           | Cost vs. makespan scatter (bubble ~ energy)    |
| `07_latency_percentiles.pdf`      | P50 and P95 makespan bars                      |

## Tear down

```sh
docker compose down -v
```

This removes the benchmark's Redis and RabbitMQ volumes — the JSON
results and PDF figures on the host stay in `results/` and `figures/`.

## Layout

```
benchmark/
├── README.md
├── main.go                 # grid runner — imports from ../internal/
├── Dockerfile              # Go build, context = project root
├── docker-compose.yml      # redis + rabbitmq + benchmark + plotter (profile)
├── config.yaml             # shared topology + generator config
├── analysis/
│   ├── plot.py
│   ├── requirements.txt
│   └── Dockerfile          # python:3.12-slim
├── results/                # JSON output (gitignored)
└── figures/                # PDF output (gitignored)
```

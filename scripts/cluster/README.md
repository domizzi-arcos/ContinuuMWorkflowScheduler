# Running the campaign on a cluster with Apptainer

No Docker, no root and no daemons on the compute nodes. Each Slurm array task
runs **one combo** (all 10 policies) with its **own** Redis and RabbitMQ,
started from Apptainer images on private ports, so tasks can share a node.
The 120 C3 combos then run in parallel instead of in sequence.

| File | Purpose |
|---|---|
| `services.sh` | start/stop Redis + RabbitMQ from `.sif` images on 4 ports from `PORT_BASE` |
| `run_combo.sh` | one combo: services → per-job base config → `grid_benchmark.py` → stop |
| `c3_array.sbatch` | Slurm array 0–119 → (topology, capacity, seed) → `run_combo.sh` |
| `python.def` | Python 3.12 image with pyyaml, numpy, pandas, scipy, matplotlib, pytest |
| `compare_runs.py` | check two platforms agree within noise (tasks ±1, cost ±5%) |

## 1. Prepare (once)

On a machine with Go and Apptainer (a workstation is fine):

```sh
# Static campaign runner: no Go needed on the cluster. Build from a clean,
# committed tree so meta.provenance records the commit.
CGO_ENABLED=0 go build -o bin/benchmark ./cmd/benchmark

# Images (pull works without root; building python.sif needs --fakeroot,
# so build it here if the cluster does not allow it).
mkdir -p ~/sif && cd ~/sif
apptainer pull redis.sif    docker://redis:7-alpine
apptainer pull rabbitmq.sif docker://rabbitmq:3-alpine
apptainer build --fakeroot python.sif <repo>/scripts/cluster/python.def
```

Copy to the cluster: the repository (with `bin/benchmark`), `~/sif/*.sif`,
and `globus dataset/GlobusComputeDataset-v1-enriched.csv` (262 MB, not in
git). Check the dataset there:

```sh
sha256sum "globus dataset/GlobusComputeDataset-v1-enriched.csv"
# c5f02b084810f9a21598ca6fc280a9d5fa238df3920ead9e90bd72a56a160b2b
```

Python without the image: any Python ≥ 3.10 with `pyyaml` works for the
runs (`export PYTHON=python3`); the analysis also needs numpy, pandas,
scipy, matplotlib.

## 2. Pilot on the cluster (required before the full array)

WP6 allows parallel runs only after a pilot shows they match a serial run
within noise. Run the C1 combo as one task and compare with the local C1:

```sh
mkdir -p logs
sbatch --array=$(( 1*15 + 1*5 + 0 )) scripts/cluster/c3_array.sbatch  # uniform_m, medium, seed 1 → index 20
python3 scripts/cluster/compare_runs.py results/grid_20261012_112405_c1 results/c3_cluster/task_*
```

(Index = topology_index × 15 + capacity_index × 5 + seed_index with the
grid's order: topologies `uniform_s, uniform_m, uniform_l, edge_heavy,
edge_med, fog_heavy, cloud_heavy, cloud_med`, capacities `small, medium,
large`, seeds 1–5. uniform_m/medium/seed 1 is index 20.) Copy the local C1
grid directory to the cluster first, or the cluster task back.

## 3. Full campaign

```sh
mkdir -p logs && sbatch scripts/cluster/c3_array.sbatch
squeue -u $USER                                   # progress
ls results/c3_cluster/task_*/manifest.json | wc -l  # finished combos (of 120)
```

A finished task is skipped on resubmission (it has a `manifest.json`), so
failed or timed-out tasks are rerun with the same `sbatch` (or
`--array=<ids>`). Each task needs ~11–16 min (36 min worst case); the
default limit is 1 h, at most 30 tasks at once (`%30`).

Then, with the analysis environment:

```sh
make analysis GRID_DIRS="$(ls -d results/c3_cluster/task_*)"
```

## Notes

- **Timing noise.** The emulator sleeps in wall-clock time, so a heavily
  loaded node can stretch runs. Every policy of a combo runs in the same
  task on the same node, so within-configuration comparisons stay paired;
  `meta.provenance.host_hash` records the node. Do not mix these results
  with a grid from another platform unless `compare_runs.py` passes.
- **Ports.** `PORT_BASE` defaults to `20000 + 10 × (array id % 1000)`;
  if a port is taken, `run_combo.sh` retries on another base. Services
  bind to 127.0.0.1 only.
- **Overrides.** `ALGORITHMS`, `WORKLOAD_FLAGS` (default: the G2 options),
  `EXTRA_FLAGS` (e.g. `--drain 180`), `SEEDS`, `OUT_ROOT`, `SIF_DIR`,
  `PYTHON`, `BINARY` are read from the environment (`sbatch --export=ALL,...`).
- **Proxies.** Not needed: every connection is plain TCP to 127.0.0.1 and
  nothing uses HTTP. If your site requires it anyway, `export
  no_proxy=localhost,127.0.0.1,...` before `sbatch` (Slurm passes the
  environment to the job by default).
- **Services fail to start.** The error prints the last lines of the
  Redis/RabbitMQ log. To inspect everything, rerun one task with
  `KEEP_STATE=1` (the state directory is then kept and its path printed),
  and raise `RABBITMQ_START_TIMEOUT` (default 300 s) if the images live on a
  slow shared filesystem. Set `TMPDIR` to node-local disk if it points at
  Lustre or NFS.

## Post-C3 experiments (pareto, load, speed) and the SLO metric

| File | Purpose |
|---|---|
| `experiments.py` | catalogue: experiment → levels → (grid flags, time limit); array index → job |
| `exp_array.sbatch` | one array task = one (level, topology, capacity, seed) via `run_combo.sh` |
| `submit.sh` | sets the array range and time limit from the catalogue and submits |

```sh
scripts/cluster/submit.sh list
```

| Experiment | Levels | What changes (grid flags) | Jobs × runs |
|---|---|---|---|
| `pareto` | `sweep` | `--weight-sweep --skip-main-grid`: 11 weight vectors (`WEIGHT_SWEEP`: u0, u10, u25, u50, time80, time80u25, cost80, energy80, t60c40, t40c60, t20c80) × {topsis, weighted_sum}; `sweep_tier` adds `--speed tier --drain 300` | 2 × 120 × 22 (the first `sweep` round ran 16, before the t/c splits) |
| `load` | `iv1000`, `iv500`, `iv250`, `win120` | workflow interval 1 s / 0.5 s / 0.25 s (C3 is 2 s), or 2 s over a 120 s window; drain 300–600 s | 4 × 120 × 10 |
| `speed` | `tier`, `cores` | `--speed tier` (edge 0.5×, fog 1×, cloud 2×) or `--speed cores` (speed = (cpu/4)^0.5) | 2 × 120 × 10 |

Results land in `results/<experiment>/<level>/task_<combo>/` (combo IDs gain
`_w_<weights>`, `_sp_<speed>` or `_iv<ms>`; the analysis reads these and
`meta.labels`). Every new option defaults to off: a config without them is
byte-identical to C3's and the Go golden tests are unchanged, so C3 remains
the 2 s / speed-1 / default-weights level of each experiment.

**1. Rebuild the binary** (the speed, interval and critical-path code is new)
from a committed tree and copy it to the cluster:

```sh
CGO_ENABLED=0 go build -o bin/benchmark ./cmd/benchmark
```

**2. Pilot one job per experiment** (index 0 = uniform_s / small / seed 1 of
the first level) and check it drained and completed every workflow before
submitting the arrays:

```sh
ARRAY=0 scripts/cluster/submit.sh pareto
ARRAY=0 scripts/cluster/submit.sh load iv250      # heaviest load level
ARRAY=0 scripts/cluster/submit.sh speed tier
grep -h "drain finished" logs/wfsched-*_*_0.out   # drained=true expected
```

Use the pilot's wall time to adjust the time limits in `experiments.py`
(C3: 6–18 min per 10-policy job; the pareto job runs 16 policies, the load
levels carry 2–8× the workflows). A run that has not drained by its drain
deadline is still exported, with `end_state.drained = false` and fewer
completed workflows; at 0.25 s this is a result, not a failure.

**3. Submit** (finished jobs are skipped on resubmission):

```sh
scripts/cluster/submit.sh pareto
scripts/cluster/submit.sh load              # or one level: submit.sh load iv500
scripts/cluster/submit.sh speed
SEEDS=1,2,3 scripts/cluster/submit.sh load iv250   # fewer seeds if time is short
```

**4. Analyse** (from the repository root, with the analysis environment):

```sh
# Pareto: sweep + C3 (C3 supplies the weight-independent policies).
python3 scripts/analysis/pareto.py results/pareto/sweep/task_* results/c3_cluster/task_* \
  --out analysis_out/pareto
#   -> pareto_points.csv, pareto_per_config.csv, replication.csv
#      (topsis[u25] must match C3 topsis within noise), pareto.pdf

# Pareto under tier speed: speed/tier supplies the weight-independent policies.
python3 scripts/analysis/pareto.py results/pareto/sweep_tier/task_* results/speed/tier/task_* \
  --out analysis_out/pareto_tier

# Load and speed: the usual pipeline, one level at a time or together with C3.
make analysis GRID_DIRS="$(ls -d results/load/iv500/task_*)" ANALYSIS_OUT=analysis_out/load_iv500
make analysis GRID_DIRS="$(ls -d results/speed/tier/task_*)" ANALYSIS_OUT=analysis_out/speed_tier

# SLO / deadline metric: share of workflows done within k × critical path
# (+ optional fixed slack). Works on any grid, including C3; needs Go.
python3 scripts/analysis/slo.py results/c3_cluster/task_* results/load/*/task_* \
  --k 1.5,2,3,5 --slack-ms 2000 --out analysis_out/slo_load
```

The critical path of a workflow is its longest DAG path with each task at its
fastest feasible effective time (duration / speed + latency) on an idle
system, recomputed by `cmd/audit` from each grid's `_configs/`. In C3, 42% of
workflows have a critical path under 1 s while dispatch costs about 450 ms per
DAG level, so a pure k × CP deadline is unreachable for them under any
policy; `--slack-ms` (e.g. 2000) adds a fixed allowance.

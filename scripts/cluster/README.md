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

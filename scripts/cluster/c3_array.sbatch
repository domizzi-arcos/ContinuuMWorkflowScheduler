#!/bin/bash
# C3 on a Slurm cluster with Apptainer: one array task per combo
# (8 topologies × 3 capacities × 5 seeds = 120), each with its own Redis and
# RabbitMQ on private ports. Submit from the repository root:
#
#   mkdir -p logs && sbatch scripts/cluster/c3_array.sbatch
#
# Then analyse:  make analysis GRID_DIRS="$(ls -d results/c3_cluster/task_*)"
# Adjust partition/account and the limits below for your cluster.
#SBATCH --job-name=wfsched-c3
#SBATCH --array=0-119%30
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --output=logs/c3_%A_%a.out

set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-.}"
module load apptainer 2>/dev/null || module load singularity 2>/dev/null || true

export SIF_DIR=${SIF_DIR:-$HOME/sif}
export PYTHON=${PYTHON:-apptainer exec --cleanenv --bind $PWD $SIF_DIR/python.sif python3}
SEEDS=${SEEDS:-1,2,3,4,5}
OUT_ROOT=${OUT_ROOT:-results/c3_cluster}

# Index -> (topology, capacity, seed) in grid order (topology, capacity, seed),
# using the grid script's own lists so they cannot drift.
read -r TOPOLOGY CAPACITY SEED < <($PYTHON - "$SLURM_ARRAY_TASK_ID" "$SEEDS" <<'PY'
import sys
sys.path.insert(0, "scripts")
import grid_benchmark as g
idx, seeds = int(sys.argv[1]), [int(s) for s in sys.argv[2].split(",")]
combos = [(t["label"], c, s) for t in g.TOPOLOGIES for c in g.CAPACITY_PROFILES for s in seeds]
if idx >= len(combos):
    sys.exit(f"array index {idx} >= {len(combos)} combos")
print(*combos[idx])
PY
)
echo "task $SLURM_ARRAY_TASK_ID on $(hostname -s): $TOPOLOGY $CAPACITY seed $SEED"
scripts/cluster/run_combo.sh "$TOPOLOGY" "$CAPACITY" "$SEED" "$OUT_ROOT"

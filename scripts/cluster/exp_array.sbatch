#!/bin/bash
# Post-C3 experiments (pareto, load, speed) on Slurm with Apptainer: one array
# task per job of scripts/cluster/experiments.py, each with its own Redis and
# RabbitMQ. Submit with scripts/cluster/submit.sh, which sets the array range
# and time limit:
#
#   scripts/cluster/submit.sh pareto
#   scripts/cluster/submit.sh load iv1000,iv500
#
# Environment: EXPERIMENT (required), LEVELS (optional subset), SEEDS, plus
# everything run_combo.sh reads (SIF_DIR, PYTHON, BINARY, EXTRA_FLAGS, ...).
#SBATCH --job-name=wfsched-exp
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --output=logs/%x_%A_%a.out


set -euo pipefail
cd "${SLURM_SUBMIT_DIR:-.}"
module load apptainer 2>/dev/null || module load singularity 2>/dev/null || true

export SIF_DIR=${SIF_DIR:-$HOME/sif}
export PYTHON=${PYTHON:-apptainer exec --cleanenv --bind $PWD $SIF_DIR/python.sif python3}
: "${EXPERIMENT:?set EXPERIMENT (pareto, load or speed); use scripts/cluster/submit.sh}"

# Sets TOPOLOGY CAPACITY SEED OUT_ROOT LEVEL_FLAGS LEVEL_ALGORITHMS.
eval "$($PYTHON scripts/cluster/experiments.py job "$EXPERIMENT" "$SLURM_ARRAY_TASK_ID" "${LEVELS:-}")"
echo "task $SLURM_ARRAY_TASK_ID on $(hostname -s): $EXPERIMENT -> $OUT_ROOT $TOPOLOGY $CAPACITY seed $SEED"

export EXTRA_FLAGS="$LEVEL_FLAGS ${EXTRA_FLAGS:-}"
[ -n "$LEVEL_ALGORITHMS" ] && export ALGORITHMS="$LEVEL_ALGORITHMS"
scripts/cluster/run_combo.sh "$TOPOLOGY" "$CAPACITY" "$SEED" "$OUT_ROOT"

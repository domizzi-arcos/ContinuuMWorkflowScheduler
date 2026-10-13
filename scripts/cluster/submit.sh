#!/usr/bin/env bash
# Submit one post-C3 experiment as a Slurm array (see experiments.py).
#
#   scripts/cluster/submit.sh EXPERIMENT [LEVELS] [extra sbatch args...]
#
#   scripts/cluster/submit.sh list                    # show experiments and job counts
#   scripts/cluster/submit.sh pareto
#   scripts/cluster/submit.sh load iv1000,iv500 --partition=long
#   SEEDS=1,2,3 scripts/cluster/submit.sh speed tier
#   ARRAY=0 scripts/cluster/submit.sh speed tier       # pilot: one job only
#
# Environment: MAX_PARALLEL (default 30), ARRAY (override the index range),
# SEEDS, plus anything exp_array.sbatch / run_combo.sh read.
set -euo pipefail
cd "$(dirname "$0")/../.."

SIF_DIR=${SIF_DIR:-$HOME/sif}
PYTHON=${PYTHON:-apptainer exec --cleanenv --bind $PWD $SIF_DIR/python.sif python3}
EXP=${1:?usage: submit.sh EXPERIMENT|list [LEVELS] [sbatch args...]}
if [ "$EXP" = list ]; then
  exec $PYTHON scripts/cluster/experiments.py list
fi
LEVELS=""
if [ $# -ge 2 ] && [ "${2#-}" = "$2" ]; then LEVELS=$2; shift; fi
shift

read -r N LIMIT < <($PYTHON scripts/cluster/experiments.py count "$EXP" "$LEVELS")
ARRAY=${ARRAY:-0-$((N - 1))%${MAX_PARALLEL:-30}}
if [ ! -x bin/benchmark ]; then
  echo "error: bin/benchmark missing; build it from a committed tree (see scripts/cluster/README.md)" >&2
  exit 1
fi
mkdir -p logs
echo "submitting $EXP${LEVELS:+ ($LEVELS)}: $N jobs, array $ARRAY, time limit $LIMIT"
sbatch --job-name="wfsched-$EXP" --array="$ARRAY" --time="$LIMIT" \
  --export=ALL,EXPERIMENT="$EXP",LEVELS="$LEVELS",SEEDS="${SEEDS:-1,2,3,4,5}" \
  "$@" scripts/cluster/exp_array.sbatch

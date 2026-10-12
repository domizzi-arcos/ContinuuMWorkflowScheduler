#!/usr/bin/env bash
# Run one grid combo (all selected policies) on a private Redis + RabbitMQ.
#
#   scripts/cluster/run_combo.sh TOPOLOGY CAPACITY SEED OUT_ROOT
#
# Writes OUT_ROOT/task_<combo>/ in the normal grid layout (configs, results,
# manifest), so `make analysis GRID_DIRS="$(ls -d OUT_ROOT/task_*)"` works.
#
# Environment (defaults in brackets):
#   PORT_BASE      first of 4 consecutive free ports [20000 + 10 × (array id % 1000)]
#   SIF_DIR        directory with redis.sif, rabbitmq.sif, python.sif [$HOME/sif]
#   PYTHON         command running Python ≥ 3.10 with pyyaml
#                  [apptainer exec --cleanenv $SIF_DIR/python.sif python3]
#   BINARY         prebuilt static campaign runner [bin/benchmark]
#   ALGORITHMS     comma-separated subset (default: all, as in the grid script)
#   WORKLOAD_FLAGS [--demand-scale --drop-unschedulable --max-duration-ms 30000]
#   EXTRA_FLAGS    any further grid_benchmark.py flags (e.g. --drain 180)
#   RABBITMQ_START_TIMEOUT  seconds to wait for RabbitMQ [300]
#   KEEP_STATE=1   keep the services' state and logs after the run (debugging)
set -euo pipefail

TOPOLOGY=${1:?topology} CAPACITY=${2:?capacity} SEED=${3:?seed} OUT_ROOT=${4:?out root}
REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO"

SIF_DIR=${SIF_DIR:-$HOME/sif}
PYTHON=${PYTHON:-apptainer exec --cleanenv --bind $REPO $SIF_DIR/python.sif python3}
BINARY=${BINARY:-bin/benchmark}
WORKLOAD_FLAGS=${WORKLOAD_FLAGS:---demand-scale --drop-unschedulable --max-duration-ms 30000}
COMBO="topo_${TOPOLOGY}_cap_${CAPACITY}_seed${SEED}"
GRID="$OUT_ROOT/task_$COMBO"

if [ ! -x "$BINARY" ]; then
  echo "error: $BINARY not found; build it with: CGO_ENABLED=0 go build -o bin/benchmark ./cmd/benchmark" >&2
  exit 1
fi
if [ -f "$GRID/manifest.json" ]; then
  echo "$COMBO already finished ($GRID/manifest.json exists); skipping"
  exit 0
fi

STATE=$(mktemp -d "${TMPDIR:-/tmp}/wfsched_svc.XXXXXX")
source "$REPO/scripts/cluster/services.sh"
trap 'services_stop "$STATE"; if [ "${KEEP_STATE:-0}" = 1 ]; then echo "services state kept in $STATE"; else rm -rf "$STATE"; fi' EXIT

# Find 4 free consecutive ports (other jobs may share the node).
base=${PORT_BASE:-$((20000 + 10 * (${SLURM_ARRAY_TASK_ID:-0} % 1000)))}
for attempt in 1 2 3 4 5; do
  if services_start "$base" "$STATE"; then break; fi
  services_stop "$STATE"
  [ "$attempt" = 5 ] && { echo "error: could not start services" >&2; exit 1; }
  base=$((20000 + (base - 20000 + 3000 + 10 * (RANDOM % 300)) % 40000))  # stays in 20000-59999
done

# Per-job base config: the campaign base with this job's endpoints.
mkdir -p "$GRID"
CONFIG="$GRID/base_config.yaml"
$PYTHON - "$REPO/configs/campaign_base.yaml" "$CONFIG" "$REDIS_ADDR" "$AMQP_URL" <<'PY'
import sys, yaml
src, dst, redis, amqp = sys.argv[1:]
cfg = yaml.safe_load(open(src))
cfg["redis"]["address"] = redis
cfg["rabbitmq"]["url"] = amqp
yaml.safe_dump(cfg, open(dst, "w"), default_flow_style=False)
PY

ALGO_FLAG=()
[ -n "${ALGORITHMS:-}" ] && ALGO_FLAG=(--algorithms "$ALGORITHMS")
# shellcheck disable=SC2086
$PYTHON -u scripts/grid_benchmark.py --config "$CONFIG" --binary "$BINARY" \
  --output-dir "$GRID" --seeds "$SEED" --topologies "$TOPOLOGY" --capacities "$CAPACITY" \
  --skip-topsis-weights $WORKLOAD_FLAGS ${EXTRA_FLAGS:-} "${ALGO_FLAG[@]}"

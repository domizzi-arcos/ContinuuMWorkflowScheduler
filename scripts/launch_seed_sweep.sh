#!/usr/bin/env bash
# =============================================================================
# launch_seed_sweep.sh — Overnight 3-seed main-grid sweep for the thesis defense.
# =============================================================================
#
# WHAT IT LAUNCHES
#   1. caffeinate -dis      (macOS: prevents sleep/display sleep while bench runs)
#   2. grid_benchmark.py    (24 combos × 3 seeds = 72 runs total)
#   3. auto_plot.py         (watches the run, fires grid_plot + report on 72/72)
#
# WHY THIS WRAPPER EXISTS
#   The three processes above need to be detached from this shell (nohup +
#   background) so the laptop lid can be closed and SSH sessions can drop
#   without killing the overnight run. caffeinate in particular needs an
#   explicit `disown` because earlier versions of this script had an EXIT
#   trap that killed it on shell exit — leaving the bench to sleep midway.
#
# Runs the 24 main-grid combos (8 topologies × 3 capacities) three times with
# seeds 1, 2, 3. Skips the 72 TOPSIS weight-sensitivity runs — they already
# have well-separated boxes in §6 of the report.
#
# Estimated wall time: ~9h at 30s/algo (the original grid took 11.3h with the
# weight-sensitivity branch enabled; this run drops that branch so it scales
# linearly with seed count).
#
# Prereqs: Docker Desktop running with redis + rabbitmq containers up.
# Run from distributed-workflow-scheduler/.

set -euo pipefail

# Sanity: bail out early if Docker daemon is down (errors are easier to read
# now than 6 hours into an overnight run that quietly failed).
docker info > /dev/null 2>&1 || { echo "Docker is not running. Start Docker Desktop first."; exit 1; }
# If Redis isn't responding, bring the stack up via compose and pause 3s for
# the containers to become reachable before launching the benchmark.
redis-cli -h localhost ping > /dev/null 2>&1 || {
    echo "Redis not reachable on localhost:6379. Running: docker-compose up -d redis rabbitmq";
    docker-compose up -d redis rabbitmq
    sleep 3
}

STAMP="$(date -u +%Y%m%d_%H%M%S)"
GRID_DIR="results/grid_seedsweep_${STAMP}"
mkdir -p "$GRID_DIR"

# Keep the laptop awake for the full run. Use nohup + disown so caffeinate
# survives this wrapper script exiting (otherwise the EXIT trap killed it).
# -dis = prevent display sleep, idle sleep, and system sleep.
nohup caffeinate -dis > /tmp/seedsweep_caffeinate.log 2>&1 &
CAFF=$!
# disown removes the job from the shell's job table so it's not killed when
# this wrapper exits or the terminal closes.
disown $CAFF
echo "caffeinate PID=$CAFF (detached — kill manually with: kill $CAFF)"

# nohup + & detaches grid_benchmark from the terminal so an SSH disconnect
# (or `exit` from the launching shell) doesn't terminate the overnight job.
# Logs are redirected into the grid dir so they end up alongside the results.
nohup python3 scripts/grid_benchmark.py \
    --skip-topsis-weights \
    --seeds 1,2,3 \
    --duration 30 \
    --output-dir "$GRID_DIR" \
    > "$GRID_DIR/bench.log" 2>&1 &
BENCH=$!
echo "grid_benchmark PID=$BENCH"
echo "Output dir: $GRID_DIR"

# Arm the auto-plot watcher: fires grid_plot.py and generate_report.py when
# manifest hits 72/72. (24 baselines × 3 seeds, with --skip-topsis-weights.)
nohup python3 scripts/auto_plot.py "$GRID_DIR" --total 72 > "$GRID_DIR/auto_plot.log" 2>&1 &
WATCH=$!
echo "auto_plot watcher PID=$WATCH"

echo ""
# `tail -f` follows the bench log live — typical post-launch verification step
# to confirm the orchestrator is making progress before walking away.
echo "Tail progress:  tail -f $GRID_DIR/bench.log"
echo "Stop the run:   kill $BENCH $WATCH; kill $CAFF"
echo "Resume later:   python3 scripts/grid_benchmark.py --resume $GRID_DIR --seeds 1,2,3 --skip-topsis-weights"

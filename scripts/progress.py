#!/usr/bin/env python3
"""progress.py - One-shot CLI progress bar for a running grid benchmark.

Tails the most recent ``results/grid_*`` directory and re-renders a terminal
progress bar every 15s. Shows:

  * completed combos / TOTAL with a unicode block bar
  * any failed combos by id
  * the combo currently running (detected via ``ps aux``)
  * an ETA computed from the average elapsed-per-completed-combo so far

Usage:
    python scripts/progress.py

Ctrl-C exits cleanly.
"""
import json, os, sys, time, glob

# Hardcoded for the full 96-combo grid. The seed-sweep variants use the
# dashboard instead so this constant doesn't need to be parameterized.
TOTAL = 96
# Pick the newest grid run by lexicographic sort over the timestamped dir
# names. Falls back to "results/grid" so subsequent file checks just no-op.
GRID  = sorted(glob.glob("results/grid_*"))[-1] if glob.glob("results/grid_*") else "results/grid"

def bar(done, total, width=40):
    """Render a unicode progress bar string.

    Args:
        done: Completed unit count.
        total: Total unit count.
        width: Character width of the bar body.

    Returns:
        Formatted bar like ``[████░░░░] 12/96 (12%)``.
    """
    filled = int(width * done / total) if total else 0
    pct    = 100 * done / total if total else 0
    return f"[{'█' * filled}{'░' * (width - filled)}] {done}/{total} ({pct:.0f}%)"

def read_progress():
    """Read ``<GRID>/progress.json`` and split runs by status.

    Returns:
        ``(done_runs, failed_runs)`` — lists of run dicts. Returns
        ``([], [])`` if the file is missing or unreadable (orchestrator may
        not have written its first checkpoint yet).
    """
    path = os.path.join(GRID, "progress.json")
    if not os.path.exists(path):
        return [], []
    try:
        with open(path) as f:
            runs = json.load(f).get("runs", [])
        done   = [r for r in runs if r["status"] == "done"]
        failed = [r for r in runs if r["status"] == "failed"]
        return done, failed
    except Exception:
        return [], []

def current_combo():
    """Detect the currently running combo by inspecting ``ps aux``.

    Looks for the Go ``_benchmark_bin`` process and extracts the
    ``-config path/to/topo_*.yaml`` argument to recover the combo id.

    Returns:
        The combo id (e.g. ``topo_uniform_s_cap_small``) or ``None`` when
        no benchmark binary is currently running.
    """
    try:
        import subprocess
        # `ps aux` is portable enough across macOS/Linux for this dev tool;
        # stderr is discarded to keep the bar redraw clean.
        out = subprocess.check_output(
            ["ps", "aux"], text=True, stderr=subprocess.DEVNULL
        )
        for line in out.splitlines():
            if "_benchmark_bin" in line and "-config" in line:
                # Walk tokens until we find the topo_*.yaml config argument.
                for part in line.split():
                    if "topo_" in part and part.endswith(".yaml"):
                        return os.path.basename(part).replace(".yaml", "")
    except Exception:
        pass
    return None

def eta(done_count, elapsed_s):
    """Estimate remaining wall time using average per-combo runtime.

    Args:
        done_count: Number of completed combos so far.
        elapsed_s: Total seconds elapsed since the watcher started.

    Returns:
        ``"Hh Mm"`` formatted ETA, or ``"calculating..."`` until at least
        one combo has finished (so per-combo time is defined).
    """
    if done_count == 0:
        return "calculating..."
    # Naive linear extrapolation: per_combo = elapsed / done, then multiply
    # by remaining count. Accurate enough once a handful of combos have run.
    per_combo = elapsed_s / done_count
    remaining = (TOTAL - done_count) * per_combo
    h, m = int(remaining // 3600), int((remaining % 3600) // 60)
    return f"{h}h {m}m"

# Wall-clock anchor for ETA math — the script's own start time, not the
# benchmark's start (an acceptable approximation for a live watcher).
start = time.time()
print(f"Watching {GRID}\n")

try:
    while True:
        done, failed = read_progress()
        running = current_combo()
        elapsed = time.time() - start

        # `clear` resets the terminal each tick so the bar updates in place
        # instead of scrolling. 15s redraw cadence keeps CPU negligible.
        os.system("clear")
        print(f"Grid Benchmark Progress\n")
        print(f"  {bar(len(done), TOTAL)}")
        if failed:
            print(f"  ⚠  {len(failed)} failed: {', '.join(r['combo_id'] for r in failed)}")
        print()
        if running:
            print(f"  Running : {running}")
        print(f"  ETA     : {eta(len(done), elapsed)}")
        print(f"  Elapsed : {int(elapsed//3600)}h {int((elapsed%3600)//60)}m")
        print(f"\n  (Ctrl-C to exit)")

        time.sleep(15)
except KeyboardInterrupt:
    print("\nDone.")

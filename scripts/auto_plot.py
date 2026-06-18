#!/usr/bin/env python3
"""
auto_plot.py - Watch the grid and fire grid_plot.py when all combos finish.

Behavior:
  1. Polls grid_progress.json every 30s.
  2. When every entry in grid_progress.json has status == "done" AND the total
     count matches the expected combo count, triggers grid_plot.py.
  3. Logs everything to <grid_dir>/auto_plot.log.
  4. Writes <grid_dir>/AUTO_PLOT_STATUS.json with state for the dashboard.
  5. Posts a macOS Notification Center alert on completion.
  6. Idempotent: if AUTO_PLOT_STATUS shows status=='done', exits immediately.

Usage:
    python scripts/auto_plot.py <grid_dir> [--total N]

    --total N   Expected combo count. Defaults to 96 (the legacy full-grid size).
                Pass 72 for the seed-sweep (24 baselines × 3 seeds, skip-weights).
                Pass 24 for a single-seed main-grid-only run.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Interval between grid_progress.json polls. 30s is conservative: combos run
# for minutes, so missing the exact completion by a few seconds doesn't matter.
POLL_SECONDS = 30
# Legacy full-grid size (8 topologies × 3 capacities × 4 seeds). Overridden
# by --total on the command line for the seed-sweep variants.
DEFAULT_TOTAL_COMBOS = 96


def write_status(grid_dir: Path, **kv) -> None:
    """Atomically write the watcher status file.

    The dashboard reads ``AUTO_PLOT_STATUS.json`` constantly; writing
    in-place would race and let it observe a truncated/empty file. The
    write-then-rename pattern leverages POSIX rename atomicity so a reader
    always sees either the previous full payload or the new full payload.

    Args:
        grid_dir: Directory the status file lives in.
        **kv: Arbitrary keys merged into the payload alongside ``updated``.
    """
    payload = {"updated": datetime.now(timezone.utc).isoformat(timespec="seconds"), **kv}
    tmp = grid_dir / "AUTO_PLOT_STATUS.json.tmp"
    tmp.write_text(json.dumps(payload, indent=2))
    # os.replace is atomic on POSIX — readers never see a half-written file.
    os.replace(tmp, grid_dir / "AUTO_PLOT_STATUS.json")


def read_status(grid_dir: Path) -> dict:
    """Read the current watcher status file.

    Args:
        grid_dir: Directory containing ``AUTO_PLOT_STATUS.json``.

    Returns:
        The parsed status dict, or an empty dict if the file is missing or
        malformed. Used to detect prior completion for idempotent re-runs.
    """
    p = grid_dir / "AUTO_PLOT_STATUS.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def grid_complete(grid_dir: Path, total: int) -> tuple[bool, int]:
    """Check whether grid_benchmark.py has finished all expected combos.

    Args:
        grid_dir: Path to the grid run directory.
        total: Expected combo count (24/72/96 depending on sweep type).

    Returns:
        ``(done, count)`` where ``done`` is True once at least ``total``
        runs have ``status == "done"`` and ``count`` is the current number
        of completed runs. Missing/malformed progress file returns ``(False, 0)``.
    """
    prog = grid_dir / "grid_progress.json"
    if not prog.exists():
        return False, 0
    try:
        data = json.loads(prog.read_text())
        runs = data.get("runs", [])
        done = sum(1 for r in runs if r.get("status") == "done")
        return done >= total, done
    except Exception:
        return False, 0


def notify(title: str, msg: str) -> None:
    """Fire a macOS Notification Center alert via ``osascript``.

    Best-effort — silently no-ops on Linux/CI where ``osascript`` isn't
    present. Embedded double-quotes are downgraded to single-quotes so they
    don't break the AppleScript string literal we synthesize.

    Args:
        title: Notification title.
        msg: Notification body.
    """
    # Double quotes would terminate the AppleScript string — sanitize them.
    safe_title = title.replace('"', "'")
    safe_msg = msg.replace('"', "'")
    try:
        # macOS fallback: AppleScript via `osascript` is the simplest no-deps
        # way to post a banner. check=False because notifications are nice-to-have.
        subprocess.run(
            ["osascript", "-e", f'display notification "{safe_msg}" with title "{safe_title}"'],
            check=False, timeout=5,
        )
    except Exception:
        pass


def log(grid_dir: Path, line: str) -> None:
    """Append a timestamped line to ``<grid_dir>/auto_plot.log``.

    Args:
        grid_dir: Directory holding the log file.
        line: Message to write (newline is appended automatically).
    """
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(grid_dir / "auto_plot.log", "a") as f:
        f.write(f"[{ts}] {line}\n")


def main() -> int:
    """Watch a grid run and trigger plotting + report generation on completion.

    Flow:
      1. Parse args, validate the grid directory.
      2. Bail out idempotently if a prior invocation already reached ``done``.
      3. Poll ``grid_progress.json`` every :data:`POLL_SECONDS` until the
         "done" count reaches ``--total``.
      4. Run ``scripts/grid_plot.py`` (30 minute timeout).
      5. If ``scripts/generate_report.py`` exists, run it too (10 minute timeout).
      6. Update ``AUTO_PLOT_STATUS.json`` and fire macOS notifications at
         each phase boundary.

    Returns:
        ``0`` on success, ``1`` for argument/setup errors, ``2`` on plot
        script timeout, ``3`` on unexpected exception.
    """
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("grid_dir", help="path to results/grid_<stamp>/")
    parser.add_argument("--total", type=int, default=DEFAULT_TOTAL_COMBOS,
                        help=f"expected combo count (default: {DEFAULT_TOTAL_COMBOS})")
    args = parser.parse_args()

    grid_dir = Path(args.grid_dir).resolve()
    total_combos = args.total
    if not grid_dir.is_dir():
        print(f"Not a directory: {grid_dir}", file=sys.stderr)
        return 1

    repo_root = grid_dir.parent.parent  # results/<grid>/ → repo root
    plot_script = repo_root / "scripts" / "grid_plot.py"
    report_script = repo_root / "scripts" / "generate_report.py"
    if not plot_script.is_file():
        print(f"Plot script not found: {plot_script}", file=sys.stderr)
        return 1

    # Idempotent guard: re-launching the watcher after a clean run is a no-op.
    # Lets the bash launcher re-arm safely without duplicate work.
    existing = read_status(grid_dir)
    if existing.get("status") == "done":
        log(grid_dir, "already done — exiting idempotently")
        return 0

    write_status(grid_dir, status="armed", combos_done=0, total=total_combos)
    log(grid_dir, f"watcher armed for {grid_dir.name}; will fire on {total_combos}/{total_combos}")

    # File-polling cadence loop: every POLL_SECONDS, re-read the progress
    # checkpoint and re-publish the count so the dashboard can show live
    # progress through the status file.
    while True:
        done_flag, done_n = grid_complete(grid_dir, total_combos)
        write_status(grid_dir, status="armed", combos_done=done_n, total=total_combos)
        if done_flag:
            log(grid_dir, f"grid reached {done_n}/{total_combos} — firing grid_plot.py")
            break
        time.sleep(POLL_SECONDS)

    write_status(grid_dir, status="plotting", combos_done=total_combos, total=total_combos)
    notify("Grid benchmark complete", f"Running grid_plot.py for {grid_dir.name}")

    started = time.time()
    try:
        # Phase 1: comparative plots. Capture stdout/stderr so we can persist
        # the tail in the log for postmortem; 30-min cap is well above the
        # observed ~3 min plot runtime even on the full 96-combo grid.
        proc = subprocess.run(
            [sys.executable, str(plot_script), str(grid_dir)],
            cwd=str(repo_root),
            capture_output=True, text=True, timeout=1800,  # 30-min cap on plotting
        )
        elapsed = time.time() - started
        ok = proc.returncode == 0
        log(grid_dir, f"grid_plot.py exit={proc.returncode} in {elapsed:.1f}s")
        if proc.stdout:
            log(grid_dir, f"stdout (last 1KB):\n{proc.stdout[-1024:]}")
        if proc.stderr:
            log(grid_dir, f"stderr (last 1KB):\n{proc.stderr[-1024:]}")
        write_status(
            grid_dir,
            status="done" if ok else "failed",
            combos_done=total_combos,
            total=total_combos,
            plot_seconds=round(elapsed, 1),
            plot_returncode=proc.returncode,
        )
        if not ok:
            notify("Comparative plots FAILED", f"exit={proc.returncode} — see auto_plot.log")

        # --- Phase 2: generate the thesis report PDF ----------------------
        report_ok = None
        report_seconds = None
        if report_script.is_file():
            write_status(
                grid_dir, status="reporting",
                combos_done=total_combos, total=total_combos,
                plot_seconds=round(elapsed, 1), plot_returncode=proc.returncode,
            )
            log(grid_dir, "starting report generation")
            r_started = time.time()
            try:
                rproc = subprocess.run(
                    [sys.executable, str(report_script), str(grid_dir)],
                    cwd=str(repo_root),
                    capture_output=True, text=True, timeout=600,
                )
                report_seconds = time.time() - r_started
                report_ok = rproc.returncode == 0
                log(grid_dir, f"generate_report.py exit={rproc.returncode} in {report_seconds:.1f}s")
                if rproc.stdout:
                    log(grid_dir, f"report stdout (last 1KB):\n{rproc.stdout[-1024:]}")
                if rproc.stderr:
                    log(grid_dir, f"report stderr (last 1KB):\n{rproc.stderr[-1024:]}")
            except subprocess.TimeoutExpired:
                log(grid_dir, "generate_report.py timeout after 10m")
                report_ok = False
            except Exception as exc:
                log(grid_dir, f"report generation error: {exc!r}")
                report_ok = False

        # --- Final status -----------------------------------------------------
        final_status = "done" if (ok and report_ok is not False) else "partial"
        if ok and report_ok is False:
            final_status = "plots_only"
        write_status(
            grid_dir,
            status=final_status,
            combos_done=total_combos, total=total_combos,
            plot_seconds=round(elapsed, 1), plot_returncode=proc.returncode,
            report_seconds=round(report_seconds, 1) if report_seconds else None,
            report_ok=report_ok,
        )
        if ok and report_ok:
            notify("Grid + report ready", f"{grid_dir.name}/THESIS_REPORT.pdf")
        elif ok and report_ok is False:
            notify("Plots ready, report failed", grid_dir.name)
        elif ok and report_ok is None:
            notify("Comparative plots ready", f"{grid_dir.name}/comparative/")
    except subprocess.TimeoutExpired:
        log(grid_dir, "grid_plot.py timeout after 30m")
        write_status(grid_dir, status="timeout", combos_done=total_combos, total=total_combos)
        notify("Plot script timed out", grid_dir.name)
        return 2
    except Exception as exc:
        log(grid_dir, f"unexpected error: {exc!r}")
        write_status(grid_dir, status="error", error=str(exc))
        notify("Auto-plot watcher crashed", str(exc))
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
dashboard.py - Live grid benchmark dashboard.

Serves a single auto-refreshing HTML page on http://localhost:8765 showing:
  - Overall combo + algorithm progress (with ETA)
  - Currently-running combo and elapsed time
  - Recently completed combos
  - Per-algorithm aggregated metrics
  - Process / redis / rabbitmq health
  - Stall warning if no new result file in >5 min

Usage:
    python scripts/dashboard.py /path/to/results/grid_YYYYMMDD_HHMMSS

The path can be omitted; in that case the most recent results/grid_* dir is used.
"""

from __future__ import annotations

import glob
import html
import json
import os
import re
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

# HTTP port the dashboard binds. Hardcoded so the bookmark/QR code stays stable.
PORT = 8765
# Browser meta-refresh interval. Short enough to feel live, long enough that
# the work of collect_state() (file globbing + subprocess polling) stays cheap.
REFRESH_SECONDS = 5
# results/ lives one level up from scripts/ (repo layout convention).
RESULTS_ROOT = Path(__file__).resolve().parent.parent / "results"

# Algorithm slugs as they appear in result JSON filenames (round_robin_*.json).
ALGOS = ["round_robin", "random", "best_fit", "worst_fit", "topsis"]
# Per-algorithm accent colors used for the algo-card top borders. Picked to be
# legible on the dark panel background and consistent with the thesis plots.
ALGO_COLORS = {
    "round_robin": "#6aa6ff",
    "random":      "#c084fc",
    "best_fit":    "#34d399",
    "worst_fit":   "#fbbf24",
    "topsis":      "#f87171",
}

# Plots that grid_benchmark.py emits per finished combo (PNG + PDF for each).
PLOT_NAMES = [
    "throughput", "makespan", "tasks_completed", "response_time_cdf",
    "tier_distribution", "tier_duration_heatmap", "cost", "energy", "load_balance",
]


def latest_grid_dir() -> Path | None:
    """Find the most recently modified ``results/grid_*`` directory.

    Returns:
        The newest ``grid_<stamp>`` Path under :data:`RESULTS_ROOT`, or
        ``None`` if no such directory exists. Sorting by ``st_mtime`` (not
        name) picks up resumed/edited runs whose stamp isn't the latest.
    """
    candidates = sorted(RESULTS_ROOT.glob("grid_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def safe_load_json(path: Path) -> dict | list | None:
    """Load a JSON file, swallowing missing/corrupt-file errors.

    grid_benchmark.py writes JSON files atomically but the dashboard polls
    constantly — this guards against the brief window where a file exists but
    is empty/half-written, and against files that disappear mid-read.

    Args:
        path: File path to load.

    Returns:
        Parsed JSON (dict or list) on success, ``None`` on any I/O or decode
        error.
    """
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def ps_grid_processes() -> list[dict]:
    """Snapshot running grid_benchmark and Go ``_benchmark_bin`` processes.

    Shells out to ``ps -ax`` so we don't need a Python ``psutil`` dependency.
    Used by the dashboard to show whether the orchestrator + per-combo Go
    binary are still alive (the "no process found" warning that hints at a
    crashed overnight run).

    Returns:
        A list of dicts with keys ``pid``, ``etime``, ``pcpu``, ``cmd``. Empty
        list if ``ps`` fails or no matching process is running.
    """
    try:
        # Custom -o format with `=` suffixes suppresses the header row, so each
        # output line is a pure data row that we can split on whitespace.
        out = subprocess.run(
            ["ps", "-ax", "-o", "pid=,etime=,pcpu=,command="],
            capture_output=True, text=True, timeout=3,
        ).stdout
    except Exception:
        return []
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        # Match either the Python orchestrator or the per-combo Go binary that
        # grid_benchmark.py spawns (named via `go build -o _benchmark_bin`).
        if "grid_benchmark.py" in line or "_benchmark_bin" in line:
            # maxsplit=3 keeps the (possibly space-containing) command tail intact.
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            pid, etime, pcpu, cmd = parts
            rows.append({"pid": pid, "etime": etime, "pcpu": pcpu, "cmd": cmd})
    return rows


def docker_health() -> dict:
    """Probe Redis + RabbitMQ container status and live counters.

    Three independent ``docker exec`` calls; each is wrapped in its own
    try/except so a single failing call doesn't blank the whole card. The
    container names are the Compose-generated defaults from this repo
    (``distributed-workflow-scheduler-redis-1`` etc).

    Returns:
        ``{redis, rabbitmq, redis_keys, rabbit_msgs}`` — status strings from
        ``docker ps`` plus integer counters. Values are ``None`` for any probe
        that couldn't be obtained.
    """
    info = {"redis": None, "rabbitmq": None, "redis_keys": None, "rabbit_msgs": None}
    try:
        # `docker ps --format` keeps the parse trivial (no JSON dep needed).
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}|{{.Status}}"],
            capture_output=True, text=True, timeout=3,
        ).stdout
        for line in out.splitlines():
            if "|" not in line:
                continue
            name, status = line.split("|", 1)
            if "redis" in name:
                info["redis"] = status
            elif "rabbit" in name:
                info["rabbitmq"] = status
    except Exception:
        pass
    try:
        # DBSIZE is O(1) — safe to poll every refresh.
        out = subprocess.run(
            ["docker", "exec", "distributed-workflow-scheduler-redis-1", "redis-cli", "DBSIZE"],
            capture_output=True, text=True, timeout=3,
        ).stdout.strip()
        info["redis_keys"] = int(out) if out.isdigit() else None
    except Exception:
        pass
    try:
        # rabbitmqctl list_queues with -q (quiet) + --no-table-headers gives
        # plain "name<TAB>messages" rows. Sum the message counts across all
        # queues (task tier queues + result_queue) for a single backlog number.
        out = subprocess.run(
            ["docker", "exec", "distributed-workflow-scheduler-rabbitmq-1",
             "rabbitmqctl", "list_queues", "-q", "--no-table-headers", "name", "messages"],
            capture_output=True, text=True, timeout=4,
        ).stdout
        total = 0
        for line in out.splitlines():
            parts = line.split()
            if len(parts) == 2 and parts[1].isdigit():
                total += int(parts[1])
        info["rabbit_msgs"] = total
    except Exception:
        pass
    return info


def collect_state(grid_dir: Path) -> dict:
    """Walk a grid run directory and assemble the full snapshot for rendering.

    Reads ``grid_progress.json``, every per-algorithm result JSON, the
    ``AUTO_PLOT_STATUS.json`` watcher file, and live process/docker probes.
    Also computes an ETA based on the median observed combo duration.

    Args:
        grid_dir: Path to a ``results/grid_<stamp>/`` directory.

    Returns:
        A flat dict consumed by :func:`render_html` containing progress
        counters, the active combo, recent runs, per-algorithm aggregates,
        plot availability, watcher status, process/health snapshots, and ETA
        in seconds. Numeric fields are ``None`` when undefined.
    """
    state: dict = {
        "grid_dir":    str(grid_dir),
        "grid_name":   grid_dir.name,
        "now":         datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "now_local":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 1. Progress checkpoint
    prog = safe_load_json(grid_dir / "grid_progress.json") or {"runs": []}
    runs = prog.get("runs", [])
    state["runs"] = runs
    state["combos_done"] = sum(1 for r in runs if r.get("status") == "done")

    # 2. All per-algorithm result JSONs (excluding meta files)
    skip = {"grid_progress.json", "manifest.json"}
    algo_files: list[Path] = []
    for p in grid_dir.rglob("*.json"):
        if p.name in skip:
            continue
        if "_configs" in p.parts:
            continue
        algo_files.append(p)
    algo_files.sort(key=lambda p: p.stat().st_mtime)
    state["algo_files"] = algo_files
    state["algos_done"] = len(algo_files)

    # 3. Active combo dir = newest topo_* subdir
    combo_dirs = sorted(
        [p for p in grid_dir.iterdir() if p.is_dir() and p.name.startswith("topo_")],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    state["active_combo"] = combo_dirs[0].name if combo_dirs else None

    # 4. Latest algo result + age
    if algo_files:
        latest = algo_files[-1]
        state["latest_file"]    = latest.name
        state["latest_combo"]   = latest.parent.name
        state["latest_mtime"]   = latest.stat().st_mtime
        state["latest_age_sec"] = max(0, time.time() - state["latest_mtime"])
    else:
        state["latest_file"]    = None
        state["latest_mtime"]   = None
        state["latest_age_sec"] = None

    # 5. Per-algorithm aggregated metrics
    per_algo: dict[str, dict] = {a: {"runs": 0, "tasks": 0, "throughput": [], "makespan": []} for a in ALGOS}
    for p in algo_files:
        algo_match = re.match(r"^([a-z_]+)_edge", p.name)
        if not algo_match:
            continue
        algo = algo_match.group(1)
        if algo not in per_algo:
            continue
        data = safe_load_json(p)
        if not data:
            continue
        summary = data.get("summary", {})
        per_algo[algo]["runs"] += 1
        per_algo[algo]["tasks"] += summary.get("total_tasks_completed", 0) or 0
        tp = summary.get("throughput_tasks_per_min")
        ms = summary.get("avg_workflow_makespan_ms")
        if tp:
            per_algo[algo]["throughput"].append(tp)
        if ms:
            per_algo[algo]["makespan"].append(ms)
    state["per_algo"] = per_algo

    # 6. Combos with plots ready (most-recent-first)
    combos_with_plots: list[tuple[str, float]] = []
    for d in grid_dir.iterdir():
        if not d.is_dir() or not d.name.startswith("topo_"):
            continue
        plots_dir = d / "plots"
        if plots_dir.is_dir() and any(plots_dir.glob("*.png")):
            combos_with_plots.append((d.name, plots_dir.stat().st_mtime))
    combos_with_plots.sort(key=lambda x: x[1], reverse=True)
    state["combos_with_plots"] = [c[0] for c in combos_with_plots]

    # 7. Auto-plot watcher status + final artifacts
    auto_status = safe_load_json(grid_dir / "AUTO_PLOT_STATUS.json") or {}
    state["auto_status"] = auto_status if isinstance(auto_status, dict) else {}
    state["report_pdf_exists"] = (grid_dir / "THESIS_REPORT.pdf").is_file()
    state["comparative_dir"] = (grid_dir / "comparative")
    state["comparative_pdfs"] = sorted([p.name for p in (grid_dir / "comparative").glob("*.pdf")]) if (grid_dir / "comparative").is_dir() else []

    # 8. Processes + docker
    state["procs"] = ps_grid_processes()
    state["docker"] = docker_health()

    # 9. ETA
    # Total combo count: prefer the value the auto_plot watcher wrote (always
    # correct for the live run, e.g. 72 for the seed sweep), else infer from
    # the manifest's max-seen run count, else fall back to the legacy 96.
    auto_total = state["auto_status"].get("total")
    if isinstance(auto_total, int) and auto_total > 0:
        total_combos = auto_total
    else:
        # Heuristic fallback: assume 4 algos per combo when computing total_algos
        total_combos = max(len(runs), 96)
    state["total_combos"] = total_combos
    state["total_algos"]  = total_combos * 4  # rough; informational only
    done = state["combos_done"]
    if runs:
        elapsed_per_combo = [r.get("elapsed_s", 0) for r in runs if r.get("status") == "done"]
        avg_combo = sum(elapsed_per_combo) / len(elapsed_per_combo) if elapsed_per_combo else None
        if avg_combo:
            remaining_combos = total_combos - done
            state["eta_seconds"] = remaining_combos * avg_combo
            state["avg_combo_sec"] = avg_combo
        else:
            state["eta_seconds"] = None
            state["avg_combo_sec"] = None
    else:
        # No combo done yet — estimate from algo cadence
        if algo_files and len(algo_files) >= 2:
            t0 = algo_files[0].stat().st_mtime
            tN = algo_files[-1].stat().st_mtime
            spans = tN - t0
            per_algo_sec = spans / max(1, len(algo_files) - 1)
            state["eta_seconds"] = per_algo_sec * (192 - len(algo_files))
            state["avg_combo_sec"] = per_algo_sec * 5
        else:
            state["eta_seconds"] = None
            state["avg_combo_sec"] = None

    return state


def fmt_duration(seconds: float | None) -> str:
    """Format a duration in seconds as the most compact human-readable string.

    Args:
        seconds: Duration in seconds. ``None`` renders as an em-dash.

    Returns:
        ``"45s"``, ``"3m 12s"``, or ``"2h 14m"`` depending on magnitude. The
        em-dash placeholder keeps tables aligned when a value is unknown.
    """
    if seconds is None:
        return "—"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h, rem = divmod(s, 3600)
    return f"{h}h {rem // 60}m"


def fmt_age(seconds: float | None) -> str:
    """Alias for :func:`fmt_duration` used for "time since X" fields."""
    return fmt_duration(seconds) if seconds is not None else "—"


def render_pipeline(state: dict) -> str:
    """Render the post-completion pipeline panel (watcher status + artifacts).

    Args:
        state: Snapshot dict from :func:`collect_state`.

    Returns:
        HTML for the "Post-completion pipeline" section: a colored status pill
        for the auto_plot watcher state plus download links to the finished
        ``THESIS_REPORT.pdf`` and per-metric comparative PDFs.
    """
    auto = state.get("auto_status") or {}
    s = auto.get("status") or "not running"
    color = {
        "armed":     "var(--accent)",
        "plotting":  "var(--warn)",
        "reporting": "var(--warn)",
        "done":      "var(--ok)",
        "plots_only":"var(--warn)",
        "failed":    "var(--bad)",
        "timeout":   "var(--bad)",
        "error":     "var(--bad)",
        "not running": "var(--mut)",
    }.get(s, "var(--mut)")
    label_map = {
        "armed":     "Watcher armed",
        "plotting":  "Running grid_plot.py",
        "reporting": "Generating THESIS_REPORT.pdf",
        "done":      "All artifacts ready",
        "plots_only":"Plots ready, report failed",
        "failed":    "Comparative plots FAILED",
        "timeout":   "Plot script timed out",
        "error":     "Watcher crashed",
        "not running": "Watcher not running",
    }
    label = label_map.get(s, s)
    extra = ""
    if s == "armed":
        cd, total = auto.get("combos_done", 0), auto.get("total", 96)
        extra = f"<div class=sub>Will fire at {total}/{total}. Currently {cd}/{total}.</div>"
    elif s in ("plotting", "reporting"):
        extra = f"<div class=sub>Phase started at {auto.get('updated', '?')}.</div>"
    elif s in ("done", "plots_only"):
        extra = f"<div class=sub>Plot {auto.get('plot_seconds', '?')}s, report {auto.get('report_seconds', '?')}s.</div>"

    # Download links for final artifacts (served as static files)
    links = []
    if state.get("report_pdf_exists"):
        links.append('<a class=dlink href="/artifact/THESIS_REPORT.pdf">THESIS_REPORT.pdf</a>')
    for name in state.get("comparative_pdfs", []):
        links.append(f'<a class=dlink href="/artifact/comparative/{name}">{name}</a>')
    links_html = ('<div class=dlinks>' + "".join(links) + '</div>') if links else (
        '<p class=muted>No final artifacts yet. They appear here when the watcher completes.</p>'
    )

    return f"""
    <div class="pipeline">
      <div class="pstate" style="border-left:4px solid {color}">
        <div class=label>Status</div>
        <div class=val>{html.escape(label)}</div>
        {extra}
      </div>
    </div>
    {links_html}
    """


def render_plot_gallery(state: dict, selected_combo: str | None) -> str:
    """Render the per-combo plot gallery: combo chips + thumbnail grid.

    Args:
        state: Snapshot dict from :func:`collect_state`.
        selected_combo: Combo id from the ``?combo=`` query string, or ``None``
            to default to the most recent combo with plots.

    Returns:
        HTML containing the row of clickable combo chips and a 3-column grid
        of thumbnails for the selected combo. Returns a "no plots yet"
        placeholder when no combo has finished its plots.
    """
    combos = state["combos_with_plots"]
    if not combos:
        return "<p class=muted>No plots yet — first combo's plots are written when it completes.</p>"

    if selected_combo not in combos:
        selected_combo = combos[0]

    chips = "".join(
        f'<a class="chip{" active" if c == selected_combo else ""}" '
        f'href="?combo={c}#plots">{html.escape(c)}</a>'
        for c in combos
    )

    grid = "".join(
        f"""<a class=thumb href="/plots/{selected_combo}/{name}.png" target=_blank>
              <img src="/plots/{selected_combo}/{name}.png" alt="{name}" loading=lazy>
              <div class=tcap>{name.replace('_', ' ')}</div>
            </a>"""
        for name in PLOT_NAMES
    )
    return f"""
    <div class=chip-row>{chips}</div>
    <div class=thumb-grid>{grid}</div>
    """


def render_html(state: dict, selected_combo: str | None = None) -> str:
    """Render the full dashboard HTML page.

    Assembles header, stall banner (if any), the two progress panels (combo
    and algo), health grid, live process table, per-algorithm aggregate
    cards, recent combo table, post-completion pipeline, and the plot
    gallery. The page sets ``<meta http-equiv=refresh>`` so the browser
    re-fetches itself every :data:`REFRESH_SECONDS`.

    Args:
        state: Snapshot dict from :func:`collect_state`.
        selected_combo: Optional combo id for the plot gallery selection.

    Returns:
        Full HTML document as a string (UTF-8 encodable).
    """
    combos_done = state["combos_done"]
    total_combos = state["total_combos"]
    pct_combos = (combos_done / total_combos * 100) if total_combos else 0

    algos_done = state["algos_done"]
    total_algos = state["total_algos"]
    pct_algos = (algos_done / total_algos * 100) if total_algos else 0

    age = state["latest_age_sec"]
    # Stall heuristic: >5 minutes since the last result JSON was written.
    # 300s comfortably exceeds the worst observed combo duration; tripping it
    # while the orchestrator is still alive points at a drain deadlock.
    stalled = age is not None and age > 300
    stall_banner = ""
    if stalled and any(p["cmd"].startswith("/opt/homebrew") and "grid_benchmark" in p["cmd"] for p in state["procs"]):
        stall_banner = f"""
        <div class="banner stall">
          <strong>STALL WARNING</strong>
          No new result JSON for {fmt_age(age)}. Orchestrator still alive — possible drain deadlock.
        </div>"""

    # Recent runs table
    recent = list(reversed(state["runs"]))[:8]
    if recent:
        rows = "".join(
            f"<tr><td>{html.escape(r.get('combo_id', '?'))}</td>"
            f"<td>{r.get('capacity', '')}</td>"
            f"<td class=num>{fmt_duration(r.get('elapsed_s'))}</td>"
            f"<td>{r.get('status', '')}</td></tr>"
            for r in recent
        )
        recent_table = f"""
        <table class="data">
          <thead><tr><th>combo</th><th>cap</th><th>elapsed</th><th>status</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>"""
    else:
        recent_table = "<p class=muted>No combos completed yet — first checkpoint writes after combo #1 finishes all 5 algos.</p>"

    # Per-algo aggregates
    algo_cards = ""
    for algo in ALGOS:
        a = state["per_algo"][algo]
        color = ALGO_COLORS[algo]
        avg_tp = (sum(a["throughput"]) / len(a["throughput"])) if a["throughput"] else None
        avg_ms = (sum(a["makespan"]) / len(a["makespan"])) if a["makespan"] else None
        algo_cards += f"""
        <div class="algo-card" style="border-top:3px solid {color}">
          <div class="algo-name">{algo}</div>
          <div class="algo-stat"><span class=label>runs</span><span class=val>{a['runs']}</span></div>
          <div class="algo-stat"><span class=label>tasks</span><span class=val>{a['tasks']:,}</span></div>
          <div class="algo-stat"><span class=label>avg tp</span><span class=val>{avg_tp:.1f}/min</span></div>
          <div class="algo-stat"><span class=label>avg ms</span><span class=val>{avg_ms:,.0f}ms</span></div>
        </div>""" if avg_tp and avg_ms else f"""
        <div class="algo-card" style="border-top:3px solid {color}">
          <div class="algo-name">{algo}</div>
          <div class="algo-stat"><span class=label>runs</span><span class=val>{a['runs']}</span></div>
          <div class="algo-stat"><span class=label>tasks</span><span class=val>{a['tasks']:,}</span></div>
          <div class="algo-stat muted"><span class=label>avg tp</span><span class=val>—</span></div>
          <div class="algo-stat muted"><span class=label>avg ms</span><span class=val>—</span></div>
        </div>"""

    # Processes
    if state["procs"]:
        proc_rows = "".join(
            f"<tr><td class=num>{p['pid']}</td><td class=num>{p['etime']}</td>"
            f"<td class=num>{p['pcpu']}%</td>"
            f"<td class=cmd>{html.escape(p['cmd'][:120])}</td></tr>"
            for p in state["procs"]
        )
        proc_table = f"""
        <table class="data">
          <thead><tr><th>PID</th><th>elapsed</th><th>CPU</th><th>command</th></tr></thead>
          <tbody>{proc_rows}</tbody>
        </table>"""
    else:
        proc_table = "<p class=muted bad>No grid_benchmark / _benchmark_bin process found. Run may have crashed.</p>"

    d = state["docker"]
    redis_ok = bool(d["redis"] and "healthy" in (d["redis"] or "").lower())
    rabbit_ok = bool(d["rabbitmq"] and "healthy" in (d["rabbitmq"] or "").lower())

    health = f"""
    <div class="health-grid">
      <div class="hcell {'ok' if redis_ok else 'bad'}">
        <div class=label>Redis</div>
        <div class=val>{html.escape(d['redis'] or 'down')}</div>
        <div class=sub>{d['redis_keys']!s} keys</div>
      </div>
      <div class="hcell {'ok' if rabbit_ok else 'bad'}">
        <div class=label>RabbitMQ</div>
        <div class=val>{html.escape(d['rabbitmq'] or 'down')}</div>
        <div class=sub>{d['rabbit_msgs']!s} msgs queued</div>
      </div>
      <div class="hcell {'warn' if stalled else 'ok'}">
        <div class=label>Last result</div>
        <div class=val>{fmt_age(age)} ago</div>
        <div class=sub>{html.escape(state['latest_file'] or '—')}</div>
      </div>
      <div class="hcell ok">
        <div class=label>Active combo</div>
        <div class=val>{html.escape((state['active_combo'] or '—')[:32])}</div>
        <div class=sub>avg combo ≈ {fmt_duration(state.get('avg_combo_sec'))}</div>
      </div>
    </div>"""

    return f"""<!doctype html>
<html lang=en>
<head>
<meta charset=utf-8>
<meta http-equiv="refresh" content="{REFRESH_SECONDS}">
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Grid Benchmark — {html.escape(state['grid_name'])}</title>
<style>
  :root {{
    --bg:#0d1117; --panel:#161b22; --panel2:#1c232c; --line:#30363d;
    --fg:#e6edf3; --mut:#8b949e; --ok:#3fb950; --warn:#d29922; --bad:#f85149;
    --accent:#58a6ff;
  }}
  * {{ box-sizing:border-box; }}
  html,body {{ background:var(--bg); color:var(--fg); margin:0;
    font:14px/1.4 -apple-system,BlinkMacSystemFont,"SF Pro Text",sans-serif; }}
  body {{ padding:24px; max-width:1400px; margin:0 auto; }}
  @media (max-width:700px) {{
    body {{ padding:14px; font-size:13px; }}
    header {{ flex-direction:column; align-items:flex-start; gap:8px; }}
    header .meta {{ text-align:left; }}
    .grid, .grid.three {{ grid-template-columns:1fr; gap:14px; }}
    .health-grid {{ grid-template-columns:repeat(2,1fr); gap:10px; }}
    .algo-row {{ grid-template-columns:repeat(2,1fr); gap:10px; }}
    .progress-row .pct {{ font-size:18px; }}
    .panel {{ padding:14px; }}
    section {{ margin-bottom:14px; }}
    table.data td.cmd {{ display:none; }}
  }}
  h1 {{ font-size:22px; margin:0 0 4px; }}
  h2 {{ font-size:14px; text-transform:uppercase; letter-spacing:0.08em;
    color:var(--mut); margin:0 0 12px; }}
  .sub {{ color:var(--mut); font-size:12px; }}
  .muted {{ color:var(--mut); }}
  .bad {{ color:var(--bad); }}
  header {{ display:flex; justify-content:space-between; align-items:flex-end;
    border-bottom:1px solid var(--line); padding-bottom:14px; margin-bottom:24px; }}
  header .meta {{ text-align:right; color:var(--mut); font-size:12px; }}
  .banner.stall {{ background:rgba(248,81,73,0.12); border:1px solid var(--bad);
    color:#ffd6d3; padding:14px 18px; border-radius:8px; margin-bottom:20px; }}
  .grid {{ display:grid; gap:20px; grid-template-columns:repeat(2,1fr); }}
  .grid.three {{ grid-template-columns:repeat(3,1fr); }}
  .panel {{ background:var(--panel); border:1px solid var(--line);
    border-radius:10px; padding:18px; }}
  .progress-bar {{ background:var(--panel2); height:32px; border-radius:6px;
    overflow:hidden; position:relative; margin:8px 0 4px; }}
  .progress-fill {{ height:100%; background:linear-gradient(90deg,#1f6feb,#58a6ff);
    transition:width 0.4s; position:relative; }}
  .progress-fill.algos {{ background:linear-gradient(90deg,#238636,#3fb950); }}
  .progress-fill::after {{ content:""; position:absolute; inset:0;
    background:linear-gradient(90deg,transparent 0%,rgba(255,255,255,0.15) 50%,transparent 100%);
    animation:shimmer 2s linear infinite; }}
  @keyframes shimmer {{ 0% {{ transform:translateX(-100%); }} 100% {{ transform:translateX(100%); }} }}
  .progress-row {{ display:flex; justify-content:space-between; align-items:baseline; }}
  .progress-row .pct {{ font-size:22px; font-weight:600; }}
  .progress-row .count {{ color:var(--mut); font-size:12px; }}
  .progress-row .eta {{ color:var(--accent); font-weight:500; }}
  table.data {{ width:100%; border-collapse:collapse; font-size:13px; }}
  table.data th {{ text-align:left; padding:8px 10px; background:var(--panel2);
    color:var(--mut); font-weight:500; text-transform:uppercase;
    font-size:11px; letter-spacing:0.05em; border-bottom:1px solid var(--line); }}
  table.data td {{ padding:8px 10px; border-bottom:1px solid var(--line); }}
  table.data td.num {{ font-variant-numeric:tabular-nums; }}
  table.data td.cmd {{ font-family:ui-monospace,SFMono-Regular,monospace; font-size:11px;
    color:var(--mut); }}
  .algo-row {{ display:grid; grid-template-columns:repeat(5,1fr); gap:12px; }}
  .algo-card {{ background:var(--panel2); border-radius:8px; padding:12px; }}
  .algo-card .algo-name {{ font-weight:600; margin-bottom:8px; font-size:12px;
    text-transform:uppercase; letter-spacing:0.05em; }}
  .algo-stat {{ display:flex; justify-content:space-between; font-size:12px;
    padding:3px 0; color:var(--mut); }}
  .algo-stat .val {{ color:var(--fg); font-variant-numeric:tabular-nums; }}
  .algo-stat.muted .val {{ color:var(--mut); }}
  .health-grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; }}
  .hcell {{ background:var(--panel2); padding:14px; border-radius:8px;
    border-left:3px solid var(--ok); }}
  .hcell.warn {{ border-left-color:var(--warn); }}
  .hcell.bad {{ border-left-color:var(--bad); }}
  .hcell .label {{ font-size:11px; text-transform:uppercase;
    letter-spacing:0.06em; color:var(--mut); margin-bottom:4px; }}
  .hcell .val {{ font-size:15px; margin-bottom:2px; word-break:break-word; }}
  .hcell .sub {{ font-size:11px; color:var(--mut); }}
  section {{ margin-bottom:22px; }}
  .ts-pulse {{ display:inline-block; width:8px; height:8px; border-radius:50%;
    background:var(--ok); margin-right:6px; animation:pulse 1.5s infinite; }}
  @keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:0.3}} }}
  .chip-row {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:14px; }}
  .chip {{ background:var(--panel2); color:var(--mut); padding:6px 12px;
    border-radius:14px; text-decoration:none; font-size:12px;
    border:1px solid transparent; transition:all 0.15s; }}
  .chip:hover {{ background:#22303d; color:var(--fg); }}
  .chip.active {{ background:rgba(88,166,255,0.18); color:var(--accent);
    border-color:rgba(88,166,255,0.35); }}
  .thumb-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:12px; }}
  .thumb {{ background:#fff; border-radius:6px; overflow:hidden;
    text-decoration:none; color:inherit; display:block;
    border:1px solid var(--line); transition:transform 0.15s, border-color 0.15s; }}
  .thumb:hover {{ transform:translateY(-2px); border-color:var(--accent); }}
  .thumb img {{ width:100%; height:auto; display:block; }}
  .thumb .tcap {{ background:var(--panel2); color:var(--mut);
    padding:6px 10px; font-size:11px; text-transform:uppercase;
    letter-spacing:0.05em; text-align:center; }}
  @media (max-width:700px) {{
    .thumb-grid {{ grid-template-columns:repeat(2,1fr); gap:8px; }}
  }}
  .pipeline {{ margin-bottom:14px; }}
  .pstate {{ background:var(--panel2); padding:14px 18px; border-radius:8px; }}
  .pstate .label {{ font-size:11px; text-transform:uppercase;
    letter-spacing:0.06em; color:var(--mut); margin-bottom:4px; }}
  .pstate .val {{ font-size:18px; font-weight:600; }}
  .dlinks {{ display:flex; flex-wrap:wrap; gap:8px; }}
  .dlink {{ background:rgba(88,166,255,0.12); color:var(--accent);
    border:1px solid rgba(88,166,255,0.35); border-radius:6px;
    padding:8px 14px; text-decoration:none; font-size:12px;
    font-family:ui-monospace,SFMono-Regular,monospace; }}
  .dlink:hover {{ background:rgba(88,166,255,0.22); }}
</style>
</head>
<body>

<header>
  <div>
    <h1>Grid Benchmark Dashboard</h1>
    <div class=sub>{html.escape(state['grid_name'])}</div>
  </div>
  <div class=meta>
    <span class=ts-pulse></span>{html.escape(state['now_local'])} · refresh {REFRESH_SECONDS}s
  </div>
</header>

{stall_banner}

<section class="grid">
  <div class=panel>
    <h2>Combos completed</h2>
    <div class=progress-row>
      <span class=pct>{combos_done} / {total_combos}</span>
      <span class=eta>ETA {fmt_duration(state.get('eta_seconds'))}</span>
    </div>
    <div class=progress-bar><div class=progress-fill style="width:{pct_combos:.2f}%"></div></div>
    <div class=sub>{pct_combos:.1f}% — avg combo: {fmt_duration(state.get('avg_combo_sec'))}</div>
  </div>
  <div class=panel>
    <h2>Algorithm runs completed</h2>
    <div class=progress-row>
      <span class=pct>{algos_done} / {total_algos}</span>
      <span class=count>(24 main + 72 topsis-only)</span>
    </div>
    <div class=progress-bar><div class="progress-fill algos" style="width:{pct_algos:.2f}%"></div></div>
    <div class=sub>{pct_algos:.1f}% of 192 algo executions</div>
  </div>
</section>

<section class=panel>
  <h2>Health</h2>
  {health}
</section>

<section class=panel>
  <h2>Live processes</h2>
  {proc_table}
</section>

<section class=panel>
  <h2>Per-algorithm aggregates (across {algos_done} runs)</h2>
  <div class=algo-row>{algo_cards}</div>
</section>

<section class=panel>
  <h2>Recent combo checkpoints</h2>
  {recent_table}
</section>

<section class=panel>
  <h2>Post-completion pipeline</h2>
  {render_pipeline(state)}
</section>

<section class=panel id=plots>
  <h2>Per-combo plots ({len(state['combos_with_plots'])} combo{'s' if len(state['combos_with_plots']) != 1 else ''} ready)</h2>
  {render_plot_gallery(state, selected_combo)}
</section>

</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    """HTTP handler serving the dashboard, plot thumbnails, and artifact PDFs.

    The grid directory being watched is shared across all handler instances
    via the :attr:`grid_dir` class attribute (set once in :func:`main`).
    """

    grid_dir: Path = None  # type: ignore[assignment]

    def log_message(self, format: str, *args) -> None:  # silence default logging
        """Silence BaseHTTPRequestHandler's default stderr access log."""
        return

    def do_GET(self) -> None:
        """Route GET requests: dashboard HTML, plot images, or artifact files."""
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/plots/"):
            self._serve_plot(path[len("/plots/"):])
            return

        if path.startswith("/artifact/"):
            self._serve_artifact(path[len("/artifact/"):])
            return

        if path != "/" and path != "/index.html":
            self.send_error(404)
            return

        # ?combo=topo_uniform_s_cap_small — selects which combo's plots show.
        from urllib.parse import parse_qs
        qs = parse_qs(parsed.query)
        selected = (qs.get("combo") or [None])[0]

        try:
            state = collect_state(self.grid_dir)
            body = render_html(state, selected).encode("utf-8")
        except Exception as exc:
            # Surface any render error in-page so a single bad poll doesn't
            # take the whole dashboard offline mid-run.
            body = f"<pre>dashboard error: {html.escape(str(exc))}</pre>".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        # no-store: every refresh must hit the server (live data, not cache).
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_artifact(self, relpath: str) -> None:
        """Serve a whitelisted final artifact (PDF report or auto_plot log).

        Args:
            relpath: Path relative to ``/artifact/`` from the URL. Only three
                shapes are allowed; anything else returns 404.
        """
        # Allowed: THESIS_REPORT.pdf, auto_plot.log, comparative/<name>.pdf
        if ".." in relpath:
            self.send_error(404); return
        if relpath == "THESIS_REPORT.pdf":
            file_path = self.grid_dir / relpath
            ctype = "application/pdf"
        elif relpath == "auto_plot.log":
            file_path = self.grid_dir / relpath
            ctype = "text/plain; charset=utf-8"
        elif relpath.startswith("comparative/") and re.match(r"^comparative/[a-zA-Z0-9_.-]+\.pdf$", relpath):
            file_path = self.grid_dir / relpath
            ctype = "application/pdf"
        else:
            self.send_error(404); return
        if not file_path.is_file():
            self.send_error(404); return
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=30")
        self.end_headers()
        self.wfile.write(data)

    def _serve_plot(self, relpath: str) -> None:
        """Serve a single per-combo plot file (PNG or PDF).

        Args:
            relpath: ``<combo>/<name>.{png,pdf}`` from the URL after the
                ``/plots/`` prefix. Anything not matching that shape returns
                404. The resolved path is checked to stay under
                :attr:`grid_dir` to defend against symlink traversal.
        """
        # Path traversal guard: resolve and confirm under grid_dir/<combo>/plots/
        parts = relpath.split("/")
        if len(parts) != 2 or not parts[0].startswith("topo_") or ".." in parts:
            self.send_error(404)
            return
        combo, fname = parts
        if not re.match(r"^[a-z_]+\.(png|pdf)$", fname):
            self.send_error(404)
            return
        file_path = (self.grid_dir / combo / "plots" / fname).resolve()
        try:
            file_path.relative_to(self.grid_dir.resolve())
        except ValueError:
            self.send_error(404)
            return
        if not file_path.is_file():
            self.send_error(404)
            return
        ctype = "image/png" if fname.endswith(".png") else "application/pdf"
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        self.wfile.write(data)


def main() -> int:
    """Start the dashboard HTTP server.

    Resolves the grid directory (from argv or the newest ``results/grid_*``),
    binds the threaded HTTP server (host overridable via ``DASHBOARD_HOST``),
    prints local + LAN URLs, then serves until Ctrl-C.

    Returns:
        Process exit code: ``0`` on clean shutdown, ``1`` if the grid dir is
        missing, ``2`` if the port is already in use.
    """
    if len(sys.argv) > 1:
        grid_dir = Path(sys.argv[1]).resolve()
    else:
        grid_dir = latest_grid_dir()
    if not grid_dir or not grid_dir.is_dir():
        print(f"Error: grid dir not found: {grid_dir}", file=sys.stderr)
        return 1

    Handler.grid_dir = grid_dir
    # 0.0.0.0 by default so a phone on the same Wi-Fi can hit the LAN URL.
    bind_host = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
    try:
        server = ThreadingHTTPServer((bind_host, PORT), Handler)
    except OSError as exc:
        print(f"Port {PORT} in use: {exc}", file=sys.stderr)
        return 2

    # Best-effort LAN IP discovery: open a UDP socket to a public address
    # (no packet sent — connect() on UDP just picks the outbound interface)
    # and read back the local endpoint. Falls back to None when offline.
    lan_ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass

    print(f"Dashboard bind: http://{bind_host}:{PORT}/")
    print(f"Local URL:      http://127.0.0.1:{PORT}/")
    if lan_ip:
        print(f"LAN URL:        http://{lan_ip}:{PORT}/  (phone on same Wi-Fi)")
    print(f"Watching:       {grid_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

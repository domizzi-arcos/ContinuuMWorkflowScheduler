"""Flatten grid result directories into three tidy CSV tables.

Walks one or more grid directories (``<grid>/<combo>/*.json``) and builds:

* ``runs``      - one row per run (combo × algorithm): identifiers, every
                  ``summary`` scalar, ``by_tier.<TIER>.<metric>`` and
                  ``end_state.<key>``;
* ``tasks``     - one row per completed task;
* ``workflows`` - one row per workflow instance.

Run identity comes from ``meta.labels`` when present (v2) and otherwise from
the combo directory name (v1). When a combo directory holds more than one
file for the same algorithm (a failed combo that was resumed), the newest
file by ``meta.timestamp`` is kept and a warning is printed.

Usage: python3 scripts/analysis/load.py GRID_DIR [GRID_DIR ...] --out DIR
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
import warnings

import pandas as pd

# topo_<topology>_cap_<capacity>[_w_<weights>][_sp_<speed>][_seed<N>][_iv<ms>]
COMBO_RE = re.compile(
    r"^topo_(?P<topology>.+?)_cap_(?P<capacity>small|medium|large)"
    r"(?:_w_(?P<weights>[a-z0-9]+))?(?:_sp_(?P<speed>[a-z0-9]+))?"
    r"(?:_seed(?P<seed>\d+))?(?:_iv(?P<interval_ms>\d+))?$"
)

DEFAULT_INTERVAL_MS = 2000
DEFAULT_SPEED = "none"
DEFAULT_WINDOW_S = 30
TIERS = ["EDGE", "FOG", "CLOUD"]
RUN_KEYS = ["grid", "combo", "topology", "capacity", "seed", "interval_ms", "window_s", "speed",
            "weights", "algorithm"]


def parse_combo(combo: str) -> dict:
    """Topology, capacity, weights, seed and interval from a combo ID.

    Returns an empty dict when the name is not a grid combo.
    """
    m = COMBO_RE.match(combo)
    if not m:
        return {}
    d = m.groupdict()
    return {
        "topology": d["topology"],
        "capacity": d["capacity"],
        "weights": d["weights"] or "balanced",
        "speed": d["speed"] or DEFAULT_SPEED,
        "seed": int(d["seed"]) if d["seed"] else None,
        "interval_ms": int(d["interval_ms"]) if d["interval_ms"] else DEFAULT_INTERVAL_MS,
    }


def run_identity(grid: str, combo: str, meta: dict) -> dict | None:
    """Identity columns of one run, preferring meta.labels over the path."""
    ident = parse_combo(combo)
    labels = meta.get("labels") or {}
    if not ident and not labels:
        return None
    for key, cast in (("topology", str), ("capacity", str), ("weights", str),
                      ("speed", str), ("seed", int), ("interval_ms", int)):
        if key in labels:
            ident[key] = cast(labels[key])
    ident.setdefault("weights", "balanced")
    ident.setdefault("speed", DEFAULT_SPEED)
    ident.setdefault("interval_ms", DEFAULT_INTERVAL_MS)
    ident["window_s"] = int(meta.get("duration_seconds") or DEFAULT_WINDOW_S)
    ident.setdefault("seed", None)
    ident.update(grid=os.path.basename(os.path.normpath(grid)), combo=combo,
                 algorithm=meta.get("algorithm"))
    return ident


def _result_files(grid: str):
    for path in sorted(glob.glob(os.path.join(grid, "*", "*.json"))):
        combo = os.path.basename(os.path.dirname(path))
        if combo.startswith("_"):
            continue
        yield combo, path


def load_grids(grids: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (runs, tasks, workflows) DataFrames for the given grid dirs."""
    chosen: dict[tuple, tuple[str, dict, dict]] = {}
    for grid in grids:
        for combo, path in _result_files(grid):
            with open(path) as f:
                data = json.load(f)
            meta = data.get("meta", {})
            ident = run_identity(grid, combo, meta)
            if ident is None:
                continue
            key = (ident["grid"], combo, ident["algorithm"])
            if key in chosen:
                warnings.warn(f"duplicate result for {key}; keeping the newest by meta.timestamp")
                if meta.get("timestamp", "") <= chosen[key][1].get("meta", {}).get("timestamp", ""):
                    continue
            chosen[key] = (path, data, ident)

    run_rows, task_rows, wf_rows = [], [], []
    warned_seq = False
    for path, data, ident in chosen.values():
        base = {k: ident.get(k) for k in RUN_KEYS}
        meta, summary = data.get("meta", {}), data.get("summary", {})
        row = dict(base, file=os.path.basename(path), timestamp=meta.get("timestamp"),
                   git_commit=(meta.get("provenance") or {}).get("git_commit"))
        num = meta.get("num_servers") or {}
        for tier in TIERS:
            row[f"num_servers.{tier}"] = num.get(tier, 0)
        row["num_nodes"] = sum(num.values())
        for k, v in summary.items():
            if k in ("by_tier", "end_state"):
                continue
            row[k] = v
        for tier, vals in (summary.get("by_tier") or {}).items():
            for k, v in vals.items():
                row[f"by_tier.{tier}.{k}"] = v
        for k, v in (summary.get("end_state") or {}).items():
            row[f"end_state.{k}"] = v
        wfs = data.get("workflow_results") or []
        row["workflows_total"] = len(wfs)
        run_rows.append(row)

        for t in data.get("task_results") or []:
            task_rows.append(dict(base, **{k: v for k, v in t.items() if k != "dataflow_path"}))

        # v2 files carry seq; v1 files are ranked by creation time.
        has_seq = all("seq" in w for w in wfs)
        if not has_seq and wfs and not warned_seq:
            warnings.warn("workflow_results without seq (v1 files); using rank of created_at")
            warned_seq = True
        order = sorted(range(len(wfs)), key=lambda i: wfs[i].get("created_at", ""))
        rank = {i: r for r, i in enumerate(order)}
        for i, w in enumerate(wfs):
            wf = dict(base, **w)
            if not has_seq:
                wf["seq"] = rank[i]
            wf_rows.append(wf)

    runs = pd.DataFrame(run_rows)
    tasks = pd.DataFrame(task_rows)
    workflows = pd.DataFrame(wf_rows)
    if not runs.empty:
        runs = runs.sort_values(RUN_KEYS[1:], na_position="first", kind="stable").reset_index(drop=True)
    return runs, tasks, workflows


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("grids", nargs="+", help="grid result directories")
    p.add_argument("--out", required=True, help="output directory for runs/tasks/workflows.csv")
    args = p.parse_args(argv)
    runs, tasks, workflows = load_grids(args.grids)
    if runs.empty:
        print("no runs found", file=sys.stderr)
        return 1
    os.makedirs(args.out, exist_ok=True)
    runs.to_csv(os.path.join(args.out, "runs.csv"), index=False)
    tasks.to_csv(os.path.join(args.out, "tasks.csv"), index=False)
    workflows.to_csv(os.path.join(args.out, "workflows.csv"), index=False)
    print(f"{len(runs)} runs, {len(tasks)} tasks, {len(workflows)} workflows -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

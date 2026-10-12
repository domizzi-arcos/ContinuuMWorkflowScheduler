"""Regenerate every analysis output from raw grid JSON.

    python3 scripts/analysis/run.py GRID_DIR [GRID_DIR ...] [--out analysis_out]
                                    [--audit results/audit_x/audit.csv]

Writes <out>/{runs,tasks,workflows}.csv, <out>/tables/*.csv,
<out>/figures/*.pdf and LaTeX (<out>/latex/, or paper/generated/ when a
paper/ directory exists). Deterministic: same inputs, same outputs.
"""

from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import pandas as pd  # noqa: E402

import figures  # noqa: E402
import latex  # noqa: E402
import load  # noqa: E402
import stats  # noqa: E402


def server_table(grids: list[str]) -> pd.DataFrame:
    """One row per (run, server) from v2 server snapshots (power_watts)."""
    import glob
    import json
    rows = []
    for grid in grids:
        for path in sorted(glob.glob(os.path.join(grid, "*", "*.json"))):
            combo = os.path.basename(os.path.dirname(path))
            if combo.startswith("_"):
                continue
            with open(path) as f:
                data = json.load(f)
            algo = data.get("meta", {}).get("algorithm")
            for s in data.get("servers") or []:
                if "power_watts" in s:
                    rows.append({"grid": os.path.basename(os.path.normpath(grid)), "combo": combo,
                                 "algorithm": algo, "server_type": s["server_type"], "power_watts": s["power_watts"]})
    return pd.DataFrame(rows)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("grids", nargs="+")
    p.add_argument("--out", default="analysis_out")
    p.add_argument("--audit", default=None, help="audit CSV from cmd/audit to join (optional)")
    p.add_argument("--ref", default=stats.REF)
    args = p.parse_args(argv)

    runs, tasks, workflows = load.load_grids(args.grids)
    if runs.empty:
        print("no runs found", file=sys.stderr)
        return 1
    os.makedirs(args.out, exist_ok=True)
    runs.to_csv(os.path.join(args.out, "runs.csv"), index=False)
    tasks.to_csv(os.path.join(args.out, "tasks.csv"), index=False)
    workflows.to_csv(os.path.join(args.out, "workflows.csv"), index=False)

    tables = os.path.join(args.out, "tables")
    os.makedirs(tables, exist_ok=True)
    win = stats.wins(runs)
    normalised = pd.concat([stats.normalised_means(runs, m, args.ref) for m in stats.METRICS], ignore_index=True)
    tests = pd.concat([stats.paired_tests(runs, m, args.ref) for m in stats.METRICS], ignore_index=True)
    matched, completion = stats.matched_workflows(workflows)
    tiers = stats.per_tier(tasks)
    outputs = {
        "grand_mean": stats.grand_mean(runs), "wins": win, "tier_share": stats.tier_share(runs),
        "normalised": normalised, "paired_tests": tests, "matched_makespan": matched,
        "completion": completion, "per_tier": tiers, "queueing_delay": stats.queueing_delay(tasks),
    }
    servers = server_table(args.grids)
    if not servers.empty:
        outputs["power_choice"] = stats.power_choice(tasks, servers)
    if args.audit:
        audit = pd.read_csv(args.audit)
        outputs["feasibility"] = runs.merge(audit, on="combo", how="left", suffixes=("", "_audit"))[
            ["combo", "algorithm", "workflows_total", "total_workflows_completed", "wf_infeasible"]]
    for name, df in outputs.items():
        df.to_csv(os.path.join(tables, f"{name}.csv"), float_format="%.10g")

    figs = figures.all_figures(runs, os.path.join(args.out, "figures"))
    latex_dir = os.path.join("paper", "generated") if os.path.isdir("paper") else os.path.join(args.out, "latex")
    tex = latex.write_all(latex_dir, runs, normalised, tests, win, tiers)

    print(f"{len(runs)} runs from {len(args.grids)} grid(s)")
    print(f"tables : {tables}/ ({len(outputs)} files)")
    print(f"figures: {', '.join(os.path.basename(f) for f in figs)}")
    print(f"latex  : {', '.join(tex)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

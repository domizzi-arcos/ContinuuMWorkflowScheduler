#!/usr/bin/env python3
"""
Experiment analysis and plotting for the distributed workflow scheduler.

Usage:
    python analysis/plot.py results/benchmark_<timestamp>
    python analysis/plot.py results/*.json

Outputs:
    analysis/figures/<figure_name>.pdf

When multiple result files are provided, every figure compares all algorithms
found in those inputs on the same plot.
"""

import glob
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

_SCRIPTS = Path(__file__).resolve().parent.parent.parent / "scripts"
if _SCRIPTS.is_dir():  # repo checkout; in the image it sits next to this file
    sys.path.insert(0, str(_SCRIPTS))
import algorithms  # noqa: E402

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "legend.fontsize": 10,
    "figure.dpi": 150,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
})

# Policy colours and labels come from scripts/algorithms.py (copied next to
# this file in the plotter image; found via ../../scripts in a checkout).
ALGO_COLORS = algorithms.COLORS
TIER_COLORS = {"EDGE": "#F28E2B", "FOG": "#76B7B2", "CLOUD": "#59A14F"}

FIGURES_DIR = Path(__file__).parent / "figures"
REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_input_paths(args):
    """Resolve files, directories, and glob patterns from cwd or repo root."""
    if not args:
        default = REPO_ROOT / "results"
        paths = sorted(default.glob("*.json"))
        if not paths:
            sys.exit("Usage: python analysis/plot.py results/*.json\nNo .json files found in results/")
        return paths

    resolved = []
    seen = set()
    for arg in args:
        candidates = []
        raw = Path(arg)
        repo_relative = REPO_ROOT / arg

        if raw.is_file():
            candidates = [raw]
        elif raw.is_dir():
            candidates = sorted(raw.glob("*.json"))
        elif repo_relative.is_file():
            candidates = [repo_relative]
        elif repo_relative.is_dir():
            candidates = sorted(repo_relative.glob("*.json"))
        else:
            patterns = [arg, str(repo_relative)]
            for pattern in patterns:
                candidates.extend(Path(match) for match in glob.glob(pattern))

        for path in sorted(candidates):
            if path.suffix != ".json":
                continue
            key = path.resolve()
            if key in seen:
                continue
            seen.add(key)
            resolved.append(key)

    if not resolved:
        sys.exit(f"No result files found for inputs: {', '.join(args)}")
    return resolved

def load_results(paths):
    """Return a list of parsed experiment dicts."""
    experiments = []
    for p in sorted(paths):
        with open(p) as f:
            data = json.load(f)
        data["_file"] = str(p)
        experiments.append(data)
    if not experiments:
        sys.exit("No result files found.")
    return experiments


def tasks_df(experiments):
    rows = []
    for exp in experiments:
        algo = exp["meta"]["algorithm"]
        for t in exp.get("task_results", []):
            rows.append({
                "algorithm":      algo,
                "server_type":    t["server_type"],
                "effective_ms":   t["effective_duration_ms"],
                "cost_usd":       t["cost_usd"],
                "energy_wh":      t["energy_wh"],
                "start_time":     pd.to_datetime(t["start_time"]),
                "end_time":       pd.to_datetime(t["end_time"]),
                "workflow_id":    t.get("workflow_id", ""),
            })
    return pd.DataFrame(rows)


def workflows_df(experiments):
    rows = []
    for exp in experiments:
        algo = exp["meta"]["algorithm"]
        for w in exp.get("workflow_results", []):
            rows.append({
                "algorithm":    algo,
                "template":     w["template_name"],
                "makespan_ms":  w["makespan_ms"],
                "status":       w["status"],
                "priority":     w["priority"],
            })
    return pd.DataFrame(rows)


def summary_df(experiments):
    rows = []
    for exp in experiments:
        m = exp["meta"]
        s = exp["summary"]
        rows.append({
            "algorithm":     m["algorithm"],
            "edge":          m["num_servers"].get("EDGE", 0),
            "fog":           m["num_servers"].get("FOG", 0),
            "cloud":         m["num_servers"].get("CLOUD", 0),
            "tasks":         s["total_tasks_completed"],
            "workflows":     s["total_workflows_completed"],
            "avg_makespan":  s["avg_workflow_makespan_ms"],
            "p50_makespan":  s["p50_workflow_makespan_ms"],
            "p95_makespan":  s["p95_workflow_makespan_ms"],
            "cost_usd":      s["total_cost_usd"],
            "energy_wh":     s["total_energy_wh"],
            "throughput":    s["throughput_tasks_per_min"],
            "avg_duration":  s["avg_effective_duration_ms"],
            "file":          exp["_file"],
        })
    return pd.DataFrame(rows)


def algo_label(name):
    return algorithms.label(name)


def save(fig, name):
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURES_DIR / f"{name}.pdf"
    fig.savefig(path, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def color_for(algo):
    return algorithms.color(algo)


def ordered_algos(df, col="algorithm"):
    return algorithms.ordered(df[col].unique().tolist())


def fig_makespan_cdf(wf):
    done = wf[wf["status"] == "DONE"]
    if done.empty:
        print("  [skip] No completed workflows for CDF.")
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for algo in ordered_algos(done):
        vals = done[done["algorithm"] == algo]["makespan_ms"].dropna().sort_values() / 1000
        if vals.empty:
            continue
        cdf = np.linspace(0, 1, len(vals))
        ax.plot(vals, cdf, label=algo_label(algo), color=color_for(algo), linewidth=2)
    ax.set_xlabel("Workflow makespan (s)")
    ax.set_ylabel("Cumulative fraction")
    ax.set_title("Workflow Makespan CDF by Algorithm")
    ax.legend()
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))
    save(fig, "01_makespan_cdf")


def fig_makespan_per_template(wf):
    done = wf[(wf["status"] == "DONE") & (wf["makespan_ms"] > 0)]
    if done.empty:
        print("  [skip] No data for template makespan.")
        return
    templates = sorted(done["template"].unique())
    algos = ordered_algos(done)
    n_algos = len(algos)
    width = 0.8 / n_algos
    fig, ax = plt.subplots(figsize=(8, 5))
    for i, algo in enumerate(algos):
        sub = done[done["algorithm"] == algo]
        positions = [j + (i - n_algos / 2 + 0.5) * width for j in range(len(templates))]
        data = [sub[sub["template"] == t]["makespan_ms"].dropna().values / 1000 for t in templates]
        ax.boxplot(data, positions=positions, widths=width * 0.85,
                   patch_artist=True, notch=False,
                   boxprops=dict(facecolor=color_for(algo), alpha=0.7),
                   medianprops=dict(color="black", linewidth=1.5),
                   whiskerprops=dict(linewidth=1),
                   capprops=dict(linewidth=1),
                   flierprops=dict(marker=".", markersize=3, alpha=0.4))
        ax.bar(0, 0, color=color_for(algo), alpha=0.7, label=algo_label(algo))
    ax.set_xticks(range(len(templates)))
    ax.set_xticklabels([t.replace("_", "\n") for t in templates])
    ax.set_xlabel("Workflow template")
    ax.set_ylabel("Makespan (s)")
    ax.set_title("Workflow Makespan by Template and Algorithm")
    ax.legend(loc="upper right")
    save(fig, "02_makespan_per_template")


def fig_cost_energy(sumdf):
    algos = ordered_algos(sumdf)
    x = np.arange(len(algos))
    fig, ax1 = plt.subplots(figsize=(7, 4.5))
    ax2 = ax1.twinx()
    width = 0.35
    ax1.bar(x - width / 2, [sumdf[sumdf["algorithm"] == a]["cost_usd"].mean() for a in algos],
            width, color=[color_for(a) for a in algos], alpha=0.85, label="Cost (USD)")
    ax2.bar(x + width / 2, [sumdf[sumdf["algorithm"] == a]["energy_wh"].mean() for a in algos],
            width, color=[color_for(a) for a in algos], alpha=0.45, hatch="//", label="Energy (Wh)")
    ax1.set_xticks(x)
    ax1.set_xticklabels([algo_label(a) for a in algos], rotation=15, ha="right")
    ax1.set_ylabel("Total cost (USD)", color="#333333")
    ax2.set_ylabel("Total energy (Wh)", color="#666666")
    ax1.set_title("Total Cost and Energy Consumption by Algorithm")
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")
    save(fig, "03_cost_energy")


def fig_throughput_timeline(tk):
    if tk.empty:
        return
    fig, ax = plt.subplots(figsize=(8, 4.5))
    for algo in ordered_algos(tk):
        sub = tk[tk["algorithm"] == algo].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("end_time")
        t0 = sub["end_time"].min()
        sub["elapsed_s"] = (sub["end_time"] - t0).dt.total_seconds()
        sub["bin"] = (sub["elapsed_s"] // 30).astype(int)
        rate = sub.groupby("bin").size().reset_index(name="count")
        rate["elapsed_s"] = rate["bin"] * 30 + 15
        rate["per_min"] = rate["count"] * (60 / 30)
        ax.plot(rate["elapsed_s"], rate["per_min"],
                label=algo_label(algo), color=color_for(algo), linewidth=2)
    ax.set_xlabel("Elapsed time (s)")
    ax.set_ylabel("Tasks completed per minute")
    ax.set_title("Task Throughput Over Time")
    ax.legend()
    save(fig, "04_throughput_timeline")


def fig_tier_distribution(tk, sumdf):
    algos = ordered_algos(sumdf)
    tiers = ["EDGE", "FOG", "CLOUD"]
    x = np.arange(len(algos))
    width = 0.5
    bottom = np.zeros(len(algos))
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for tier in tiers:
        counts = []
        for algo in algos:
            sub = tk[(tk["algorithm"] == algo) & (tk["server_type"] == tier)]
            counts.append(len(sub))
        counts = np.array(counts, dtype=float)
        ax.bar(x, counts, width, bottom=bottom, label=tier, color=TIER_COLORS[tier], alpha=0.85)
        bottom += counts
    ax.set_xticks(x)
    ax.set_xticklabels([algo_label(a) for a in algos], rotation=15, ha="right")
    ax.set_ylabel("Tasks completed")
    ax.set_title("Task Distribution Across Tiers by Algorithm")
    ax.legend(title="Tier")
    save(fig, "05_tier_distribution")


def fig_multiobjective(sumdf):
    if sumdf.empty:
        return
    fig, ax = plt.subplots(figsize=(7, 5))
    algos = ordered_algos(sumdf)
    for algo in algos:
        sub = sumdf[sumdf["algorithm"] == algo]
        for _, row in sub.iterrows():
            ms = row["avg_makespan"] / 1000
            cost = row["cost_usd"]
            energy = max(row["energy_wh"], 1)
            ax.scatter(ms, cost, s=energy * 3, color=color_for(algo),
                       alpha=0.75, edgecolors="white", linewidth=0.5)
        ax.scatter([], [], s=80, color=color_for(algo), label=algo_label(algo), alpha=0.85)
    ax.set_xlabel("Avg workflow makespan (s)")
    ax.set_ylabel("Total cost (USD)")
    ax.set_title("Multi-objective Trade-off\n(bubble size proportional to energy)")
    ax.legend(title="Algorithm")
    save(fig, "06_multiobjective")


def fig_latency_bars(sumdf):
    algos = ordered_algos(sumdf)
    x = np.arange(len(algos))
    p50 = [sumdf[sumdf["algorithm"] == a]["p50_makespan"].mean() / 1000 for a in algos]
    p95 = [sumdf[sumdf["algorithm"] == a]["p95_makespan"].mean() / 1000 for a in algos]
    width = 0.35
    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.bar(x - width / 2, p50, width, label="P50 makespan",
           color=[color_for(a) for a in algos], alpha=0.85)
    ax.bar(x + width / 2, p95, width, label="P95 makespan",
           color=[color_for(a) for a in algos], alpha=0.45, hatch="//")
    ax.set_xticks(x)
    ax.set_xticklabels([algo_label(a) for a in algos], rotation=15, ha="right")
    ax.set_ylabel("Workflow makespan (s)")
    ax.set_title("P50 and P95 Workflow Makespan by Algorithm")
    ax.legend()
    save(fig, "07_latency_percentiles")


def main():
    paths = resolve_input_paths(sys.argv[1:])

    print(f"Loading {len(paths)} experiment file(s)...")
    experiments = load_results(paths)

    tk = tasks_df(experiments)
    wf = workflows_df(experiments)
    sumdf = summary_df(experiments)

    algos = ordered_algos(sumdf)
    print(f"Algorithms: {algos}")
    if len(algos) < 2:
        print("  [note] Only one algorithm was loaded. Pass multiple result files to generate comparison plots.")
    print(f"Total task results: {len(tk)}")
    print(f"Total workflow results: {len(wf)}")
    print(f"Generating figures in {FIGURES_DIR}/")

    fig_makespan_cdf(wf)
    fig_makespan_per_template(wf)
    fig_cost_energy(sumdf)
    fig_throughput_timeline(tk)
    fig_tier_distribution(tk, sumdf)
    fig_multiobjective(sumdf)
    fig_latency_bars(sumdf)

    print("Done.")


if __name__ == "__main__":
    main()

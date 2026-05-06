#!/usr/bin/env python3
"""
plot_results.py — Generate comparison graphs from a benchmark run.

Usage:
    python scripts/plot_results.py [results/benchmark_YYYYMMDD_HHMMSS]

If no directory is given, uses the most recent results/benchmark_* directory.
Outputs PDFs and PNGs to <dir>/plots/.

Graphs produced:
  1.  throughput.pdf/png            Tasks/min per algorithm
  2.  tasks_completed.pdf/png       Total tasks completed
  3.  makespan.pdf/png              Avg/P50/P95 workflow makespan
  4.  response_time_cdf.pdf/png     CDF of task effective duration
  5.  cost.pdf/png                  Total cost and cost-per-task
  6.  energy.pdf/png                Total energy and energy-per-task
  7.  load_balance.pdf/png          Box plot of completed tasks per server
  8.  tier_distribution.pdf/png     Stacked bar of tasks by tier
  9.  tier_duration_heatmap.pdf/png Avg task duration by algorithm × tier
"""

import json
import sys
import os
import glob

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALGORITHMS = ["round_robin", "random", "best_fit", "worst_fit", "topsis"]

ALGO_LABELS = {
    "round_robin": "Round Robin",
    "random":      "Random",
    "best_fit":    "Best Fit",
    "worst_fit":   "Worst Fit",
    "topsis":      "TOPSIS",
}

COLORS = {
    "round_robin": "#4C72B0",
    "random":      "#DD8452",
    "best_fit":    "#55A868",
    "worst_fit":   "#C44E52",
    "topsis":      "#8172B2",
}

TIERS = ["EDGE", "FOG", "CLOUD"]
TIER_COLORS = {"EDGE": "#E8B94F", "FOG": "#5DADE2", "CLOUD": "#58D68D"}

plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        11,
    "axes.titlesize":   12,
    "axes.labelsize":   11,
    "legend.fontsize":  10,
    "figure.dpi":       150,
})


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def find_latest_benchmark():
    dirs = sorted(glob.glob("results/benchmark_*"), reverse=True)
    if not dirs:
        raise FileNotFoundError(
            "No results/benchmark_* directory found. Run cmd/benchmark first."
        )
    return dirs[0]


def load_results(directory):
    """Return dict[algo] -> parsed JSON for every *.json file in directory."""
    data = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        with open(path) as f:
            result = json.load(f)
        algo = result["meta"]["algorithm"]
        data[algo] = result
    if not data:
        raise ValueError(f"No JSON files found in {directory}")
    return data


def present_algos(data):
    """Return algorithms in canonical order, limited to those present in data."""
    return [a for a in ALGORITHMS if a in data]


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------

def save(fig, out_dir, name):
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Graph 1: Throughput
# ---------------------------------------------------------------------------

def plot_throughput(data, out_dir):
    algos  = present_algos(data)
    values = [data[a]["summary"]["throughput_tasks_per_min"] for a in algos]
    labels = [ALGO_LABELS[a] for a in algos]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(labels, values, color=[COLORS[a] for a in algos],
                  width=0.55, edgecolor="white", linewidth=0.8)
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=9)
    ax.set_ylabel("Tasks / minute")
    ax.set_title("Throughput")
    ax.set_ylim(0, max(values) * 1.25 if values else 1)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "throughput")


# ---------------------------------------------------------------------------
# Graph 2: Total tasks completed
# ---------------------------------------------------------------------------

def plot_tasks_completed(data, out_dir):
    algos  = present_algos(data)
    values = [data[a]["summary"]["total_tasks_completed"] for a in algos]
    labels = [ALGO_LABELS[a] for a in algos]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(labels, values, color=[COLORS[a] for a in algos],
                  width=0.55, edgecolor="white", linewidth=0.8)
    ax.bar_label(bars, padding=3, fontsize=9)
    ax.set_ylabel("Tasks completed")
    ax.set_title("Total Tasks Completed")
    ax.set_ylim(0, max(values) * 1.25 if values else 1)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "tasks_completed")


# ---------------------------------------------------------------------------
# Graph 3: Makespan (avg / P50 / P95)
# ---------------------------------------------------------------------------

def plot_makespan(data, out_dir):
    algos  = present_algos(data)
    avg    = [data[a]["summary"]["avg_workflow_makespan_ms"] / 1000 for a in algos]
    p50    = [data[a]["summary"]["p50_workflow_makespan_ms"] / 1000 for a in algos]
    p95    = [data[a]["summary"]["p95_workflow_makespan_ms"] / 1000 for a in algos]
    labels = [ALGO_LABELS[a] for a in algos]

    x     = np.arange(len(algos))
    width = 0.25

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(x - width, avg, width, label="Avg", color="#4C72B0", edgecolor="white")
    ax.bar(x,         p50, width, label="P50", color="#55A868", edgecolor="white")
    ax.bar(x + width, p95, width, label="P95", color="#C44E52", edgecolor="white")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Makespan (s)")
    ax.set_title("Workflow Makespan Distribution")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "makespan")


# ---------------------------------------------------------------------------
# Graph 4: Response time CDF
# ---------------------------------------------------------------------------

def plot_response_time_cdf(data, out_dir):
    algos = present_algos(data)
    fig, ax = plt.subplots(figsize=(8, 5))

    for algo in algos:
        durations = [
            t["effective_duration_ms"]
            for t in data[algo].get("task_results", [])
            if t.get("effective_duration_ms", 0) > 0
        ]
        if not durations:
            continue
        xs = np.sort(durations)
        ys = np.linspace(0, 1, len(xs))
        ax.plot(xs / 1000, ys, label=ALGO_LABELS[algo], color=COLORS[algo], linewidth=1.8)

    ax.set_xlabel("Effective duration (s)")
    ax.set_ylabel("CDF")
    ax.set_title("Response Time CDF")
    ax.legend()
    ax.set_ylim(0, 1.05)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "response_time_cdf")


# ---------------------------------------------------------------------------
# Graph 5: Cost (total + per-task)
# ---------------------------------------------------------------------------

def plot_cost(data, out_dir):
    algos    = present_algos(data)
    total    = [data[a]["summary"]["total_cost_usd"] for a in algos]
    counts   = [data[a]["summary"]["total_tasks_completed"] for a in algos]
    per_task = [t / max(c, 1) * 1000 for t, c in zip(total, counts)]  # milli-USD
    labels   = [ALGO_LABELS[a] for a in algos]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    bars1 = ax1.bar(labels, total, color=[COLORS[a] for a in algos],
                    width=0.55, edgecolor="white")
    ax1.bar_label(bars1, fmt="$%.4f", padding=3, fontsize=8)
    ax1.set_ylabel("Total cost (USD)")
    ax1.set_title("Total Cost")
    ax1.spines[["top", "right"]].set_visible(False)

    bars2 = ax2.bar(labels, per_task, color=[COLORS[a] for a in algos],
                    width=0.55, edgecolor="white")
    ax2.bar_label(bars2, fmt="%.2f", padding=3, fontsize=8)
    ax2.set_ylabel("Cost per task (mUSD)")
    ax2.set_title("Cost per Task")
    ax2.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    save(fig, out_dir, "cost")


# ---------------------------------------------------------------------------
# Graph 6: Energy (total + per-task)
# ---------------------------------------------------------------------------

def plot_energy(data, out_dir):
    algos    = present_algos(data)
    total    = [data[a]["summary"]["total_energy_wh"] for a in algos]
    counts   = [data[a]["summary"]["total_tasks_completed"] for a in algos]
    per_task = [e / max(c, 1) * 1000 for e, c in zip(total, counts)]  # mWh
    labels   = [ALGO_LABELS[a] for a in algos]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.5))

    bars1 = ax1.bar(labels, total, color=[COLORS[a] for a in algos],
                    width=0.55, edgecolor="white")
    ax1.bar_label(bars1, fmt="%.4f", padding=3, fontsize=8)
    ax1.set_ylabel("Total energy (Wh)")
    ax1.set_title("Total Energy Consumption")
    ax1.spines[["top", "right"]].set_visible(False)

    bars2 = ax2.bar(labels, per_task, color=[COLORS[a] for a in algos],
                    width=0.55, edgecolor="white")
    ax2.bar_label(bars2, fmt="%.2f", padding=3, fontsize=8)
    ax2.set_ylabel("Energy per task (mWh)")
    ax2.set_title("Energy per Task")
    ax2.spines[["top", "right"]].set_visible(False)

    fig.tight_layout()
    save(fig, out_dir, "energy")


# ---------------------------------------------------------------------------
# Graph 7: Load balance (box plot of tasks-per-server)
# ---------------------------------------------------------------------------

def plot_load_balance(data, out_dir):
    algos  = present_algos(data)
    series = []
    labels = []
    for algo in algos:
        counts = [
            srv["completed_tasks"]
            for srv in data[algo].get("servers", [])
            if srv.get("completed_tasks", 0) > 0
        ]
        if counts:
            series.append(counts)
            labels.append(ALGO_LABELS[algo])

    if not series:
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    bp = ax.boxplot(series, tick_labels=labels, patch_artist=True, widths=0.5,
                    medianprops={"color": "black", "linewidth": 1.5})
    present = [a for a in algos if a in data and
               any(s.get("completed_tasks", 0) > 0 for s in data[a].get("servers", []))]
    for patch, algo in zip(bp["boxes"], present):
        patch.set_facecolor(COLORS[algo])
        patch.set_alpha(0.7)
    ax.set_ylabel("Tasks completed per server")
    ax.set_title("Load Balance across Servers")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "load_balance")


# ---------------------------------------------------------------------------
# Graph 8: Per-tier task distribution (stacked bar)
# ---------------------------------------------------------------------------

def plot_tier_distribution(data, out_dir):
    algos  = present_algos(data)
    labels = [ALGO_LABELS[a] for a in algos]
    x      = np.arange(len(algos))
    width  = 0.55

    fig, ax = plt.subplots(figsize=(8, 5))
    bottom = np.zeros(len(algos))
    for tier in TIERS:
        values = np.array([
            data[a]["summary"]["by_tier"].get(tier, {}).get("tasks_completed", 0)
            for a in algos
        ], dtype=float)
        ax.bar(x, values, width, label=tier, bottom=bottom,
               color=TIER_COLORS[tier], edgecolor="white", linewidth=0.8)
        bottom += values

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Tasks completed")
    ax.set_title("Task Distribution by Tier")
    ax.legend(title="Tier")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "tier_distribution")


# ---------------------------------------------------------------------------
# Graph 9: Avg task duration heatmap (algorithm × tier)
# ---------------------------------------------------------------------------

def plot_tier_duration_heatmap(data, out_dir):
    algos  = present_algos(data)
    matrix = np.zeros((len(algos), len(TIERS)))
    for i, algo in enumerate(algos):
        for j, tier in enumerate(TIERS):
            ms = data[algo]["summary"]["by_tier"].get(tier, {}).get("avg_effective_duration_ms", 0)
            matrix[i, j] = ms / 1000  # seconds

    fig, ax = plt.subplots(figsize=(6, len(algos) * 0.9 + 1.5))
    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(TIERS)))
    ax.set_xticklabels(TIERS)
    ax.set_yticks(range(len(algos)))
    ax.set_yticklabels([ALGO_LABELS[a] for a in algos])
    vmax = matrix.max()
    for i in range(len(algos)):
        for j in range(len(TIERS)):
            color = "white" if matrix[i, j] > vmax * 0.7 else "black"
            ax.text(j, i, f"{matrix[i, j]:.1f}s", ha="center", va="center",
                    fontsize=9, color=color)
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("Avg duration (s)")
    ax.set_title("Avg Task Duration by Algorithm × Tier")
    fig.tight_layout()
    save(fig, out_dir, "tier_duration_heatmap")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) > 1:
        bench_dir = sys.argv[1]
    else:
        bench_dir = find_latest_benchmark()

    print(f"Reading results from: {bench_dir}")
    data = load_results(bench_dir)
    print(f"Loaded algorithms:    {list(data.keys())}")

    out_dir = os.path.join(bench_dir, "plots")
    os.makedirs(out_dir, exist_ok=True)
    print(f"Writing plots to:     {out_dir}")

    plot_throughput(data, out_dir)
    plot_tasks_completed(data, out_dir)
    plot_makespan(data, out_dir)
    plot_response_time_cdf(data, out_dir)
    plot_cost(data, out_dir)
    plot_energy(data, out_dir)
    plot_load_balance(data, out_dir)
    plot_tier_distribution(data, out_dir)
    plot_tier_duration_heatmap(data, out_dir)

    print(f"\nDone. 9 graphs saved to {out_dir}/")
    print("Each graph is available as .pdf (for LaTeX) and .png (for preview).")


if __name__ == "__main__":
    main()

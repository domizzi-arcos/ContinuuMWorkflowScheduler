#!/usr/bin/env python3
"""
plot_results.py — Per-configuration visualization for a single benchmark run.

Purpose
-------
Generates the nine canonical per-config figures used in the thesis to compare
scheduling algorithms within a *single* infrastructure configuration. This is
the "leaf" plotter: one benchmark directory in, nine PDF/PNG pairs out. For
cross-configuration aggregation (scalability curves, winner matrices, TOPSIS
sensitivity, etc.) see ``grid_plot.py``.

Input
-----
Reads ``*.json`` files from a directory ``results/benchmark_YYYYMMDD_HHMMSS/``
(or the user-supplied directory). Each JSON is one ``ExperimentResult`` as
serialized by ``internal/results/exporter.go``. The relevant schema fragments
consumed here are::

    {
      "meta":    { "algorithm": "round_robin", "timestamp": "...", ... },
      "summary": {                                # ExperimentSummary
        "total_tasks_completed":      int,
        "throughput_tasks_per_min":   float,
        "avg_workflow_makespan_ms":   float,
        "p50_workflow_makespan_ms":   float,
        "p95_workflow_makespan_ms":   float,
        "total_cost_usd":             float,
        "total_energy_wh":            float,
        "by_tier": {                              # ByTier map
          "EDGE":  { "tasks_completed": int, "avg_effective_duration_ms": float, ... },
          "FOG":   { ... },
          "CLOUD": { ... }
        }
      },
      "task_results": [ { "effective_duration_ms": float, ... }, ... ],
      "servers":      [ { "completed_tasks": int, ... }, ... ]
    }

One JSON file per scheduling algorithm is expected; the algorithm key is taken
from ``meta.algorithm``.

Output
------
PDF + PNG pairs are written to ``<input_dir>/plots/``:

  1. throughput.pdf/png             Bar: tasks/min per algorithm.
  2. tasks_completed.pdf/png        Bar: total tasks completed per algorithm.
  3. makespan.pdf/png               Grouped bar: avg / P50 / P95 makespan (s).
  4. response_time_cdf.pdf/png      Empirical CDF of per-task effective duration.
  5. cost.pdf/png                   Two-panel bar: total cost + cost-per-task.
  6. energy.pdf/png                 Two-panel bar: total energy + energy-per-task.
  7. load_balance.pdf/png           Box plot of completed-tasks per server.
  8. tier_distribution.pdf/png      Stacked bar of tasks by tier (EDGE/FOG/CLOUD).
  9. tier_duration_heatmap.pdf/png  Heatmap of avg task duration (algorithm × tier).

Relationship to grid_plot.py
----------------------------
``plot_results.py`` is *intra-configuration*: it answers "which algorithm wins
under this exact topology+capacity?". ``grid_plot.py`` is *inter-configuration*:
it aggregates many such runs across topologies, capacity profiles, and TOPSIS
weight variants. ``grid_plot.py`` actually invokes this script as a subprocess
to (re)generate per-config plots that are missing under a grid directory.

Usage
-----
    python scripts/plot_results.py [results/benchmark_YYYYMMDD_HHMMSS]

If no directory is given, the most recent ``results/benchmark_*`` is used.
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

import algorithms

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Canonical algorithm order: drives bar order, legend order, and color keys.
# Any algorithm absent from a given benchmark is filtered out by present_algos.
# Names, labels and colours come from scripts/algorithms.py.
ALGORITHMS  = algorithms.ALL
ALGO_LABELS = algorithms.LABELS
COLORS      = algorithms.COLORS

# Three-tier infrastructure ordering. EDGE→FOG→CLOUD is the conventional bottom-up
# ordering used for stacked tier-distribution bars and heatmap columns.
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
    """Locate the most recent benchmark output directory.

    Globs ``results/benchmark_*`` relative to the current working directory
    (expected to be ``distributed-workflow-scheduler/``) and returns the
    lexicographically last entry. Because the suffix is ``YYYYMMDD_HHMMSS``,
    lexicographic order matches chronological order.

    Returns:
        str: Path to the latest ``results/benchmark_YYYYMMDD_HHMMSS/`` dir.

    Raises:
        FileNotFoundError: If no ``results/benchmark_*`` directory exists.
    """
    dirs = sorted(glob.glob("results/benchmark_*"), reverse=True)
    if not dirs:
        raise FileNotFoundError(
            "No results/benchmark_* directory found. Run cmd/benchmark first."
        )
    return dirs[0]


def load_results(directory):
    """Load every ``ExperimentResult`` JSON in ``directory`` keyed by algorithm.

    Iterates over ``*.json`` files (one per algorithm, written by the Go
    exporter) and parses each into a Python dict. The key is taken from
    ``result["meta"]["algorithm"]`` — so files can be named arbitrarily.

    Args:
        directory: Path containing per-algorithm benchmark JSONs.

    Returns:
        dict: Mapping ``{algorithm_name: ExperimentResult dict}``.

    Raises:
        ValueError: If ``directory`` contains no JSON files.
    """
    data = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        with open(path) as f:
            result = json.load(f)
        # meta.algorithm is the canonical algorithm key (e.g. "topsis"); see
        # internal/results/exporter.go::ExperimentMeta.
        algo = result["meta"]["algorithm"]
        data[algo] = result
    if not data:
        raise ValueError(f"No JSON files found in {directory}")
    return data


def present_algos(data):
    """Filter and re-order the algorithms found in ``data``.

    Used to render bars/lines in a stable order regardless of file-system
    iteration order, and to silently skip algorithms not run in this benchmark.

    Args:
        data: Output of :func:`load_results`.

    Returns:
        list[str]: Subset of :data:`ALGORITHMS` actually present, in canonical order.
    """
    return [a for a in ALGORITHMS if a in data]


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------

def save(fig, out_dir, name):
    """Persist ``fig`` to disk as both PDF (for LaTeX) and PNG (for preview).

    Args:
        fig: Matplotlib ``Figure`` to save.
        out_dir: Destination directory (must already exist).
        name: Filename stem; extensions ``.pdf`` and ``.png`` are appended.

    Returns:
        None. Side effect: writes two files and closes the figure to release memory.
    """
    for ext in ("pdf", "png"):
        # bbox_inches="tight" trims surplus whitespace, important because thesis
        # figures are placed in fixed \includegraphics widths.
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Graph 1: Throughput
# ---------------------------------------------------------------------------

def plot_throughput(data, out_dir):
    """Bar chart of completed tasks per minute per algorithm.

    Reads ``summary.throughput_tasks_per_min`` (already pre-computed by the Go
    exporter as ``total_tasks_completed / experiment_seconds * 60``). No
    additional aggregation here — one value per algorithm.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``throughput.pdf`` + ``throughput.png``.
    """
    algos  = present_algos(data)
    # Single scalar per algorithm; throughput is already a rate (tasks/min).
    values = [data[a]["summary"]["throughput_tasks_per_min"] for a in algos]
    labels = [ALGO_LABELS[a] for a in algos]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(labels, values, color=[COLORS[a] for a in algos],
                  width=0.55, edgecolor="white", linewidth=0.8)
    ax.bar_label(bars, fmt="%.1f", padding=3, fontsize=9)
    ax.set_ylabel("Tasks / minute")
    ax.set_title("Throughput")
    # 25% head-room so the bar-label fmt="%.1f" never collides with the top spine.
    ax.set_ylim(0, max(values) * 1.25 if values else 1)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "throughput")


# ---------------------------------------------------------------------------
# Graph 2: Total tasks completed
# ---------------------------------------------------------------------------

def plot_tasks_completed(data, out_dir):
    """Bar chart of absolute task throughput (total completed) per algorithm.

    Complementary to :func:`plot_throughput`: same ranking but in absolute
    counts rather than a normalized rate — useful when experiment durations
    differ across runs.

    Reads ``summary.total_tasks_completed``.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``tasks_completed.pdf`` + ``tasks_completed.png``.
    """
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
    """Grouped bar chart of workflow makespan: mean, median (P50), 95th percentile.

    Makespan is the wall-clock duration from a workflow's first task being
    submitted to its last task completing. Showing avg + P50 + P95 exposes
    both central tendency and tail behaviour (a long-tail-prone algorithm
    will have P95 ≫ P50 even when averages look comparable).

    Reads from ``summary``:
      - ``avg_workflow_makespan_ms``  → mean
      - ``p50_workflow_makespan_ms``  → median
      - ``p95_workflow_makespan_ms``  → 95th percentile

    All three are precomputed in Go from the per-workflow makespan series.
    Here they are just converted from ms to s for display.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``makespan.pdf`` + ``makespan.png``.
    """
    algos  = present_algos(data)
    # ms → s for readable axis labels (workflow makespans are typically seconds).
    avg    = [data[a]["summary"]["avg_workflow_makespan_ms"] / 1000 for a in algos]
    p50    = [data[a]["summary"]["p50_workflow_makespan_ms"] / 1000 for a in algos]
    p95    = [data[a]["summary"]["p95_workflow_makespan_ms"] / 1000 for a in algos]
    labels = [ALGO_LABELS[a] for a in algos]

    x     = np.arange(len(algos))
    # 3 bars per algorithm group; width 0.25 leaves a small gap between groups.
    width = 0.25

    fig, ax = plt.subplots(figsize=(9, 5))
    # Left/center/right offsets within each group: Avg | P50 | P95.
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
    """Empirical CDF of per-task effective duration, one curve per algorithm.

    Plots the empirical cumulative distribution F(x) = P(duration ≤ x). A curve
    that rises faster (more to the left) indicates better response times. The
    long-tail crossover point between algorithms is the typical insight here.

    Reads ``task_results[*].effective_duration_ms`` (the per-execution wall
    clock duration including queueing). Filters out non-positive entries
    (defensive: zero usually means an unfinished/aborted task).

    Aggregation: no aggregation across runs; this is the full empirical
    distribution. The CDF is built by sorting durations and assigning each its
    rank/N — no kernel smoothing, no binning.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``response_time_cdf.pdf`` + ``response_time_cdf.png``.
    """
    algos = present_algos(data)
    fig, ax = plt.subplots(figsize=(8, 5))

    for algo in algos:
        # Collect raw per-task durations (ms). Skip zeros/missing to avoid
        # contaminating the CDF with unfinished tasks.
        durations = [
            t["effective_duration_ms"]
            for t in data[algo].get("task_results", [])
            if t.get("effective_duration_ms", 0) > 0
        ]
        if not durations:
            continue
        # Empirical CDF: sort + uniform [0,1] y-axis ranks.
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
    """Two-panel cost figure: absolute total cost and normalized per-task cost.

    Left panel: ``summary.total_cost_usd`` (raw experiment cost).
    Right panel: ``total_cost_usd / max(total_tasks_completed, 1) * 1000``,
    expressed in milli-USD so that small per-task costs render with enough
    significant digits. The ``max(c, 1)`` guard avoids division-by-zero when
    a benchmark completed no tasks.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``cost.pdf`` + ``cost.png``.
    """
    algos    = present_algos(data)
    total    = [data[a]["summary"]["total_cost_usd"] for a in algos]
    counts   = [data[a]["summary"]["total_tasks_completed"] for a in algos]
    # Per-task cost in milli-USD. *1000 promotes typical values into a 0.1-100 range.
    # max(c, 1) prevents ZeroDivision when an algorithm completed zero tasks.
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
    """Two-panel energy figure: total Wh and normalized mWh-per-task.

    Mirrors :func:`plot_cost` but for the energy criterion. The exporter sums
    ``server.energy_draw_w * task.duration_seconds`` (or a sleep-aware variant)
    across all completed tasks to produce ``summary.total_energy_wh``.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``energy.pdf`` + ``energy.png``.
    """
    algos    = present_algos(data)
    total    = [data[a]["summary"]["total_energy_wh"] for a in algos]
    counts   = [data[a]["summary"]["total_tasks_completed"] for a in algos]
    # Wh → mWh per task (same *1000 normalization rationale as cost).
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
    """Box plot of ``completed_tasks`` per server, one box per algorithm.

    The box plot is the diagnostic for load skew: a tightly bunched box
    indicates the algorithm spread work evenly; long whiskers / outliers
    indicate hotspots. Idle servers (completed_tasks == 0) are excluded so
    that a wide infrastructure with many idle nodes does not artificially
    inflate the lower whisker — the question being answered is "among
    *participating* servers, how balanced was the load?".

    Reads ``servers[*].completed_tasks``.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``load_balance.pdf`` + ``load_balance.png`` (or no-ops
        if every algorithm had only idle servers — defensive guard).
    """
    algos  = present_algos(data)
    series = []
    labels = []
    for algo in algos:
        # Per-server task counts; skip idle servers so the box reflects only
        # nodes that actually received load under this algorithm.
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
    # Rebuild the algorithm order matching the boxes we actually drew so we can
    # colour each box with its algorithm's signature colour.
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
    """Stacked bar of tasks completed per tier, one stack per algorithm.

    Visualises *where* each algorithm ran its tasks. EDGE→FOG→CLOUD stack
    order is fixed by :data:`TIERS`; ``bottom`` accumulates as we go so each
    new tier sits on top of the previous.

    Reads ``summary.by_tier[TIER].tasks_completed`` (the ByTier map produced
    by ``ExperimentSummary``). Missing tiers default to 0.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``tier_distribution.pdf`` + ``tier_distribution.png``.
    """
    algos  = present_algos(data)
    labels = [ALGO_LABELS[a] for a in algos]
    x      = np.arange(len(algos))
    width  = 0.55

    fig, ax = plt.subplots(figsize=(8, 5))
    # Stack EDGE, then FOG on top, then CLOUD on top — running sum in `bottom`.
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
    """Heatmap of average effective task duration across algorithm × tier.

    Rows: algorithms. Columns: tiers (EDGE / FOG / CLOUD). Each cell shows
    ``summary.by_tier[TIER].avg_effective_duration_ms / 1000`` (seconds), the
    mean wall-clock duration of tasks routed by ``algorithm`` to ``tier``.

    Annotation contrast: cells whose value exceeds 70% of the matrix max get
    white text (because the YlOrRd colormap is dark there); otherwise black.

    Args:
        data: Output of :func:`load_results`.
        out_dir: Destination directory for the figure.

    Returns:
        None. Writes ``tier_duration_heatmap.pdf`` + ``tier_duration_heatmap.png``.
    """
    algos  = present_algos(data)
    matrix = np.zeros((len(algos), len(TIERS)))
    # Build (algo × tier) matrix of mean per-tier task duration in seconds.
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
    # Annotate each cell with its value. Text color flips to white on the dark
    # end of YlOrRd (>70% of max) to keep labels legible.
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
    """CLI entry point: resolve input dir, load JSONs, emit 9 figures.

    Args:
        None (reads ``sys.argv[1]`` or falls back to the most recent benchmark).

    Returns:
        None. Side effects: prints progress to stdout, writes 9*2 = 18 files
        (PDF + PNG each) under ``<bench_dir>/plots/``.
    """
    if len(sys.argv) > 1:
        bench_dir = sys.argv[1]
    else:
        # No argument: pick the freshest results/benchmark_* directory.
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

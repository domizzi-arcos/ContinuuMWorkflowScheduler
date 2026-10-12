"""Paper figures from the tidy tables (vector PDF, IEEE column widths).

Colours, markers and labels come from scripts/algorithms.py. Every figure
with more than three policies uses markers or text as well as colour, since
eight hues cannot all be told apart pairwise (see algorithms.py).
"""

from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402

import algorithms  # noqa: E402
import stats  # noqa: E402

SINGLE_COL_IN = 3.5
DOUBLE_COL_IN = 7.16
FONT_PT = 7
TIER_COLORS = {"EDGE": "#86b6ef", "FOG": "#2a78d6", "CLOUD": "#104281"}  # one-hue ordinal ramp
CAPACITIES = ["small", "medium", "large"]

plt.rcParams.update({
    "font.size": FONT_PT, "axes.titlesize": FONT_PT + 1, "axes.labelsize": FONT_PT,
    "xtick.labelsize": FONT_PT, "ytick.labelsize": FONT_PT, "legend.fontsize": FONT_PT,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.alpha": 0.25, "grid.linewidth": 0.5,
    "lines.linewidth": 1.0, "pdf.fonttype": 42, "ps.fonttype": 42,
    "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
    # Deterministic PDF output for identical inputs.
    "svg.hashsalt": "eval-v2", "path.simplify": False,
})


def _save(fig, out_dir: str, name: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{name}.pdf")
    fig.savefig(path, metadata={"CreationDate": None, "ModDate": None, "Producer": None, "Creator": None})
    plt.close(fig)
    return path


def winner_matrix(runs: pd.DataFrame, out_dir: str) -> str:
    """Three panels (cost, energy, makespan): rows topologies, columns
    capacities; each cell shows the policy with the lowest seed-mean (its
    abbreviation, coloured), or "tie"."""
    metrics = list(stats.METRICS)
    cm = stats.config_means(runs, [stats.METRICS[m] for m in metrics])
    topos = sorted(cm["topology"].unique())
    caps = [c for c in CAPACITIES if c in set(cm["capacity"])]
    fig, axes = plt.subplots(1, 3, figsize=(DOUBLE_COL_IN, 0.25 * len(topos) + 0.8), sharey=True)
    for ax, m in zip(axes, metrics):
        col = stats.METRICS[m]
        ax.set_xlim(-0.5, len(caps) - 0.5)
        ax.set_ylim(len(topos) - 0.5, -0.5)
        for r, topo in enumerate(topos):
            for c, cap in enumerate(caps):
                g = cm[(cm.topology == topo) & (cm.capacity == cap)]
                if g.empty:
                    continue
                best = g[col].min()
                winners = g.loc[np.isclose(g[col], best, rtol=1e-12, atol=0), "algorithm"].tolist()
                if len(winners) == 1:
                    a = winners[0]
                    ax.add_patch(plt.Rectangle((c - 0.48, r - 0.45), 0.96, 0.9, color=algorithms.color(a), alpha=0.25, lw=0))
                    ax.text(c, r, algorithms.ABBREV.get(a, a), ha="center", va="center", fontsize=FONT_PT)
                else:
                    ax.text(c, r, "tie", ha="center", va="center", fontsize=FONT_PT, color="#52514e")
        ax.set_xticks(range(len(caps)), [c.capitalize() for c in caps])
        ax.set_yticks(range(len(topos)), topos)
        ax.grid(False)
        ax.set_title(f"Lowest {m}")
    present = algorithms.ordered(cm["algorithm"].unique())
    fig.legend(handles=[Patch(color=algorithms.color(a), alpha=0.4, label=f"{algorithms.ABBREV.get(a, a)} = {algorithms.label(a)}")
                        for a in present], loc="lower center", ncol=5, frameon=False, bbox_to_anchor=(0.5, -0.12))
    return _save(fig, out_dir, "winner_matrix")


def cost_vs_makespan(runs: pd.DataFrame, out_dir: str) -> str:
    """Configuration means of cost against makespan, one marker per policy."""
    cm = stats.config_means(runs, [stats.METRICS["cost"], stats.METRICS["makespan"]])
    fig, ax = plt.subplots(figsize=(SINGLE_COL_IN, 2.6))
    for a in algorithms.ordered(cm["algorithm"].unique()):
        g = cm[cm.algorithm == a]
        hollow = a in algorithms.HOLLOW
        ax.scatter(g[stats.METRICS["makespan"]] / 1000, g[stats.METRICS["cost"]],
                   marker=algorithms.MARKERS.get(a, "o"), s=18, linewidths=0.8,
                   facecolors="none" if hollow else algorithms.color(a),
                   edgecolors=algorithms.color(a), label=algorithms.label(a))
    ax.set_xlabel("Mean workflow makespan (s)")
    ax.set_ylabel("Total cost per run (USD)")
    ax.legend(ncol=2, frameon=False, markerscale=1.2, handletextpad=0.2, columnspacing=0.8)
    return _save(fig, out_dir, "cost_vs_makespan")


def tier_share_bars(runs: pd.DataFrame, out_dir: str) -> str:
    """Stacked bars of the mean share of tasks per tier, per policy."""
    ts = stats.tier_share(runs)
    order = algorithms.ordered(ts.index)
    fig, ax = plt.subplots(figsize=(SINGLE_COL_IN, 2.2))
    bottom = np.zeros(len(order))
    for tier in stats.TIERS:
        vals = ts.loc[order, f"share_{tier}_mean"].to_numpy()
        ax.bar(range(len(order)), vals, bottom=bottom, color=TIER_COLORS[tier], label=tier.capitalize(),
               edgecolor="white", linewidth=0.8, width=0.75)
        bottom += vals
    ax.set_xticks(range(len(order)), [algorithms.ABBREV.get(a, a) for a in order])
    ax.set_ylabel("Share of tasks (%)")
    ax.set_ylim(0, 100)
    ax.legend(ncol=3, frameon=False, loc="lower center", bbox_to_anchor=(0.5, 1.0))
    return _save(fig, out_dir, "tier_share")


def per_task_vs_nodes(runs: pd.DataFrame, out_dir: str) -> str:
    """Cost and energy per completed task against total node count (two
    panels, one y-axis each), one line per policy."""
    df = runs.copy()
    df["cost_per_task"] = df["total_cost_usd"] / df["total_tasks_completed"]
    df["energy_per_task"] = df["total_energy_wh"] / df["total_tasks_completed"]
    g = df.groupby(["algorithm", "num_nodes"])[["cost_per_task", "energy_per_task"]].mean().reset_index()
    fig, axes = plt.subplots(1, 2, figsize=(DOUBLE_COL_IN, 2.2))
    for ax, col, lab in ((axes[0], "cost_per_task", "Cost per task (USD)"),
                         (axes[1], "energy_per_task", "Energy per task (Wh)")):
        for a in algorithms.ordered(g["algorithm"].unique()):
            s = g[g.algorithm == a].sort_values("num_nodes")
            ax.plot(s["num_nodes"], s[col], color=algorithms.color(a), linestyle=algorithms.LINESTYLES.get(a, "-"),
                    marker=algorithms.MARKERS.get(a, "o"), markersize=3.5,
                    markerfacecolor="none" if a in algorithms.HOLLOW else algorithms.color(a),
                    label=algorithms.label(a))
        ax.set_xlabel("Total nodes")
        ax.set_ylabel(lab)
    axes[1].legend(ncol=2, frameon=False, fontsize=FONT_PT - 0.5, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    return _save(fig, out_dir, "per_task_vs_nodes")


def all_figures(runs: pd.DataFrame, out_dir: str) -> list[str]:
    return [winner_matrix(runs, out_dir), cost_vs_makespan(runs, out_dir),
            tier_share_bars(runs, out_dir), per_task_vs_nodes(runs, out_dir)]

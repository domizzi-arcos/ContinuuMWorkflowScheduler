#!/usr/bin/env python3
"""
grid_plot.py — Cross-configuration comparative plots for a grid benchmark.

Purpose
-------
Aggregates results across every (topology × capacity × algorithm [× TOPSIS
weight]) combination of a grid sweep and produces the ten cross-config thesis
figures. Unlike ``plot_results.py`` (which only compares algorithms inside a
single benchmark directory), this script collapses an entire grid into
scalability curves, topology comparisons, capacity-effect studies, TOPSIS
weight sensitivity, an algorithm-winner matrix, and a tier-ratio breakdown.

Multi-seed aggregation
----------------------
When a grid is run with multiple seeds, the manifest contains one run per
``(combo, seed)``. :func:`_aggregate_results` collapses those N per-seed
``ExperimentResult`` JSONs into a single result whose ``summary`` numeric
fields are seed-wise *means*; for every aggregated field a sibling
``"<field>__stdev"`` entry is added (zero when N==1) so downstream consumers
could render error bars. The first result is used as the structural template
(deep-copied) before its values are overwritten.

Input
-----
Reads ``<grid_dir>/manifest.json`` (written by ``cmd/grid_benchmark``):

    {
      "runs": [
        {
          "status":         "done" | ...,
          "topology":       { "label": "uniform_m", ... },
          "capacity":       "small" | "medium" | "large",
          "algorithm":      "topsis" | null,                # null for full-grid combos
          "topsis_weights": { "label": "time" | ... } | null,
          "seed":           int | null,
          "output_dir":     "results/grid_xxx/<combo_id>/",
        },
        ...
      ]
    }

For each ``run`` it walks the per-run output directory (same format the Go
exporter writes for ``plot_results.py``: one ``ExperimentResult`` JSON per
algorithm) via :func:`load_run_jsons`. Runs are bucketed:

  * ``topsis_weights is None`` → main grid bucket (all 5 algorithms).
  * ``topsis_weights is not None`` → TOPSIS sensitivity bucket (TOPSIS only,
    keyed by weight label parsed from filenames/manifest).

Output
------
PDF + PNG pairs are written to ``<grid_dir>/comparative/``:

  01_scalability_throughput   Tasks/min vs total-node-count (uniform_s/m/l, medium cap).
  02_scalability_makespan     Avg makespan (s) vs total-node-count.
  03_scalability_cost         Cost-per-task (mUSD) vs total-node-count.
  04_scalability_energy       Energy-per-task (mWh) vs total-node-count.
  05_topology_throughput      Grouped bars: throughput by topology × algorithm.
  06_topology_makespan        Grouped bars: makespan by topology × algorithm.
  07a/b/c/d_capacity_*        Grouped bars: metric by capacity profile (uniform_m only).
  08_topsis_sensitivity       TOPSIS under balanced + time/cost/energy weight variants.
  09_algorithm_winner_matrix  Per (topology × capacity) winner heatmap, one panel per metric.
  10_tier_ratio_effect        Stacked tier bar for edge-heavy/uniform_m/cloud-heavy (TOPSIS).

Additionally re-runs ``plot_results.py`` for any main-grid combo whose
``<combo>/plots/`` directory is missing or incomplete.

Winner determination
--------------------
``plot_winner_matrix`` performs a per-cell argmax/argmin: for each
(topology, capacity, metric) it iterates the algorithms present and tracks
the running extremum according to ``higher_is_better``. Ties keep the
first-seen algorithm.

Scalability fitting
-------------------
No regression is performed; scalability "curves" are linear segments between
the three uniform topologies' x-points (total node count). Y-values are the
raw aggregated summary metric (or the manually computed per-task ratio for
cost/energy).

TOPSIS weight detection
-----------------------
Weight variants are *not* parsed from filenames — they come straight from
``run["topsis_weights"]["label"]`` in the manifest. Plotting order is
``["balanced", "time", "cost", "energy"]``; balanced comes from the main
grid, the rest from the sensitivity bucket.

Usage
-----
    python scripts/grid_plot.py results/grid_YYYYMMDD_HHMMSS/
"""

import json
import os
import sys
import glob

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap, ListedColormap

import algorithms

plt.rcParams.update({
    "font.family":     "serif",
    "font.size":       10,
    "axes.titlesize":  11,
    "axes.labelsize":  10,
    "legend.fontsize": 9,
    "figure.dpi":      150,
})

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Policy names, labels and colours come from scripts/algorithms.py.
ALGORITHMS  = algorithms.ALL
ALGO_LABELS = algorithms.LABELS
COLORS      = algorithms.COLORS

TIERS       = ["EDGE", "FOG", "CLOUD"]
TIER_COLORS = {"EDGE": "#E8B94F", "FOG": "#5DADE2", "CLOUD": "#58D68D"}

CAP_LABELS  = {"small": "Small", "medium": "Medium", "large": "Large"}
CAP_ORDER   = ["small", "medium", "large"]

WEIGHT_LABELS = {
    "balanced": "Balanced\n(0.40/0.30/0.30)",
    "time":     "Time-focused\n(0.70/0.15/0.15)",
    "cost":     "Cost-focused\n(0.15/0.70/0.15)",
    "energy":   "Energy-focused\n(0.15/0.15/0.70)",
}

# Uniform topologies for scalability curves. Values are total node counts
# (edge + fog + cloud) used as the x-axis in 01-04 scalability plots.
# These three labels MUST match the ones produced by grid_benchmark.py.
UNIFORM_TOPOS = {
    "uniform_s": 6,   # 2+2+2
    "uniform_m": 15,  # 5+5+5
    "uniform_l": 24,  # 8+8+8
}


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_manifest(grid_dir: str) -> dict:
    """Read the grid sweep's ``manifest.json`` from ``grid_dir``.

    Args:
        grid_dir: Path to a ``results/grid_YYYYMMDD_HHMMSS/`` directory.

    Returns:
        dict: Parsed manifest with the schema described in the module docstring.

    Raises:
        FileNotFoundError: If ``manifest.json`` is missing.
    """
    path = os.path.join(grid_dir, "manifest.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"manifest.json not found in {grid_dir}. Run grid_benchmark.py first.")
    with open(path) as f:
        return json.load(f)


def load_run_jsons(output_dir: str) -> dict:
    """Load per-algorithm ``ExperimentResult`` JSONs from a single combo dir.

    For each ``*.json`` file in ``output_dir`` the function picks the result
    with the latest ``meta.timestamp`` per algorithm — useful when a combo
    has been re-run (e.g. retries) and the directory contains stale entries.

    Args:
        output_dir: Path to a per-combo output directory (one of the
            ``run["output_dir"]`` paths from the manifest).

    Returns:
        dict: ``{algorithm: ExperimentResult}`` with the freshest JSON per algo.
            Empty dict if the directory contains no parseable JSONs.
    """
    data = {}
    for path in glob.glob(os.path.join(output_dir, "*.json")):
        try:
            with open(path) as f:
                result = json.load(f)
            algo = result["meta"]["algorithm"]
            # Keep only the freshest result per algorithm (timestamp comparison
            # is a string compare; the ISO-8601-like format used by the Go
            # exporter sorts lexicographically by chronological order).
            if algo not in data or result["meta"]["timestamp"] > data[algo]["meta"]["timestamp"]:
                data[algo] = result
        except Exception:
            # Silently ignore unparseable files — defensive against partial
            # writes if a benchmark was interrupted mid-export.
            pass
    return data


def _aggregate_results(results: list) -> dict:
    """Collapse N per-seed ``ExperimentResult`` dicts into one mean-aggregated result.

    For every numeric field in ``summary`` (top-level and inside ``by_tier``)
    the function computes the seed-wise *arithmetic mean* and writes a sibling
    ``<field>__stdev`` entry holding the sample standard deviation
    (``statistics.stdev``, n-1 denominator). When only one seed is supplied
    the standard deviation defaults to ``0.0``. Non-numeric fields (strings,
    bools, dicts, lists) are taken from the first result via deep copy and
    left untouched — this preserves ``task_results``, ``servers``,
    ``meta.algorithm``, etc. from the template seed.

    Aggregation:
      * Mean      → ``summary[key]`` is overwritten with ``statistics.mean(vals)``.
      * Stdev     → ``summary[f"{key}__stdev"]`` is added (parallel to mean field).
      * Seed count → ``summary["__n_seeds"]`` records N.
      * Tier means → identical procedure applied to ``summary["by_tier"][tier][field]``.

    Args:
        results: List of ``ExperimentResult`` dicts, all from the same combo,
            differing only by seed.

    Returns:
        dict: Aggregated result. When ``len(results) == 1`` the input is returned
        unmodified (no stdev fields, no ``__n_seeds`` flag) for backward
        compatibility with single-seed grids. When ``len(results) == 0`` returns ``{}``.
    """
    if not results:
        return {}
    if len(results) == 1:
        # Single-seed grids: short-circuit. No error bars, no metadata flag.
        return results[0]

    # Use the first result as the structural template so non-numeric fields
    # (task_results, servers, meta, ...) survive untouched. Deep copy via JSON
    # round-trip to avoid aliasing into the source dict.
    import statistics
    template = json.loads(json.dumps(results[0]))  # deep copy
    summary = template.get("summary", {})
    summary["__n_seeds"] = len(results)

    def is_num(x):
        # Booleans subclass int in Python; explicitly exclude them so True/False
        # flags don't get averaged into the metrics.
        return isinstance(x, (int, float)) and not isinstance(x, bool)

    # Pass 1: top-level summary numeric fields → mean ± stdev.
    for key, val in list(summary.items()):
        if key.startswith("__") or not is_num(val):
            # Skip metadata keys (already added above) and non-numeric fields.
            continue
        vals = []
        for r in results:
            v = r.get("summary", {}).get(key)
            if is_num(v):
                vals.append(float(v))
        if not vals:
            continue
        summary[key] = statistics.mean(vals)
        # statistics.stdev requires >=2 samples; default to 0 for the lone-seed case
        # (defensive: shouldn't trigger here since len(results)>1 above).
        if len(vals) > 1:
            summary[f"{key}__stdev"] = statistics.stdev(vals)
        else:
            summary[f"{key}__stdev"] = 0.0

    # Pass 2: per-tier numeric fields. Union the tier keys across all results in
    # case some seeds happen to have zero tasks on a particular tier and omit it.
    by_tier_keys = set()
    for r in results:
        by_tier_keys.update((r.get("summary", {}).get("by_tier") or {}).keys())
    for tier in by_tier_keys:
        tier_dict = summary.setdefault("by_tier", {}).setdefault(tier, {})
        for field in list(tier_dict.keys()):
            if not is_num(tier_dict[field]):
                continue
            vals = []
            for r in results:
                v = r.get("summary", {}).get("by_tier", {}).get(tier, {}).get(field)
                if is_num(v):
                    vals.append(float(v))
            if vals:
                tier_dict[field] = statistics.mean(vals)
                tier_dict[f"{field}__stdev"] = statistics.stdev(vals) if len(vals) > 1 else 0.0

    return template


def _is_seeded(run: dict) -> bool:
    """Return True iff a manifest run carries an explicit seed value.

    Args:
        run: One element of ``manifest["runs"]``.

    Returns:
        bool: True when ``run["seed"]`` is present and non-None.
    """
    return run.get("seed") is not None


def load_grid_data(manifest: dict) -> tuple[dict, dict]:
    """Load and seed-aggregate every completed run in the grid manifest.

    Two parallel buckets are built before aggregation:

      * ``main_buckets[topo][cap][algo]``     for full-grid combos (no TOPSIS weights).
      * ``sens_buckets[topo][cap][weight]``   for TOPSIS sensitivity sub-runs.

    Each bucket accumulates the *list* of raw per-seed ``ExperimentResult``
    dicts. After all runs are scanned, every list is collapsed to a single
    mean-aggregated dict via :func:`_aggregate_results` (which also writes
    ``__stdev`` sibling fields when N > 1).

    Args:
        manifest: Output of :func:`load_manifest`.

    Returns:
        tuple[dict, dict]:
          * ``main_data[topo_label][cap_name][algo]`` → aggregated ``ExperimentResult``.
          * ``sens_data[topo_label][cap_name][weight_label]`` → aggregated TOPSIS
            ``ExperimentResult`` (TOPSIS algorithm only) for the corresponding
            weight variant.
    """
    # Collect raw per-seed buckets first, then aggregate at the end.
    main_buckets: dict = {}   # [topo][cap][algo] -> [result, ...]
    sens_buckets: dict = {}   # [topo][cap][w_label] -> [result, ...]

    for run in manifest.get("runs", []):
        if run.get("status") != "done":
            # Skip incomplete/failed runs so partial grids don't poison aggregates.
            continue

        topo_label = run["topology"]["label"]
        cap_name   = run["capacity"]
        weights    = run.get("topsis_weights")
        out_dir    = run["output_dir"]

        run_jsons  = load_run_jsons(out_dir)
        if not run_jsons:
            continue

        if weights is None:
            # Main grid run: take all 5 algorithms' results.
            for algo, result in run_jsons.items():
                main_buckets.setdefault(topo_label, {}).setdefault(cap_name, {}).setdefault(algo, []).append(result)
        else:
            # TOPSIS sensitivity run: only the TOPSIS result is kept, keyed by
            # the manifest-supplied weight label (balanced/time/cost/energy).
            w_label = weights["label"]
            topsis_result = run_jsons.get("topsis")
            if topsis_result:
                sens_buckets.setdefault(topo_label, {}).setdefault(cap_name, {}).setdefault(w_label, []).append(topsis_result)

    # Aggregate per-seed lists into single mean ± stdev results.
    main_data: dict = {}
    sens_data: dict = {}
    for topo, caps in main_buckets.items():
        for cap, algos in caps.items():
            for algo, results in algos.items():
                main_data.setdefault(topo, {}).setdefault(cap, {})[algo] = _aggregate_results(results)
    for topo, caps in sens_buckets.items():
        for cap, ws in caps.items():
            for w_label, results in ws.items():
                sens_data.setdefault(topo, {}).setdefault(cap, {})[w_label] = _aggregate_results(results)

    return main_data, sens_data


# ---------------------------------------------------------------------------
# Save helper
# ---------------------------------------------------------------------------

def save(fig, out_dir: str, name: str):
    """Persist ``fig`` as PDF + PNG into ``out_dir`` (created if missing).

    Args:
        fig: Matplotlib ``Figure`` to save.
        out_dir: Destination directory; created with ``exist_ok=True``.
        name: Filename stem; ``.pdf`` and ``.png`` extensions are appended.

    Returns:
        None.
    """
    os.makedirs(out_dir, exist_ok=True)
    for ext in ("pdf", "png"):
        # PDF is the thesis-import target; PNG is for quick visual inspection.
        fig.savefig(os.path.join(out_dir, f"{name}.{ext}"), bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# 01-04: Scalability curves (uniform topologies)
# ---------------------------------------------------------------------------

def plot_scalability(main_data: dict, out_dir: str):
    """Emit the four scalability curves (figures 01-04).

    Plots each metric against total node count across the three uniform
    topologies (``uniform_s/m/l`` → 6/15/24 nodes). Lines connect the three
    x-points piecewise-linearly per algorithm (no smoothing, no fit).
    Capacity is fixed at ``medium`` so the only varying axis is scale.

    Metrics produced (figure ID → ``ExperimentResult`` fields and aggregation):
      * 01_scalability_throughput → ``summary.throughput_tasks_per_min``,
        raw value (already a rate, no further aggregation here — multi-seed
        averaging is done upstream by :func:`_aggregate_results`).
      * 02_scalability_makespan → ``summary.avg_workflow_makespan_ms / 1000``.
      * 03_scalability_cost → ``total_cost_usd / total_tasks_completed * 1000``
        (per-task cost in mUSD; ``max(.., 1)`` guards against zero tasks).
      * 04_scalability_energy → ``total_energy_wh / total_tasks_completed * 1000``
        (per-task energy in mWh).

    Args:
        main_data: Aggregated main-grid bucket from :func:`load_grid_data`.
        out_dir: Destination directory for the four figures.

    Returns:
        None. Writes 4 PDF+PNG pairs (01-04 with ``_throughput/makespan/cost/energy``).
    """

    metrics = [
        ("throughput_tasks_per_min",   "Throughput (tasks/min)",   "01_scalability_throughput"),
        ("avg_workflow_makespan_ms",    "Avg makespan (s)",         "02_scalability_makespan"),
    ]
    # cost/task and energy/task are derived ratios — handled in dedicated blocks below.
    cost_label   = "Cost per task (mUSD)"
    energy_label = "Energy per task (mWh)"

    cap_name = "medium"  # use medium capacity for scalability curves

    # Keep an algorithm only if it appears for ≥1 uniform topology at this capacity.
    algos = [a for a in ALGORITHMS if any(
        cap_name in main_data.get(tl, {}) and a in main_data[tl][cap_name]
        for tl in UNIFORM_TOPOS
    )]

    # X-axis: sort uniform topologies by their total node count (6, 15, 24).
    topo_items = sorted(UNIFORM_TOPOS.items(), key=lambda kv: kv[1])

    for metric_key, ylabel, fname in metrics:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for algo in algos:
            xs, ys = [], []
            for tl, total_nodes in topo_items:
                result = main_data.get(tl, {}).get(cap_name, {}).get(algo)
                if result:
                    val = result["summary"].get(metric_key, 0)
                    if "makespan" in metric_key:
                        val /= 1000  # ms → s (only applies to the makespan metric)
                    xs.append(total_nodes)
                    ys.append(val)
            if xs:
                # No regression; markers at each topology, lines connecting them.
                ax.plot(xs, ys, marker="o", label=ALGO_LABELS[algo],
                        color=COLORS[algo], linewidth=1.8)

        ax.set_xlabel("Total nodes (edge + fog + cloud)")
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel.split(" (")[0] + " vs Infrastructure Scale")
        ax.set_xticks([kv[1] for kv in topo_items])
        ax.set_xticklabels([f"{kv[1]}\n({kv[0].replace('uniform_', '')})" for kv in topo_items])
        ax.legend(fontsize=9)
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        save(fig, out_dir, fname)

    # Figure 03: cost-per-task scalability. Derived ratio, computed inline
    # because the summary only exposes raw totals.
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for algo in algos:
        xs, ys = [], []
        for tl, total_nodes in topo_items:
            result = main_data.get(tl, {}).get(cap_name, {}).get(algo)
            if result:
                tasks = result["summary"].get("total_tasks_completed", 0)
                cost  = result["summary"].get("total_cost_usd", 0)
                # USD/task * 1000 → mUSD/task. max(tasks,1) guards against div-by-zero.
                val   = (cost / max(tasks, 1)) * 1000  # mUSD
                xs.append(total_nodes)
                ys.append(val)
        if xs:
            ax.plot(xs, ys, marker="o", label=ALGO_LABELS[algo],
                    color=COLORS[algo], linewidth=1.8)
    ax.set_xlabel("Total nodes")
    ax.set_ylabel(cost_label)
    ax.set_title("Cost per Task vs Infrastructure Scale")
    ax.set_xticks([kv[1] for kv in topo_items])
    ax.set_xticklabels([f"{kv[1]}\n({kv[0].replace('uniform_', '')})" for kv in topo_items])
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "03_scalability_cost")

    # Figure 04: energy-per-task scalability. Same ratio pattern as cost above.
    fig, ax = plt.subplots(figsize=(7, 4.5))
    for algo in algos:
        xs, ys = [], []
        for tl, total_nodes in topo_items:
            result = main_data.get(tl, {}).get(cap_name, {}).get(algo)
            if result:
                tasks  = result["summary"].get("total_tasks_completed", 0)
                energy = result["summary"].get("total_energy_wh", 0)
                # Wh/task * 1000 → mWh/task. Same div-by-zero guard.
                val    = (energy / max(tasks, 1)) * 1000  # mWh
                xs.append(total_nodes)
                ys.append(val)
        if xs:
            ax.plot(xs, ys, marker="o", label=ALGO_LABELS[algo],
                    color=COLORS[algo], linewidth=1.8)
    ax.set_xlabel("Total nodes")
    ax.set_ylabel(energy_label)
    ax.set_title("Energy per Task vs Infrastructure Scale")
    ax.set_xticks([kv[1] for kv in topo_items])
    ax.set_xticklabels([f"{kv[1]}\n({kv[0].replace('uniform_', '')})" for kv in topo_items])
    ax.legend(fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "04_scalability_energy")


# ---------------------------------------------------------------------------
# 05-06: Topology comparison (all topologies, medium capacity)
# ---------------------------------------------------------------------------

def _topo_short(label: str) -> str:
    """Wrap underscores in topology labels onto two lines for x-tick rendering.

    Args:
        label: Raw topology label (e.g. ``"edge_heavy"``).

    Returns:
        str: Label with ``_`` replaced by newline (``"edge\\nheavy"``).
    """
    return label.replace("_", "\n")


def plot_topology_comparison(main_data: dict, out_dir: str):
    """Emit figures 05-06: throughput / makespan grouped bars across topologies.

    For each topology present at ``medium`` capacity, draws one grouped bar
    per algorithm. Two figures are produced (one per metric), each holding
    all topologies side-by-side.

    Aggregation: none beyond the seed-mean already in ``main_data``. Each bar
    is one cell of ``main_data[topology][medium][algorithm]["summary"][...]``.

    Args:
        main_data: Aggregated main-grid bucket from :func:`load_grid_data`.
        out_dir: Destination directory.

    Returns:
        None. Writes ``05_topology_throughput`` and ``06_topology_makespan``
        (PDF + PNG). No-ops if no topology has the medium capacity profile.
    """
    cap_name = "medium"
    # Take every topology that has data at the medium capacity profile.
    all_topos = [t for t in main_data if cap_name in main_data[t]]
    # And every algorithm that shows up for at least one of those topologies.
    algos     = [a for a in ALGORITHMS if any(a in main_data[t][cap_name] for t in all_topos)]
    if not all_topos or not algos:
        return

    x     = np.arange(len(all_topos))
    # Reserve 80% of each x-slot for bars (20% inter-group gap), divided evenly across algos.
    width = 0.8 / len(algos)

    for metric_key, ylabel, scale, fname in [
        ("throughput_tasks_per_min", "Throughput (tasks/min)", 1,      "05_topology_throughput"),
        ("avg_workflow_makespan_ms", "Avg makespan (s)",        1/1000, "06_topology_makespan"),
    ]:
        fig, ax = plt.subplots(figsize=(12, 5))
        for i, algo in enumerate(algos):
            vals = []
            for tl in all_topos:
                result = main_data[tl][cap_name].get(algo)
                # `scale` converts ms→s for makespan; identity for throughput.
                vals.append((result["summary"].get(metric_key, 0) * scale) if result else 0)
            # Center the i-th algorithm's bars about each topology x-tick.
            offset = (i - len(algos) / 2 + 0.5) * width
            ax.bar(x + offset, vals, width * 0.9, label=ALGO_LABELS[algo],
                   color=COLORS[algo], edgecolor="white", linewidth=0.5)

        ax.set_xticks(x)
        ax.set_xticklabels([_topo_short(t) for t in all_topos], fontsize=8)
        ax.set_ylabel(ylabel)
        ax.set_title(f"{ylabel.split(' (')[0]} by Topology (medium capacity)")
        ax.legend(ncol=len(algos), fontsize=8, loc="upper right")
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        save(fig, out_dir, fname)


# ---------------------------------------------------------------------------
# 07: Capacity effect
# ---------------------------------------------------------------------------

def plot_capacity_effect(main_data: dict, out_dir: str):
    """Emit figures 07a-07d: metric sensitivity to capacity profile.

    Fixes the topology at ``uniform_m`` (the medium-scale, balanced topology)
    and varies the per-node capacity profile across small/medium/large. For
    each of four metrics, draws one grouped bar chart with algorithms on the
    x-axis and capacity profile as the inner grouping.

    Metrics × output names:
      * 07a_capacity_throughput → ``throughput_tasks_per_min`` (no scaling).
      * 07b_capacity_makespan   → ``avg_workflow_makespan_ms`` × 1/1000 (s).
      * 07c_capacity_cost       → ``total_cost_usd`` (no scaling).
      * 07d_capacity_energy     → ``total_energy_wh`` (no scaling).

    All values are seed-aggregated means from upstream :func:`_aggregate_results`.

    Args:
        main_data: Aggregated main-grid bucket from :func:`load_grid_data`.
        out_dir: Destination directory.

    Returns:
        None. Writes 4 PDF+PNG pairs.
    """
    topo_label = "uniform_m"
    caps       = [c for c in CAP_ORDER if c in main_data.get(topo_label, {})]
    algos      = [a for a in ALGORITHMS if any(a in main_data[topo_label].get(c, {}) for c in caps)]
    if not caps or not algos:
        return

    x     = np.arange(len(algos))
    # 3 capacity bars per algorithm; 80% slot reserved for bars.
    width = 0.8 / len(caps)

    for metric_key, ylabel, scale, fname in [
        ("throughput_tasks_per_min", "Throughput (tasks/min)", 1,      "07a_capacity_throughput"),
        ("avg_workflow_makespan_ms", "Avg makespan (s)",        1/1000, "07b_capacity_makespan"),
        ("total_cost_usd",           "Total cost (USD)",        1,      "07c_capacity_cost"),
        ("total_energy_wh",          "Total energy (Wh)",       1,      "07d_capacity_energy"),
    ]:
        fig, ax = plt.subplots(figsize=(10, 4.5))
        # Capacity-profile palette is independent from the algorithm palette.
        cap_colors = {"small": "#9B59B6", "medium": "#2ECC71", "large": "#E74C3C"}
        for i, cap in enumerate(caps):
            vals = []
            for algo in algos:
                result = main_data[topo_label].get(cap, {}).get(algo)
                vals.append((result["summary"].get(metric_key, 0) * scale) if result else 0)
            # Center the i-th capacity's bars about each algorithm x-tick.
            offset = (i - len(caps) / 2 + 0.5) * width
            ax.bar(x + offset, vals, width * 0.9, label=CAP_LABELS[cap],
                   color=cap_colors[cap], edgecolor="white", linewidth=0.5)

        ax.set_xticks(x)
        ax.set_xticklabels([ALGO_LABELS[a] for a in algos])
        ax.set_ylabel(ylabel)
        ax.set_title(f"{ylabel.split(' (')[0]} by Capacity Profile (uniform-medium topology)")
        ax.legend(title="Capacity")
        ax.spines[["top", "right"]].set_visible(False)
        fig.tight_layout()
        save(fig, out_dir, fname)


# ---------------------------------------------------------------------------
# 08: TOPSIS weight sensitivity
# ---------------------------------------------------------------------------

def plot_topsis_sensitivity(main_data: dict, sens_data: dict, out_dir: str):
    """Emit figure 08: TOPSIS performance under four weight configurations.

    Compares TOPSIS at ``uniform_m`` / ``medium`` capacity across:
      * ``balanced``  — 0.40 time / 0.30 cost / 0.30 energy (from main grid).
      * ``time``      — 0.70 time-focused (from sensitivity bucket).
      * ``cost``      — 0.70 cost-focused.
      * ``energy``    — 0.70 energy-focused.

    The "balanced" baseline comes from the *main* grid (full 5-algo bucket),
    the three skewed variants come from the *sensitivity* sub-runs in
    ``sens_data``. Weight labels are taken from the manifest, NOT filenames.

    Three side-by-side panels show ``throughput_tasks_per_min``,
    ``avg_workflow_makespan_ms`` (×1/1000), and ``total_cost_usd`` to expose
    the tradeoff: skewing weights toward one criterion necessarily worsens
    the others.

    Args:
        main_data: Main grid bucket (provides the "balanced" datum).
        sens_data: TOPSIS sensitivity bucket (provides the three skewed variants).
        out_dir: Destination directory.

    Returns:
        None. Writes ``08_topsis_sensitivity`` (PDF + PNG). Skipped with a
        message if fewer than 2 weight variants are available.
    """
    topo_label = "uniform_m"
    cap_name   = "medium"

    # Assemble four entries keyed by weight label.
    entries = {}
    # "balanced" lives in the main grid (it is the default TOPSIS run).
    balanced = main_data.get(topo_label, {}).get(cap_name, {}).get("topsis")
    if balanced:
        entries["balanced"] = balanced
    # The three skewed variants live in the sensitivity bucket.
    for w_label, result in sens_data.get(topo_label, {}).get(cap_name, {}).items():
        entries[w_label] = result

    if len(entries) < 2:
        # Need at least two variants for a meaningful sensitivity comparison.
        print("  [skip] topsis_sensitivity: not enough data")
        return

    # Fixed display order: balanced first, then time/cost/energy in that order
    # (skipping any that are missing from the sensitivity sweep).
    w_order = ["balanced"] + [w["label"] for w in [
        {"label": "time"}, {"label": "cost"}, {"label": "energy"}
    ] if w["label"] in entries]

    labels  = [WEIGHT_LABELS.get(w, w) for w in w_order]
    # Distinct per-variant colors; up to 4 variants, indexed by w_order position.
    w_colors = ["#8172B2", "#4C72B0", "#55A868", "#C44E52"]

    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5))
    metrics = [
        ("throughput_tasks_per_min", "Throughput (tasks/min)", 1),
        ("avg_workflow_makespan_ms", "Avg makespan (s)",        1/1000),
        ("total_cost_usd",           "Total cost (USD)",        1),
    ]

    for ax, (metric_key, ylabel, scale) in zip(axes, metrics):
        vals = [(entries[w]["summary"].get(metric_key, 0) * scale) if w in entries else 0
                for w in w_order]
        bars = ax.bar(range(len(w_order)), vals,
                      color=w_colors[:len(w_order)], edgecolor="white", linewidth=0.8)
        ax.bar_label(bars, fmt="%.2f", padding=2, fontsize=7)
        ax.set_xticks(range(len(w_order)))
        ax.set_xticklabels(labels, fontsize=7)
        ax.set_ylabel(ylabel)
        ax.set_title(ylabel.split(" (")[0])
        ax.spines[["top", "right"]].set_visible(False)

    fig.suptitle("TOPSIS Weight Sensitivity (uniform-medium topology, medium capacity)", fontsize=11)
    fig.tight_layout()
    save(fig, out_dir, "08_topsis_sensitivity")


# ---------------------------------------------------------------------------
# 09: Algorithm winner matrix
# ---------------------------------------------------------------------------

def plot_winner_matrix(main_data: dict, out_dir: str):
    """Emit figure 09: per-cell winning algorithm by metric.

    For each metric, builds a (topology × capacity) integer matrix where each
    cell holds the index (into :data:`ALGORITHMS`) of the algorithm that
    wins that combination on that metric. Empty cells (missing data) are
    encoded as ``-1`` and left unannotated.

    Winner determination: a single pass through ``algo_results`` for each
    cell, tracking the running extremum:
      * If ``higher_is_better`` (throughput): argmax on ``summary[metric_key]``.
      * Else (makespan/cost/energy): argmin.
    The first algorithm seen seeds ``best_val``; ties keep the first-seen
    algorithm (deterministic if the dict iteration order is stable, which it
    is in modern Python).

    Metrics evaluated:
      * throughput → ``throughput_tasks_per_min``  (higher = better)
      * makespan   → ``avg_workflow_makespan_ms``  (lower  = better)
      * cost       → ``total_cost_usd``            (lower  = better)
      * energy     → ``total_energy_wh``           (lower  = better)

    Args:
        main_data: Aggregated main-grid bucket from :func:`load_grid_data`.
        out_dir: Destination directory.

    Returns:
        None. Writes ``09_algorithm_winner_matrix`` (PDF + PNG).
    """

    topos = sorted(main_data.keys())
    caps  = CAP_ORDER
    metrics = [
        ("throughput",  "throughput_tasks_per_min", True),   # higher is better
        ("makespan",    "avg_workflow_makespan_ms",  False),  # lower is better
        ("cost",        "total_cost_usd",            False),  # lower is better
        ("energy",      "total_energy_wh",           False),  # lower is better
    ]

    # One axis per metric. Figure height scales with topology count so labels stay legible.
    fig, axes = plt.subplots(1, len(metrics), figsize=(16, max(len(topos) * 0.7 + 1.5, 5)))
    if len(metrics) == 1:
        axes = [axes]

    # Algorithm → integer index used as the imshow value (matched against ``cmap``).
    algo_idx = {a: i for i, a in enumerate(ALGORITHMS)}
    # Discrete colormap with one band per algorithm.
    cmap = ListedColormap([algorithms.color(a) for a in ALGORITHMS])

    for ax, (metric_label, metric_key, higher_is_better) in zip(axes, metrics):
        # -1 sentinel = no winner (no data for that cell).
        matrix = np.full((len(topos), len(caps)), -1, dtype=int)

        for r, tl in enumerate(topos):
            for c, cap in enumerate(caps):
                algo_results = main_data.get(tl, {}).get(cap, {})
                if not algo_results:
                    continue
                # Linear scan with running argmax/argmin; ties keep first-seen.
                best_algo = None
                best_val  = None
                for algo, result in algo_results.items():
                    val = result["summary"].get(metric_key, None)
                    if val is None:
                        continue
                    if best_val is None:
                        best_algo, best_val = algo, val
                    elif higher_is_better and val > best_val:
                        best_algo, best_val = algo, val
                    elif not higher_is_better and val < best_val:
                        best_algo, best_val = algo, val
                if best_algo:
                    matrix[r, c] = algo_idx.get(best_algo, -1)

        im = ax.imshow(matrix, cmap=cmap, vmin=0, vmax=len(ALGORITHMS) - 1, aspect="auto")

        # Overlay a 2-letter algorithm abbreviation in each cell that has a winner.
        for r in range(len(topos)):
            for c in range(len(caps)):
                idx = matrix[r, c]
                if idx >= 0:
                    label = algorithms.ABBREV[ALGORITHMS[idx]]
                    ax.text(c, r, label, ha="center", va="center", fontsize=7,
                            color="black", fontweight="bold")

        ax.set_xticks(range(len(caps)))
        ax.set_xticklabels([CAP_LABELS[c] for c in caps], fontsize=8)
        ax.set_yticks(range(len(topos)))
        ax.set_yticklabels([_topo_short(t) for t in topos], fontsize=7)
        ax.set_title(f"Best: {metric_label}", fontsize=9)

    # Legend outside last axis
    from matplotlib.patches import Patch
    legend_elements = [Patch(facecolor=cmap(algo_idx[a]), label=ALGO_LABELS[a])
                       for a in ALGORITHMS]
    fig.legend(handles=legend_elements, loc="lower center", ncol=len(ALGORITHMS),
               fontsize=8, bbox_to_anchor=(0.5, -0.05))

    fig.suptitle("Algorithm Winner Matrix (by metric)", fontsize=11, y=1.01)
    fig.tight_layout()
    save(fig, out_dir, "09_algorithm_winner_matrix")


# ---------------------------------------------------------------------------
# 10: Tier ratio effect
# ---------------------------------------------------------------------------

def plot_tier_ratio_effect(main_data: dict, out_dir: str):
    """Emit figure 10: tier distribution under edge-heavy / balanced / cloud-heavy topologies.

    Restricts to a single algorithm (TOPSIS) to isolate the *topology ratio*
    effect from the *algorithm* effect. Reads ``summary.by_tier[TIER].tasks_completed``
    for each of the three target topologies, then stacks EDGE/FOG/CLOUD into
    a single bar per topology so the shift in routing distribution is visible.

    Args:
        main_data: Aggregated main-grid bucket from :func:`load_grid_data`.
        out_dir: Destination directory.

    Returns:
        None. Writes ``10_tier_ratio_effect`` (PDF + PNG). No-op if fewer
        than two of the three target topologies have medium-capacity data.
    """
    target_topos   = ["edge_heavy", "uniform_m", "cloud_heavy"]
    cap_name       = "medium"
    # Hold algorithm constant to isolate topology-ratio effect.
    focus_algo     = "topsis"

    present = [t for t in target_topos if cap_name in main_data.get(t, {})]
    if len(present) < 2:
        return

    algos  = [a for a in ALGORITHMS if any(
        a in main_data[t].get(cap_name, {}) for t in present
    )]

    x     = np.arange(len(present))
    width = 0.7

    fig, ax = plt.subplots(figsize=(9, 5))
    # Stack EDGE → FOG → CLOUD via a running `bottom` accumulator.
    bottom = np.zeros(len(present))
    for tier in TIERS:
        vals = np.array([
            main_data[t].get(cap_name, {}).get(focus_algo, {}).get("summary", {})
                        .get("by_tier", {}).get(tier, {}).get("tasks_completed", 0)
            for t in present
        ], dtype=float)
        ax.bar(x, vals, width, label=tier, bottom=bottom,
               color=TIER_COLORS[tier], edgecolor="white", linewidth=0.8)
        bottom += vals

    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", " ").title() for t in present])
    ax.set_ylabel("Tasks completed")
    ax.set_title(f"Tier Distribution vs Topology Ratio (TOPSIS, medium capacity)")
    ax.legend(title="Tier")
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    save(fig, out_dir, "10_tier_ratio_effect")


# ---------------------------------------------------------------------------
# Per-config plots (call plot_results.py for any missed main-grid dirs)
# ---------------------------------------------------------------------------

def run_per_config_plots_if_missing(manifest: dict):
    """Invoke ``plot_results.py`` for every main-grid combo that lacks plots.

    Walks the manifest, skipping sensitivity runs (those have
    ``run["algorithm"] is not None`` because they target a single algorithm),
    and shells out to ``plot_results.py`` for any combo whose
    ``<combo>/plots/`` directory is missing or has fewer than 9 entries.

    Args:
        manifest: Parsed grid manifest.

    Returns:
        None. Side effect: per-config plots are (re)generated under each
        combo directory's ``plots/`` subfolder.
    """
    script = os.path.join(os.path.dirname(__file__), "plot_results.py")
    if not os.path.exists(script):
        return
    import subprocess
    for run in manifest.get("runs", []):
        if run.get("status") != "done" or run.get("algorithm") is not None:
            # Skip TOPSIS-only sensitivity runs: those are single-algo and don't
            # need the cross-algorithm per-config gallery.
            continue
        out_dir   = run["output_dir"]
        plots_dir = os.path.join(out_dir, "plots")
        # Threshold of 9 matches the 9 figures produced by plot_results.py.
        if os.path.isdir(plots_dir) and len(os.listdir(plots_dir)) >= 9:
            continue  # already done
        print(f"  Per-config plots for {run['combo_id']} ...")
        subprocess.run([sys.executable, script, out_dir], capture_output=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """CLI entry point: aggregate the grid sweep and emit all comparative figures.

    Reads ``<grid_dir>/manifest.json``, builds the main + sensitivity buckets,
    seed-aggregates them, then runs each plotting step in turn. Errors in any
    individual step are caught and logged so a single failure does not abort
    the rest of the pipeline.

    Args:
        None (consumes ``sys.argv[1]``).

    Returns:
        None. Side effects: prints progress, writes PDF/PNG pairs under
        ``<grid_dir>/comparative/``, and may regenerate per-config plots
        under individual combo directories.
    """
    if len(sys.argv) < 2:
        print("Usage: python scripts/grid_plot.py <grid_output_dir>", file=sys.stderr)
        sys.exit(1)

    grid_dir = sys.argv[1]
    print(f"Reading grid results from: {grid_dir}")

    manifest = load_manifest(grid_dir)

    done_runs = sum(1 for r in manifest.get("runs", []) if r.get("status") == "done")
    print(f"Completed runs in manifest: {done_runs}")

    print("Loading result JSONs ...")
    main_data, sens_data = load_grid_data(manifest)

    topo_count = len(main_data)
    cap_count  = max((len(caps) for caps in main_data.values()), default=0)
    print(f"Loaded: {topo_count} topologies × {cap_count} capacity profiles")
    print(f"TOPSIS sensitivity data: {sum(len(w) for t in sens_data.values() for w in t.values())} entries")

    out_dir = os.path.join(grid_dir, "comparative")
    os.makedirs(out_dir, exist_ok=True)
    print(f"Writing comparative plots to: {out_dir}/\n")

    steps = [
        ("Scalability curves (01-04)",          lambda: plot_scalability(main_data, out_dir)),
        ("Topology comparison (05-06)",          lambda: plot_topology_comparison(main_data, out_dir)),
        ("Capacity effect (07)",                 lambda: plot_capacity_effect(main_data, out_dir)),
        ("TOPSIS weight sensitivity (08)",       lambda: plot_topsis_sensitivity(main_data, sens_data, out_dir)),
        ("Algorithm winner matrix (09)",         lambda: plot_winner_matrix(main_data, out_dir)),
        ("Tier ratio effect (10)",               lambda: plot_tier_ratio_effect(main_data, out_dir)),
        ("Per-config plots (any missing)",       lambda: run_per_config_plots_if_missing(manifest)),
    ]

    for label, fn in steps:
        print(f"  {label} ...", end=" ", flush=True)
        try:
            fn()
            print("done")
        except Exception as e:
            print(f"ERROR: {e}")

    plot_files = [f for f in os.listdir(out_dir) if f.endswith(".pdf")]
    print(f"\nDone. {len(plot_files)} comparative plots in {out_dir}/")
    print("Each plot is available as .pdf (for LaTeX) and .png (for preview).")


if __name__ == "__main__":
    main()

"""Pareto view of every policy and TOPSIS / weighted-sum weight vector.

    python3 scripts/analysis/pareto.py GRID_DIR [GRID_DIR ...] [--out analysis_out/pareto]
                                       [--ref topsis]

Give the weight-sweep grids (results/pareto/sweep/task_*) together with the
C3 grids (results/c3_cluster/task_*): C3 supplies the weight-independent
policies on the same configurations and seeds.

A point is a policy, or ``policy[weights]`` for a weight-sweep run. Runs are
paired by (topology, capacity, interval, window, speed, seed); weights are
not part of the pairing, so every point is compared on the same workload and
fleet. Each metric is divided by the reference point's value in the same
pair, averaged in log space over seeds, then over configurations
(geometric mean; 95% bootstrap CI over configurations). Only configurations
where every point is present are used.

Metrics: total cost, mean workflow makespan, and p95 workflow response time
(DONE workflows of the run). Lower is better on all three.

Writes <out>/pareto_points.csv (one row per point: ratios, CIs, Pareto flags),
<out>/pareto_per_config.csv (seed means per configuration and point),
<out>/replication.csv when both ``topsis`` and ``topsis[u25]`` are present
(the sweep's u25 vector equals the C3 default, so they should agree within
noise), and <out>/pareto.pdf (cost vs makespan, cost vs p95 response).
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import algorithms  # noqa: E402
import figures  # noqa: E402  (shared rcParams, sizes and _save)
import load  # noqa: E402
import stats  # noqa: E402

PAIR = ["topology", "capacity", "interval_ms", "window_s", "speed", "seed"]
CONFIG = [c for c in PAIR if c != "seed"]
METRICS = {"cost": "total_cost_usd", "makespan": "avg_workflow_makespan_ms", "p95_response": "p95_response_ms"}
# Order of the utilisation ladder; its points are joined by a line per policy.
UTIL_LADDER = ["u0", "u10", "u25", "u50"]


def point_name(algorithm: str, weights: str) -> str:
    return algorithm if weights == "balanced" else f"{algorithm}[{weights}]"


def run_table(runs: pd.DataFrame, workflows: pd.DataFrame) -> pd.DataFrame:
    """Runs with a point name, p95 response and completion rate."""
    ids = ["grid", "combo", "algorithm"]
    done = workflows[workflows["status"] == "DONE"]
    resp = done.groupby(ids)["response_ms"].quantile(0.95).rename("p95_response_ms")
    rate = workflows.groupby(ids)["status"].apply(lambda s: (s == "DONE").mean()).rename("completion_rate")
    out = runs.merge(resp.reset_index(), on=ids, how="left").merge(rate.reset_index(), on=ids, how="left")
    out["point"] = [point_name(a, w) for a, w in zip(out["algorithm"], out["weights"])]
    return out


def ratios(t: pd.DataFrame, ref: str) -> tuple[pd.DataFrame, int, int]:
    """Per-configuration log ratios to ``ref`` over complete configurations.

    Returns (per-config means with log ratios, configs used, configs total).
    """
    seed_level = t.groupby(PAIR + ["point"], dropna=False)[list(METRICS.values()) + ["completion_rate"]].mean()
    n_points = t["point"].nunique()
    per_cfg_points = seed_level.reset_index().groupby(CONFIG, dropna=False)["point"].nunique()
    complete = per_cfg_points[per_cfg_points == n_points].index
    total = len(per_cfg_points)
    sl = seed_level.reset_index().set_index(CONFIG)
    sl = sl[sl.index.isin(complete)].reset_index()
    refv = sl[sl.point == ref].set_index(PAIR)[list(METRICS.values())]
    sl = sl.join(refv, on=PAIR, rsuffix="_ref")
    sl = sl.dropna(subset=[f"{c}_ref" for c in METRICS.values()])
    for m, c in METRICS.items():
        sl[f"log_{m}"] = np.log(sl[c] / sl[f"{c}_ref"])
    per_cfg = sl.groupby(CONFIG + ["point"], dropna=False).mean(numeric_only=True).reset_index()
    return per_cfg, len(complete), total


def summarise(per_cfg: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(stats.BOOTSTRAP_SEED)
    rows = []
    for point, g in per_cfg.groupby("point"):
        row = {"point": point, "n_configs": len(g), "completion_rate": g["completion_rate"].mean()}
        idx = rng.integers(0, len(g), size=(stats.BOOTSTRAP_RESAMPLES, len(g)))
        for m in METRICS:
            v = g[f"log_{m}"].to_numpy()
            boot = v[idx].mean(axis=1)
            row[f"{m}_ratio"] = float(np.exp(v.mean()))
            row[f"{m}_ci_low"] = float(np.exp(np.quantile(boot, 0.025)))
            row[f"{m}_ci_high"] = float(np.exp(np.quantile(boot, 0.975)))
        rows.append(row)
    out = pd.DataFrame(rows)
    for y in ("makespan", "p95_response"):
        out[f"pareto_cost_{y}"] = pareto_front(out["cost_ratio"].to_numpy(), out[f"{y}_ratio"].to_numpy())
    return out.sort_values("cost_ratio").reset_index(drop=True)


def pareto_front(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    """True for points not dominated (lower-or-equal on both, lower on one)."""
    keep = np.ones(len(x), dtype=bool)
    for i in range(len(x)):
        dominated = (x <= x[i]) & (y <= y[i]) & ((x < x[i]) | (y < y[i]))
        keep[i] = not dominated.any()
    return keep


def replication(t: pd.DataFrame) -> pd.DataFrame | None:
    """topsis (C3 default weights) vs topsis[u25] (same weights, sweep run)."""
    a, b = t[t.point == "topsis"], t[t.point == "topsis[u25]"]
    if a.empty or b.empty:
        return None
    m = a.merge(b, on=PAIR, suffixes=("_c3", "_sweep"))
    out = m[PAIR].copy()
    for c in ("total_cost_usd", "avg_workflow_makespan_ms", "total_tasks_completed"):
        out[f"{c}_c3"], out[f"{c}_sweep"] = m[f"{c}_c3"], m[f"{c}_sweep"]
    out["cost_diff_pct"] = 100 * (out["total_cost_usd_sweep"] / out["total_cost_usd_c3"] - 1)
    out["within_noise"] = (out["cost_diff_pct"].abs() <= 5) & \
        ((out["total_tasks_completed_sweep"] - out["total_tasks_completed_c3"]).abs() <= 1)
    return out


def _style(point: str) -> dict:
    """Colour and marker: the parent policy's, so weight variants read as a family."""
    algo = point.split("[")[0]
    marker = algorithms.MARKERS.get(algo, "o")
    hollow = algo in algorithms.HOLLOW or "[" in point
    c = algorithms.color(algo)
    if marker in ("x", "+", "1", "2", "3", "4"):
        return {"marker": marker, "color": c}
    return {"marker": marker, "facecolors": "none" if hollow else c, "edgecolors": c}


def plot(points: pd.DataFrame, ref: str, out_dir: str) -> str:
    plt = figures.plt
    fig, axes = plt.subplots(1, 2, figsize=(figures.DOUBLE_COL_IN, 2.9), sharex=True)
    for ax, y, ylabel in ((axes[0], "makespan", "Mean makespan"), (axes[1], "p95_response", "p95 response time")):
        # Pareto front as a recessive step line under the marks.
        front = points[points[f"pareto_cost_{y}"]].sort_values("cost_ratio")
        ax.step(front["cost_ratio"], front[f"{y}_ratio"], where="post", color="#b5b4ae",
                linewidth=1.0, linestyle="--", zorder=1, label="Pareto front")
        for algo in ("topsis", "weighted_sum"):
            ladder = [f"{algo}[{w}]" for w in UTIL_LADDER]
            lad = points.set_index("point").reindex(ladder).dropna(subset=["cost_ratio"])
            if len(lad) > 1:
                ax.plot(lad["cost_ratio"], lad[f"{y}_ratio"], color=algorithms.color(algo),
                        linewidth=0.8, alpha=0.6, zorder=2)
        for _, r in points.iterrows():
            st = _style(r["point"])
            ax.errorbar(r["cost_ratio"], r[f"{y}_ratio"],
                        xerr=[[r["cost_ratio"] - r["cost_ci_low"]], [r["cost_ci_high"] - r["cost_ratio"]]],
                        yerr=[[r[f"{y}_ratio"] - r[f"{y}_ci_low"]], [r[f"{y}_ci_high"] - r[f"{y}_ratio"]]],
                        fmt="none", ecolor="#b5b4ae", elinewidth=0.5, zorder=2)
            ax.scatter(r["cost_ratio"], r[f"{y}_ratio"], s=22, linewidths=0.9, zorder=3, **st)
            # Selective direct labels: weight variants (their vector) and front
            # members (abbreviation); other policies are identified by the legend.
            if "[" in r["point"]:
                text = r["point"].split("[")[1].rstrip("]")
            elif r[f"pareto_cost_{y}"]:
                text = algorithms.ABBREV.get(r["point"], r["point"])
            else:
                continue
            ax.annotate(text, (r["cost_ratio"], r[f"{y}_ratio"]), xytext=(3, 2), textcoords="offset points",
                        fontsize=figures.FONT_PT - 1.5, color="#52514e")
        ax.set_xscale("log")
        lo, hi = points["cost_ci_low"].min() * 0.95, points["cost_ci_high"].max() * 1.08
        ticks = [t for t in (0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6) if lo <= t <= hi]
        ax.set_xlim(lo, hi)
        ax.set_xticks(ticks, [f"{t:g}" for t in ticks])
        ax.xaxis.set_minor_locator(figures.matplotlib.ticker.NullLocator())
        ax.set_xlabel(f"Cost (ratio to {ref}, geometric mean)")
        ax.set_ylabel(f"{ylabel} (ratio to {ref})")
    # Legend: one entry per policy family plus the front.
    handles = []
    for algo in algorithms.ordered({p.split("[")[0] for p in points["point"]}):
        handles.append(plt.Line2D([], [], linestyle="none", markersize=4,
                                  marker=algorithms.MARKERS.get(algo, "o"),
                                  color=algorithms.color(algo), label=algorithms.label(algo)))
    handles.append(plt.Line2D([], [], color="#b5b4ae", linestyle="--", label="Pareto front"))
    fig.legend(handles=handles, loc="lower center", ncol=6, frameon=False, bbox_to_anchor=(0.5, 0.97))
    fig.text(0.5, -0.04, "Hollow labelled markers (u0, u25, time80, …) are weight-sweep variants of that policy; "
             "lines join the utilisation ladder u0 → u50. Bars: 95% bootstrap CI over configurations.",
             ha="center", va="top", fontsize=figures.FONT_PT - 1, color="#52514e")
    return figures._save(fig, out_dir, "pareto")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("grids", nargs="+")
    p.add_argument("--out", default="analysis_out/pareto")
    p.add_argument("--ref", default="topsis", help="reference point (falls back to topsis[u25])")
    args = p.parse_args(argv)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        runs, _, workflows = load.load_grids(args.grids)
    if runs.empty:
        print("no runs found", file=sys.stderr)
        return 1
    t = run_table(runs, workflows)
    ref = args.ref
    if ref not in set(t["point"]):
        ref = "topsis[u25]" if "topsis[u25]" in set(t["point"]) else None
    if ref is None:
        print(f"reference {args.ref!r} not found among points", file=sys.stderr)
        return 1

    per_cfg, used, total = ratios(t, ref)
    if per_cfg.empty:
        print("no configuration has every point; pass grids that cover the same configurations", file=sys.stderr)
        return 1
    points = summarise(per_cfg)
    os.makedirs(args.out, exist_ok=True)
    points.to_csv(os.path.join(args.out, "pareto_points.csv"), index=False)
    per_cfg.drop(columns=[c for c in per_cfg.columns if c.startswith("log_") or c == "seed"]) \
        .to_csv(os.path.join(args.out, "pareto_per_config.csv"), index=False)
    print(f"{len(points)} points over {used}/{total} configurations (complete ones only), reference {ref}")
    if used < total:
        print(f"  warning: {total - used} configurations lack some point and were left out", file=sys.stderr)
    cols = ["point", "cost_ratio", "makespan_ratio", "p95_response_ratio", "completion_rate",
            "pareto_cost_makespan", "pareto_cost_p95_response"]
    print(points[cols].round(3).to_string(index=False))

    rep = replication(t)
    if rep is not None:
        rep.to_csv(os.path.join(args.out, "replication.csv"), index=False)
        print(f"\nreplication topsis (C3) vs topsis[u25] (sweep): {int(rep.within_noise.sum())}/{len(rep)} "
              f"pairs within noise (tasks ±1, cost ±5%); median cost diff {rep.cost_diff_pct.median():+.2f}%")
    print("figure:", plot(points, ref, args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())

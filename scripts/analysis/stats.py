"""Statistics over the tidy tables produced by load.py.

Every function takes DataFrames and returns a DataFrame; nothing here reads
files or prints, so each is unit-tested on synthetic data.

Conventions
-----------
* A *configuration* is (topology, capacity[, interval_ms, weights]); seeds are
  its replicates. ``CONFIG`` lists the columns.
* Cost, energy and makespan are lower-is-better. ``METRICS`` maps short names
  to run-table columns.
* Results are descriptive unless a paired test with Holm correction backs
  them (see paired_tests).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as sps

CONFIG = ["topology", "capacity", "interval_ms", "weights"]
METRICS = {
    "cost": "total_cost_usd",
    "energy": "total_energy_wh",
    "makespan": "avg_workflow_makespan_ms",
}
TIERS = ["EDGE", "FOG", "CLOUD"]
REF = "topsis"
BOOTSTRAP_RESAMPLES = 10_000
BOOTSTRAP_SEED = 20261012


def _config_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in CONFIG if c in df.columns]


def grand_mean(runs: pd.DataFrame, metrics=None) -> pd.DataFrame:
    """Unweighted mean over all runs of each policy."""
    cols = [METRICS[m] for m in (metrics or METRICS)]
    out = runs.groupby("algorithm")[cols].mean()
    return out.rename(columns={v: k for k, v in METRICS.items()})


def config_means(runs: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Seed-mean of ``columns`` per configuration and policy."""
    keys = _config_cols(runs) + ["algorithm"]
    return runs.groupby(keys, dropna=False)[columns].mean().reset_index()


def wins(runs: pd.DataFrame, metrics=None, rtol: float = 1e-12) -> pd.DataFrame:
    """Per configuration, which policy has the lowest seed-mean of each metric.

    Returns one row per policy with ``<metric>_wins`` (sole lowest) and
    ``<metric>_ties`` (shares the lowest value within ``rtol``).
    """
    metrics = list(metrics or METRICS)
    cm = config_means(runs, [METRICS[m] for m in metrics])
    cfg = _config_cols(runs)
    algos = sorted(runs["algorithm"].unique())
    out = pd.DataFrame(0, index=algos, columns=[f"{m}_{k}" for m in metrics for k in ("wins", "ties")])
    for _, g in cm.groupby(cfg, dropna=False):
        for m in metrics:
            col = METRICS[m]
            best = g[col].min()
            at_best = g.loc[np.isclose(g[col], best, rtol=rtol, atol=0), "algorithm"].tolist()
            if len(at_best) == 1:
                out.loc[at_best[0], f"{m}_wins"] += 1
            else:
                for a in at_best:
                    out.loc[a, f"{m}_ties"] += 1
    out.index.name = "algorithm"
    return out


def tier_share(runs: pd.DataFrame) -> pd.DataFrame:
    """Share (%) of completed tasks per tier: seed-mean per configuration,
    then mean and s.d. over configurations, per policy."""
    df = runs.copy()
    total = df["total_tasks_completed"].replace(0, np.nan)
    share_cols = []
    for t in TIERS:
        col = f"by_tier.{t}.tasks_completed"
        df[f"share_{t}"] = 100 * df[col].fillna(0) / total
        share_cols.append(f"share_{t}")
    cm = config_means(df, share_cols)
    agg = cm.groupby("algorithm")[share_cols].agg(["mean", "std"])
    agg.columns = [f"{c}_{s}" for c, s in agg.columns]
    return agg


def _ratios(runs: pd.DataFrame, metric: str, ref: str) -> pd.DataFrame:
    """Per (configuration, seed): value(policy) / value(ref)."""
    col = METRICS.get(metric, metric)
    keys = _config_cols(runs) + ["seed"]
    piv = runs.pivot_table(index=keys, columns="algorithm", values=col, aggfunc="mean", dropna=False)
    if ref not in piv.columns:
        raise ValueError(f"reference policy {ref!r} not in runs")
    return piv.div(piv[ref], axis=0)


def normalised_means(runs: pd.DataFrame, metric: str, ref: str = REF,
                     resamples: int = BOOTSTRAP_RESAMPLES, seed: int = BOOTSTRAP_SEED) -> pd.DataFrame:
    """Geometric-mean ratio to ``ref`` over configurations, with a 95%
    percentile bootstrap CI that resamples configurations.

    Per configuration the seed ratios are combined by geometric mean first,
    so every configuration has equal weight.
    """
    logr = np.log(_ratios(runs, metric, ref))
    cfg = _config_cols(runs)
    per_cfg = logr.groupby(level=cfg, dropna=False).mean()
    rng = np.random.default_rng(seed)
    n = len(per_cfg)
    idx = rng.integers(0, n, size=(resamples, n))
    rows = []
    for algo in per_cfg.columns:
        v = per_cfg[algo].to_numpy()
        ok = ~np.isnan(v)
        if not ok.any():
            continue
        boots = np.nanmean(v[idx], axis=1)
        lo, hi = np.nanpercentile(boots, [2.5, 97.5])
        rows.append({"algorithm": algo, "metric": metric, "geomean_ratio": float(np.exp(np.nanmean(v))),
                     "ci_low": float(np.exp(lo)), "ci_high": float(np.exp(hi)), "n_configs": int(ok.sum())})
    return pd.DataFrame(rows)


def holm(pvalues) -> np.ndarray:
    """Holm-Bonferroni adjusted p-values (monotone, capped at 1)."""
    p = np.asarray(pvalues, dtype=float)
    order = np.argsort(p)
    m = len(p)
    adj = np.empty(m)
    running = 0.0
    for rank, i in enumerate(order):
        running = max(running, (m - rank) * p[i])
        adj[i] = min(1.0, running)
    return adj


def paired_tests(runs: pd.DataFrame, metric: str, ref: str = REF) -> pd.DataFrame:
    """Wilcoxon signed-rank test of ``ref`` against every other policy on
    configuration-level, seed-averaged values; Holm correction across the
    comparisons of this metric.

    Columns: median_ratio (other / ref), ref_better (configurations where ref
    is strictly lower), n (paired configurations), p, p_holm.
    """
    col = METRICS.get(metric, metric)
    cm = config_means(runs, [col])
    piv = cm.pivot_table(index=_config_cols(runs), columns="algorithm", values=col, dropna=False)
    rows = []
    for algo in piv.columns:
        if algo == ref:
            continue
        pair = piv[[ref, algo]].dropna()
        diff = pair[algo] - pair[ref]
        if len(pair) == 0:
            continue
        if (diff == 0).all():
            p = 1.0
        else:
            p = float(sps.wilcoxon(pair[ref], pair[algo], zero_method="wilcox").pvalue)
        rows.append({"metric": metric, "algorithm": algo, "median_ratio": float((pair[algo] / pair[ref]).median()),
                     "ref_better": int((pair[ref] < pair[algo]).sum()), "n": len(pair), "p": p})
    out = pd.DataFrame(rows)
    if not out.empty:
        out["p_holm"] = holm(out["p"])
    return out


def matched_workflows(workflows: pd.DataFrame, policies=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Makespan/response over workflows completed by every compared policy.

    Within each (configuration, seed) workflows are matched by ``seq``; only
    seqs DONE under all policies are kept. Returns (summary, completion):
    summary has mean/p50/p95 of makespan_ms (and response_ms when present)
    per policy; completion has the completion rate per policy.
    """
    wf = workflows if policies is None else workflows[workflows["algorithm"].isin(policies)]
    keys = _config_cols(wf) + ["seed"]
    n_pol = wf["algorithm"].nunique()
    done = wf[wf["status"] == "DONE"]
    counts = done.groupby(keys + ["seq"], dropna=False)["algorithm"].nunique()
    common = counts[counts == n_pol].reset_index()[keys + ["seq"]]
    matched = done.merge(common, on=keys + ["seq"])
    agg = {"matched_n": ("seq", "size"),
           "makespan_mean": ("makespan_ms", "mean"),
           "makespan_p50": ("makespan_ms", lambda s: s.quantile(0.5)),
           "makespan_p95": ("makespan_ms", lambda s: s.quantile(0.95))}
    if "response_ms" in matched.columns and matched["response_ms"].notna().any():
        agg.update(response_mean=("response_ms", "mean"),
                   response_p50=("response_ms", lambda s: s.quantile(0.5)),
                   response_p95=("response_ms", lambda s: s.quantile(0.95)))
    summary = matched.groupby("algorithm").agg(**agg)
    completion = wf.groupby("algorithm").agg(workflows=("seq", "size"),
                                             done=("status", lambda s: int((s == "DONE").sum())))
    completion["completion_rate"] = completion["done"] / completion["workflows"]
    return summary, completion


def per_tier(tasks: pd.DataFrame) -> pd.DataFrame:
    """Cost, energy and task count per tier and policy (sums over all runs)."""
    return (tasks.groupby(["algorithm", "server_type"])
            .agg(tasks=("execution_id", "size"), cost_usd=("cost_usd", "sum"), energy_wh=("energy_wh", "sum"))
            .reset_index())


def chosen_power(tasks: pd.DataFrame) -> pd.Series:
    """Power draw (W) of the server each task ran on.

    v2 results carry server_power_watts; for v1 it is inferred from
    energy_wh × 3600 / (effective_duration_ms / 1000).
    """
    inferred = tasks["energy_wh"] * 3600 / (tasks["effective_duration_ms"] / 1000)
    if "server_power_watts" in tasks.columns:
        return tasks["server_power_watts"].where(tasks["server_power_watts"].notna(), inferred)
    return inferred


def power_choice(tasks: pd.DataFrame, servers: pd.DataFrame) -> pd.DataFrame:
    """Mean power of chosen nodes vs. mean available power, per tier and policy.

    ``servers`` has one row per (run keys, server) with power_watts and
    server_type (from the v2 server snapshots).
    """
    t = tasks.assign(chosen_power=chosen_power(tasks))
    chosen = t.groupby(["algorithm", "server_type"])["chosen_power"].mean()
    avail = servers.groupby(["algorithm", "server_type"])["power_watts"].mean()
    out = pd.DataFrame({"chosen_power_w": chosen, "available_power_w": avail}).dropna()
    out["ratio"] = out["chosen_power_w"] / out["available_power_w"]
    return out.reset_index()


def queueing_delay(tasks: pd.DataFrame) -> pd.DataFrame:
    """start_time - ready_time (ms) per tier and policy; v2 results only."""
    if "ready_time" not in tasks.columns:
        return pd.DataFrame()
    t = tasks.dropna(subset=["ready_time"]).copy()
    delay = (pd.to_datetime(t["start_time"], format="ISO8601") -
             pd.to_datetime(t["ready_time"], format="ISO8601")).dt.total_seconds() * 1000
    t["queue_ms"] = delay
    return (t.groupby(["algorithm", "server_type"])["queue_ms"]
            .agg(mean="mean", p50="median", p95=lambda s: s.quantile(0.95), n="size").reset_index())

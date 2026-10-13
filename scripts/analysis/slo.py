"""Deadline / SLO metric: share of workflows finishing within k × critical path.

    python3 scripts/analysis/slo.py GRID_DIR [GRID_DIR ...] [--out analysis_out/slo]
                                    [--k 1.5,2,3,5] [--slack-ms 0] [--ref topsis]
                                    [--audit-bin PATH]

The deadline of a workflow is k × its critical-path length: the longest path
through its DAG with every task at its fastest feasible effective time
(duration / speed + latency, idle system), i.e. the makespan it would have
alone. A workflow meets its SLO when it is DONE and its response time
(completion − creation) is at most the deadline; workflows that never finish
miss it. --slack-ms adds a fixed allowance (deadline = k × CP + slack): in C3
42% of workflows have a critical path under 1 s, while dispatch costs about
450 ms per DAG level, so a pure multiple cannot be met for them under any
policy. No new runs are needed: critical paths are recomputed by replaying
each run's workflow draws from the grid's _configs/ with cmd/audit, so this
works on C3 and on every later grid.

Run from the repository root (configs refer to the dataset by a relative
path). Needs Go unless --audit-bin points at a built cmd/audit.

Writes <out>/slo_workflows.csv (one row per workflow: critical path, response,
slowdown = response / critical path), <out>/slo_runs.csv (per run: hit rate
for each k, median and p95 slowdown), <out>/slo_summary.csv (per policy and
configuration axis level: hit rate averaged over seeds then configurations,
with the paired difference to --ref), and <out>/slo_hit_rate.pdf.
"""

from __future__ import annotations

import argparse
import glob
import os
import subprocess
import sys
import tempfile
import warnings

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
for p in (HERE, os.path.dirname(HERE)):
    if p not in sys.path:
        sys.path.insert(0, p)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import algorithms  # noqa: E402
import figures  # noqa: E402
import load  # noqa: E402

# Axes other than topology/capacity that separate result groups in the summary.
GROUP = ["interval_ms", "window_s", "speed", "weights"]
CONFIG = ["topology", "capacity"] + GROUP


def critical_paths(grids: list[str], audit_bin: str, workdir: str) -> pd.DataFrame:
    """Replay every grid's configs through cmd/audit; one row per (grid, combo, seq).

    Configs are linked into batches whose combo names are unique, so a batch
    is one audit call (the dataset is loaded once per seed, not per combo).
    """
    batches: list[dict[str, str]] = []  # combo -> grid, per batch
    links: list[list[tuple[str, str]]] = []
    for grid in grids:
        for cfg in sorted(glob.glob(os.path.join(grid, "_configs", "*.yaml"))):
            combo = os.path.splitext(os.path.basename(cfg))[0]
            for b, l in zip(batches, links):
                if combo not in b:
                    b[combo] = grid
                    l.append((cfg, combo))
                    break
            else:
                batches.append({combo: grid})
                links.append([(cfg, combo)])
    frames = []
    for i, (b, l) in enumerate(zip(batches, links)):
        cdir = os.path.join(workdir, f"batch{i}")
        os.makedirs(cdir)
        for cfg, combo in l:
            os.symlink(os.path.abspath(cfg), os.path.join(cdir, f"{combo}.yaml"))
        out = os.path.join(workdir, f"audit{i}.csv")
        subprocess.run([audit_bin, "-configs", cdir, "-out", out], cwd=REPO, check=True,
                       stdout=subprocess.DEVNULL)
        wf = pd.read_csv(out.replace(".csv", "_workflows.csv"))
        wf["grid"] = wf["combo"].map(lambda c: os.path.basename(os.path.normpath(b[c])))
        frames.append(wf)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def join(workflows: pd.DataFrame, cp: pd.DataFrame, ks: list[float], slack_ms: float = 0) -> pd.DataFrame:
    w = workflows.merge(cp[["grid", "combo", "seq", "template", "critical_path_ms"]],
                        on=["grid", "combo", "seq"], how="left")
    missing = w["critical_path_ms"].isna() & w["template"].isna()
    if missing.any():
        warnings.warn(f"{int(missing.sum())} workflows have no replayed critical path "
                      "(no _configs/ for their grid?); they are left out")
        w = w[~missing]
    bad = w["template"] != w["template_name"]
    if bad.any():
        raise SystemExit(f"replay mismatch: {int(bad.sum())} workflows have a different template "
                         "than the run; the configs do not match the results")
    done = w["status"] == "DONE"
    w["slowdown"] = np.where(done, w["response_ms"] / w["critical_path_ms"], np.nan)
    for k in ks:
        w[f"met_k{k:g}"] = done & (w["response_ms"] <= k * w["critical_path_ms"] + slack_ms)
    return w


def per_run(w: pd.DataFrame, ks: list[float]) -> pd.DataFrame:
    keys = ["grid", "combo", "topology", "capacity", "seed"] + GROUP + ["algorithm"]
    agg = {f"hit_k{k:g}": (f"met_k{k:g}", "mean") for k in ks}
    agg.update(workflows=("seq", "size"), completed=("status", lambda s: int((s == "DONE").sum())),
               slowdown_p50=("slowdown", "median"), slowdown_p95=("slowdown", lambda s: s.quantile(0.95)))
    return w.groupby(keys, dropna=False).agg(**agg).reset_index()


def summary(runs: pd.DataFrame, ks: list[float], ref: str) -> pd.DataFrame:
    """Hit rate per policy and axis level: mean over seeds, then configurations,
    plus the paired difference to ref in percentage points."""
    cols = [f"hit_k{k:g}" for k in ks] + ["slowdown_p50", "slowdown_p95"]
    cfg = runs.groupby(CONFIG + ["algorithm"], dropna=False)[cols].mean().reset_index()
    out = cfg.groupby(GROUP + ["algorithm"], dropna=False)[cols].mean()
    out["n_configs"] = cfg.groupby(GROUP + ["algorithm"], dropna=False).size()
    refc = cfg[cfg.algorithm == ref].set_index(CONFIG)[[f"hit_k{k:g}" for k in ks]]
    if not refc.empty:
        d = cfg.join(refc, on=CONFIG, rsuffix="_ref")
        for k in ks:
            d[f"diff_k{k:g}_pp"] = 100 * (d[f"hit_k{k:g}"] - d[f"hit_k{k:g}_ref"])
        out = out.join(d.groupby(GROUP + ["algorithm"], dropna=False)[[f"diff_k{k:g}_pp" for k in ks]].mean())
    return out.reset_index()


def plot(w: pd.DataFrame, out_dir: str, slack_ms: float = 0) -> str:
    """Share of workflows meeting k × critical path, as a function of k
    (all configurations pooled, one line per policy; one panel per axis level
    when the grids span several)."""
    plt = figures.plt
    levels = w.groupby(GROUP, dropna=False).size().index.tolist()
    ks = np.linspace(1, 10, 91)
    fig, axes = plt.subplots(1, len(levels), figsize=(min(figures.DOUBLE_COL_IN, 3.5 * len(levels)), 2.6),
                             sharey=True, squeeze=False)
    for ax, level in zip(axes[0], levels):
        sub = w.loc[(w[GROUP] == pd.Series(level, index=GROUP)).all(axis=1)]
        for a in algorithms.ordered(sub["algorithm"].unique()):
            g = sub[sub.algorithm == a]
            done = g["status"] == "DONE"
            resp = g["response_ms"].where(done, np.inf).to_numpy()
            cp = g["critical_path_ms"].to_numpy()
            ax.plot(ks, [(resp <= k * cp + slack_ms).mean() for k in ks], color=algorithms.color(a),
                    linestyle=algorithms.LINESTYLES.get(a, "-"), linewidth=1.0, label=algorithms.label(a))
        ax.set_xlabel("Deadline k (× critical path" + (f" + {slack_ms:g} ms)" if slack_ms else ")"))
        title = ", ".join(f"{n}={v}" for n, v in zip(GROUP, level)
                          if (n, v) not in (("interval_ms", 2000), ("window_s", 30), ("speed", "none"),
                                            ("weights", "balanced")))
        ax.set_title(title or "C3 settings")
        ax.set_ylim(0, 1.02)
    axes[0][0].set_ylabel("Workflows meeting deadline")
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=5, frameon=False, bbox_to_anchor=(0.5, 0.97))
    return figures._save(fig, out_dir, "slo_hit_rate")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("grids", nargs="+")
    p.add_argument("--out", default="analysis_out/slo")
    p.add_argument("--k", default="1.5,2,3,5", help="deadline multipliers (default 1.5,2,3,5)")
    p.add_argument("--slack-ms", type=float, default=0,
                   help="fixed allowance added to every deadline (default 0)")
    p.add_argument("--ref", default="topsis", help="policy for the paired differences")
    p.add_argument("--audit-bin", default=None, help="built cmd/audit (default: go build into a temp dir)")
    args = p.parse_args(argv)
    ks = [float(k) for k in args.k.split(",") if k.strip()]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, _, workflows = load.load_grids(args.grids)
    if workflows.empty:
        print("no workflows found", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as tmp:
        audit = args.audit_bin
        if audit is None:
            audit = os.path.join(tmp, "audit")
            subprocess.run(["go", "build", "-o", audit, "./cmd/audit"], cwd=REPO, check=True)
        cp = critical_paths(args.grids, os.path.abspath(audit), tmp)
    w = join(workflows, cp, ks, args.slack_ms)
    runs = per_run(w, ks)
    summ = summary(runs, ks, args.ref)
    summ.insert(0, "slack_ms", args.slack_ms)

    os.makedirs(args.out, exist_ok=True)
    w.drop(columns=[c for c in w.columns if c.startswith("met_k")]) \
        .to_csv(os.path.join(args.out, "slo_workflows.csv"), index=False)
    runs.to_csv(os.path.join(args.out, "slo_runs.csv"), index=False)
    summ.to_csv(os.path.join(args.out, "slo_summary.csv"), index=False)
    print(f"{len(w)} workflows, {len(runs)} runs; critical path median "
          f"{w['critical_path_ms'].median():.0f} ms, slowdown median {w['slowdown'].median():.2f}")
    print(summ.round(3).to_string(index=False))
    print("figure:", plot(w, args.out, args.slack_ms))
    return 0


if __name__ == "__main__":
    sys.exit(main())

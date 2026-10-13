"""Catalogue of the post-C3 cluster experiments: array index -> job.

    python3 scripts/cluster/experiments.py count EXPERIMENT [LEVELS]
    python3 scripts/cluster/experiments.py job   EXPERIMENT INDEX [LEVELS]
    python3 scripts/cluster/experiments.py list  [EXPERIMENT]

Each experiment has one or more levels; a job is one (level, topology,
capacity, seed) and runs one grid combo with run_combo.sh into
results/<experiment>/<level>/task_<combo>/. Index order is level, topology,
capacity, seed (outer to inner), using the grid script's own lists.

`job` prints shell assignments for exp_array.sbatch to eval:
TOPOLOGY, CAPACITY, SEED, OUT_ROOT, LEVEL_FLAGS, LEVEL_ALGORITHMS.

LEVELS is an optional comma-separated subset of an experiment's levels;
SEEDS (environment, default 1,2,3,4,5) selects seeds.
"""

from __future__ import annotations

import os
import shlex
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import grid_benchmark as g  # noqa: E402

# experiment -> level -> (extra grid_benchmark flags, policies or None = all,
#                         Slurm time limit hint for submit.sh)
# Drain is raised above the 180 s default where runs carry more work: at
# 4–8× load, with a 120 s window, or with edge nodes at half speed (task
# durations up to 60 s). A run that has not drained by then is still
# exported (end_state.drained = false) and counted as incomplete.
EXPERIMENTS: dict[str, dict[str, tuple[str, str | None, str]]] = {
    # 1. Pareto / weight sweep: 8 weight vectors × {topsis, weighted_sum}.
    #    The 8 weight-independent policies come from C3 (same configs).
    "pareto": {
        "sweep": ("--weight-sweep --skip-main-grid", None, "02:00:00"),
    },
    # 2. Load sweep: C3 is the 2000 ms level. win120 keeps 2000 ms but runs a
    #    4× longer generation window (60 workflows, as iv500 has in 30 s).
    "load": {
        "iv1000": ("--interval-ms 1000 --drain 300", None, "03:00:00"),
        "iv500":  ("--interval-ms 500 --drain 300", None, "04:00:00"),
        "iv250":  ("--interval-ms 250 --drain 600", None, "06:00:00"),
        "win120": ("--duration 120 --drain 300", None, "04:00:00"),
    },
    # 3. Heterogeneous processing speed (SPEED_PRESETS in grid_benchmark.py).
    "speed": {
        "tier":  ("--speed tier --drain 300", None, "03:00:00"),
        "cores": ("--speed cores --drain 300", None, "03:00:00"),
    },
}


def _levels(exp: str, subset: str | None) -> list[str]:
    if exp not in EXPERIMENTS:
        sys.exit(f"unknown experiment {exp!r} (choose from {', '.join(EXPERIMENTS)})")
    levels = list(EXPERIMENTS[exp])
    if subset:
        chosen = [lv.strip() for lv in subset.split(",") if lv.strip()]
        bad = [lv for lv in chosen if lv not in levels]
        if bad or not chosen:
            sys.exit(f"unknown level(s) {bad or subset!r} for {exp} (choose from {', '.join(levels)})")
        levels = chosen
    return levels


def _seeds() -> list[int]:
    return [int(s) for s in os.environ.get("SEEDS", "1,2,3,4,5").split(",") if s.strip()]


def jobs(exp: str, subset: str | None = None) -> list[tuple[str, str, str, int]]:
    """All (level, topology, capacity, seed) jobs of an experiment, in index order."""
    return [(lv, t["label"], c, s)
            for lv in _levels(exp, subset)
            for t in g.TOPOLOGIES for c in g.CAPACITY_PROFILES for s in _seeds()]


def main(argv: list[str]) -> None:
    if not argv or argv[0] not in ("count", "job", "list"):
        sys.exit(__doc__)
    cmd = argv[0]
    if cmd == "list":
        for exp in (argv[1:2] or EXPERIMENTS):
            for lv, (flags, algos, limit) in EXPERIMENTS[exp].items():
                n = len(jobs(exp, lv))
                print(f"{exp:7s} {lv:7s} {n:4d} jobs  limit {limit}  policies {algos or 'all'}  flags: {flags}")
        return
    if cmd == "count":
        exp, subset = argv[1], (argv[2] if len(argv) > 2 else None)
        levels = _levels(exp, subset)
        limit = max(EXPERIMENTS[exp][lv][2] for lv in levels)
        print(len(jobs(exp, subset)), limit)
        return
    exp, idx, subset = argv[1], int(argv[2]), (argv[3] if len(argv) > 3 else None)
    js = jobs(exp, subset)
    if not 0 <= idx < len(js):
        sys.exit(f"array index {idx} out of range: {exp} has {len(js)} jobs")
    lv, topo, cap, seed = js[idx]
    flags, algos, _ = EXPERIMENTS[exp][lv]
    for k, v in (("TOPOLOGY", topo), ("CAPACITY", cap), ("SEED", seed),
                 ("OUT_ROOT", f"results/{exp}/{lv}"), ("LEVEL_FLAGS", flags),
                 ("LEVEL_ALGORITHMS", algos or "")):
        print(f"{k}={shlex.quote(str(v))}")


if __name__ == "__main__":
    main(sys.argv[1:])

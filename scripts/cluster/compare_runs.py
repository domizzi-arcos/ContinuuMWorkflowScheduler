"""Check that runs on two platforms agree within noise (WP6 rule).

    python3 scripts/cluster/compare_runs.py GRID_A GRID_B [GRID ...]

GRID_A is the reference (e.g. the local C1 grid); the remaining arguments
are compared with it (e.g. the cluster task directories). For every
(combo, policy) present on both sides the completed tasks must differ by at
most 1 and the total cost by at most 5%. Exits 1 if any pair fails.
"""
import os
import sys
import warnings

sys.path[:0] = [os.path.join(os.path.dirname(__file__), ".."),
                os.path.join(os.path.dirname(__file__), "..", "analysis")]
import load  # noqa: E402

warnings.simplefilter("ignore")
ref, _, _ = load.load_grids([sys.argv[1]])
other, _, _ = load.load_grids(sys.argv[2:])
keys = ["combo", "algorithm"]
m = ref.merge(other, on=keys, suffixes=("_a", "_b"))
if m.empty:
    sys.exit("no (combo, policy) pairs in common")
m["d_tasks"] = (m.total_tasks_completed_b - m.total_tasks_completed_a).abs()
m["d_cost_pct"] = 100 * (m.total_cost_usd_b / m.total_cost_usd_a - 1)
m["ok"] = (m.d_tasks <= 1) & (m.d_cost_pct.abs() <= 5)
print(m[keys + ["total_tasks_completed_a", "total_tasks_completed_b", "d_cost_pct", "ok"]]
      .round(2).to_string(index=False))
print(f"\n{int(m.ok.sum())}/{len(m)} pairs within noise (tasks ±1, cost ±5%)")
sys.exit(0 if m.ok.all() else 1)

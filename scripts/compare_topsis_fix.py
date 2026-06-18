#!/usr/bin/env python3
"""
compare_topsis_fix.py - Side-by-side comparison of pre-fix vs post-fix TOPSIS.

Loads:
  - OLD: results/grid_20260516_152819/<combo>/<algo>_*.json (single-seed pre-fix)
  - NEW: <new_dir>/<algo>_*.json (single-seed post-fix from a re-bench)

Prints a per-metric table showing whether the post-fix util-aware TOPSIS has
closed the throughput gap relative to the best-other-algorithm baseline.

Usage:
    python scripts/compare_topsis_fix.py <combo_id> <new_dir>
    e.g.
    python scripts/compare_topsis_fix.py topo_uniform_m_cap_large /tmp/verify_topsis_fix
"""
import json, os, sys, glob

# Frozen reference: the original single-seed grid from before the TOPSIS
# utilization-aware fix was merged. Used as the "OLD" baseline for the diff.
OLD_GRID = "results/grid_20260516_152819"
ALGOS = ["round_robin", "random", "best_fit", "worst_fit", "topsis"]


def latest_json_per_algo(directory: str) -> dict:
    """Return ``{algo: parsed_json}`` for the most recent file of each algorithm.

    Multiple result JSONs per algorithm can accumulate in a single combo dir
    when a run is re-executed. The ``meta.timestamp`` field on each file is
    used as the recency key, so the freshest run wins.

    Args:
        directory: Combo directory to scan (e.g. ``results/grid_.../topo_*``).

    Returns:
        Mapping from algorithm slug to its newest parsed JSON. Algorithms
        with no result files are simply absent from the dict.
    """
    out = {}
    for path in glob.glob(os.path.join(directory, "*.json")):
        name = os.path.basename(path)
        algo = None
        # Filenames look like ``round_robin_edge3_fog2_..._<ts>.json``.
        # Match against the known algorithm prefixes (longest-first not needed
        # because no algo is a prefix of another in :data:`ALGOS`).
        for a in ALGOS:
            if name.startswith(a + "_"):
                algo = a
                break
        if not algo:
            continue
        try:
            data = json.load(open(path))
        except Exception:
            continue
        # Keep only the freshest run per algo (ISO timestamps compare lexically).
        if algo not in out or data["meta"]["timestamp"] > out[algo]["meta"]["timestamp"]:
            out[algo] = data
    return out


def fmt_row(algo, s):
    """Format one algorithm's summary metrics as a fixed-width table row.

    Args:
        algo: Algorithm slug (rendered left-aligned in a 12-wide column).
        s: Summary dict from a result JSON (``summary`` sub-object).

    Returns:
        A single line containing throughput-per-minute, task count,
        makespan in ms, total cost in USD and total energy in Wh.
    """
    return (f"  {algo:<12} tpm={s['throughput_tasks_per_min']:>7.2f}  "
            f"tasks={s['total_tasks_completed']:>4}  "
            f"makespan={s['avg_workflow_makespan_ms']:>7.0f}ms  "
            f"cost=${s['total_cost_usd']:.5f}  "
            f"energy={s['total_energy_wh']:>6.2f}Wh")


def main():
    """CLI entry point: print the OLD vs NEW comparison for one combo.

    Reads the two combo directories, prints side-by-side metric tables, then
    a headline section computing the ratio of TOPSIS throughput to the best
    non-TOPSIS algorithm in each run. Acceptance threshold is 85% (TOPSIS
    within 15 percentage points of best-other).
    """
    if len(sys.argv) < 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    combo_id = sys.argv[1]
    new_dir  = sys.argv[2]
    old_dir  = os.path.join(OLD_GRID, combo_id)

    if not os.path.isdir(old_dir):
        print(f"Old combo dir not found: {old_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.path.isdir(new_dir):
        print(f"New combo dir not found: {new_dir}", file=sys.stderr)
        sys.exit(1)

    old = latest_json_per_algo(old_dir)
    new = latest_json_per_algo(new_dir)

    print(f"\n=== {combo_id} ===")
    print(f"\nOLD ({old_dir}):")
    for a in ALGOS:
        if a in old:
            print(fmt_row(a, old[a]["summary"]))
    print(f"\nNEW ({new_dir}):")
    for a in ALGOS:
        if a in new:
            print(fmt_row(a, new[a]["summary"]))

    # Headline: TOPSIS throughput delta and "% of best-other"
    def topsis_vs_best(grid):
        """Compute (topsis_tpm, best_other_tpm, ratio) for one grid snapshot.

        Args:
            grid: Output of :func:`latest_json_per_algo` for one combo.

        Returns:
            Triple of TOPSIS throughput, the best non-TOPSIS throughput, and
            their ratio. Returns ``(None, None, None)`` when TOPSIS data is
            missing or no baseline algorithm produced results.
        """
        topsis_tpm = grid.get("topsis", {}).get("summary", {}).get("throughput_tasks_per_min")
        others = [grid[a]["summary"]["throughput_tasks_per_min"] for a in ALGOS
                  if a != "topsis" and a in grid]
        if not topsis_tpm or not others:
            return None, None, None
        best_other = max(others)
        return topsis_tpm, best_other, (topsis_tpm / best_other) if best_other else 0

    o_tp, o_best, o_ratio = topsis_vs_best(old)
    n_tp, n_best, n_ratio = topsis_vs_best(new)

    print("\n=== TOPSIS vs best-other throughput ===")
    if o_tp is not None:
        print(f"  OLD: TOPSIS {o_tp:.2f}, best-other {o_best:.2f}  → TOPSIS = {100*o_ratio:.0f}% of best")
    if n_tp is not None:
        print(f"  NEW: TOPSIS {n_tp:.2f}, best-other {n_best:.2f}  → TOPSIS = {100*n_ratio:.0f}% of best")
    if o_ratio is not None and n_ratio is not None:
        delta = n_ratio - o_ratio
        print(f"  Δ (post-fix - pre-fix): {100*delta:+.1f} pp")
        if n_ratio >= 0.85:
            print("  PASS: TOPSIS now within 15% of best-other (acceptance threshold).")
        else:
            print(f"  FAIL: TOPSIS still {100*(1-n_ratio):.0f}% behind best-other.")


if __name__ == "__main__":
    main()

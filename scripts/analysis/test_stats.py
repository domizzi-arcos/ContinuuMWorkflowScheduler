import numpy as np
import pandas as pd
import pytest

import stats


def runs_frame(values):
    """values: {(topology, seed, algorithm): (cost, energy, makespan)}"""
    rows = []
    for (topo, seed, algo), (c, e, m) in values.items():
        rows.append({"topology": topo, "capacity": "medium", "interval_ms": 2000, "weights": "balanced",
                     "seed": seed, "algorithm": algo, "total_cost_usd": c, "total_energy_wh": e,
                     "avg_workflow_makespan_ms": m, "total_tasks_completed": 10,
                     "by_tier.EDGE.tasks_completed": 5, "by_tier.FOG.tasks_completed": 3,
                     "by_tier.CLOUD.tasks_completed": 2})
    return pd.DataFrame(rows)


def test_grand_mean_and_wins_with_ties():
    r = runs_frame({
        ("a", 1, "topsis"): (1, 5, 10), ("a", 2, "topsis"): (3, 5, 10),
        ("a", 1, "random"): (4, 5, 9), ("a", 2, "random"): (4, 5, 9),
        ("b", 1, "topsis"): (1, 1, 10), ("b", 1, "random"): (2, 2, 10),
    })
    gm = stats.grand_mean(r)
    assert gm.loc["topsis", "cost"] == pytest.approx(5 / 3)
    w = stats.wins(r)
    assert w.loc["topsis", "cost_wins"] == 2 and w.loc["random", "cost_wins"] == 0
    assert w.loc["topsis", "energy_wins"] == 1 and w.loc["topsis", "energy_ties"] == 1
    assert w.loc["random", "energy_ties"] == 1
    assert w.loc["random", "makespan_wins"] == 1 and w.loc["topsis", "makespan_ties"] == 1


def test_tier_share():
    r = runs_frame({("a", 1, "topsis"): (1, 1, 1), ("b", 1, "topsis"): (1, 1, 1)})
    r.loc[1, "by_tier.EDGE.tasks_completed"] = 7
    r.loc[1, "by_tier.FOG.tasks_completed"] = 1
    ts = stats.tier_share(r)
    assert ts.loc["topsis", "share_EDGE_mean"] == pytest.approx(60)
    assert ts.loc["topsis", "share_EDGE_std"] == pytest.approx(np.std([50, 70], ddof=1))


def test_normalised_means_known_ratio():
    vals = {}
    for i, topo in enumerate("abcdef"):
        for seed in (1, 2):
            vals[(topo, seed, "topsis")] = (1.0, 1.0, 1.0)
            vals[(topo, seed, "random")] = (2.0, 1.0, 1.0)
    nm = stats.normalised_means(runs_frame(vals), "cost", resamples=500)
    row = nm.set_index("algorithm").loc["random"]
    assert row.geomean_ratio == pytest.approx(2.0)
    assert row.ci_low == pytest.approx(2.0) and row.ci_high == pytest.approx(2.0)
    assert row.n_configs == 6
    assert nm.set_index("algorithm").loc["topsis", "geomean_ratio"] == pytest.approx(1.0)


def test_normalised_means_is_deterministic():
    rng = np.random.default_rng(0)
    vals = {(t, s, a): (rng.uniform(1, 3), 1.0, 1.0) for t in "abcdefgh" for s in (1, 2, 3)
            for a in ("topsis", "random")}
    r = runs_frame(vals)
    a = stats.normalised_means(r, "cost", resamples=1000)
    b = stats.normalised_means(r, "cost", resamples=1000)
    pd.testing.assert_frame_equal(a, b)


def test_holm():
    adj = stats.holm([0.01, 0.04, 0.03])
    assert adj == pytest.approx([0.03, 0.06, 0.06])
    assert stats.holm([0.5, 0.9]) == pytest.approx([1.0, 1.0])


def test_paired_tests_direction_and_counts():
    vals = {}
    for i, topo in enumerate("abcdefghijkl"):
        vals[(topo, 1, "topsis")] = (1.0, 1.0, 1.0)
        vals[(topo, 1, "random")] = (1.5 + i * 0.01, 1.0, 1.0)
        vals[(topo, 1, "best_fit")] = (1.0, 1.0, 1.0)
    pt = stats.paired_tests(runs_frame(vals), "cost").set_index("algorithm")
    assert pt.loc["random", "ref_better"] == 12 and pt.loc["random", "n"] == 12
    assert pt.loc["random", "p"] < 0.001
    assert pt.loc["random", "median_ratio"] == pytest.approx(1.555)
    assert pt.loc["best_fit", "p"] == 1.0 and pt.loc["best_fit", "ref_better"] == 0
    assert (pt["p_holm"] >= pt["p"]).all()


def test_matched_workflows():
    base = {"topology": "a", "capacity": "medium", "interval_ms": 2000, "weights": "balanced", "seed": 1}
    rows = []
    for algo, statuses in (("topsis", ["DONE", "DONE", "RUNNING"]), ("random", ["DONE", "RUNNING", "DONE"])):
        for seq, st in enumerate(statuses):
            rows.append(dict(base, algorithm=algo, seq=seq, status=st,
                             makespan_ms=100 * (seq + 1) + (5 if algo == "random" else 0),
                             response_ms=1000.0))
    summary, completion = stats.matched_workflows(pd.DataFrame(rows))
    # Only seq 0 is DONE under both policies.
    assert summary.loc["topsis", "matched_n"] == 1
    assert summary.loc["topsis", "makespan_mean"] == 100 and summary.loc["random", "makespan_mean"] == 105
    assert completion.loc["topsis", "completion_rate"] == pytest.approx(2 / 3)


def test_chosen_power_inference_and_queueing():
    t = pd.DataFrame({
        "algorithm": ["topsis", "topsis"], "server_type": ["EDGE", "EDGE"],
        "energy_wh": [10 * 2 / 3600, 1.0], "effective_duration_ms": [2000, 1000],
        "server_power_watts": [np.nan, 25],
        "start_time": ["2026-01-01T00:00:01.5Z", "2026-01-01T00:00:03Z"],
        "ready_time": ["2026-01-01T00:00:01Z", "2026-01-01T00:00:02Z"],
    })
    assert list(stats.chosen_power(t)) == pytest.approx([10, 25])
    q = stats.queueing_delay(t).iloc[0]
    assert q["mean"] == pytest.approx(750) and q["n"] == 2

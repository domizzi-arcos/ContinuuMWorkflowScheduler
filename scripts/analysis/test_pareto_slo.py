import numpy as np
import pandas as pd
import pytest

import pareto
import slo


def test_pareto_front():
    # (cost, makespan): a and b trade off, c is dominated by a, d ties a.
    x = np.array([1.0, 2.0, 1.5, 1.0])
    y = np.array([2.0, 1.0, 2.5, 2.0])
    assert pareto.pareto_front(x, y).tolist() == [True, True, False, True]


def test_point_name():
    assert pareto.point_name("topsis", "balanced") == "topsis"
    assert pareto.point_name("weighted_sum", "u50") == "weighted_sum[u50]"


def _wf(**kw):
    base = {"grid": "g", "combo": "c", "seq": 0, "template_name": "pipeline", "status": "DONE",
            "response_ms": 3000.0}
    base.update(kw)
    return base


def test_slo_join_deadlines_and_misses():
    workflows = pd.DataFrame([
        _wf(seq=0, response_ms=1500),                       # slowdown 1.5
        _wf(seq=1, response_ms=2500),                       # slowdown 2.5
        _wf(seq=2, status="RUNNING", response_ms=np.nan),   # never finished: always a miss
    ])
    cp = pd.DataFrame({"grid": "g", "combo": "c", "seq": [0, 1, 2], "template": "pipeline",
                       "critical_path_ms": 1000.0})
    w = slo.join(workflows, cp, [1.5, 2])
    assert w["met_k1.5"].tolist() == [True, False, False]
    assert w["met_k2"].tolist() == [True, False, False]
    assert w["slowdown"].iloc[:2].tolist() == [1.5, 2.5]
    # A fixed allowance admits the second workflow at k = 2 (2 × 1000 + 600 ≥ 2500).
    w = slo.join(workflows, cp, [2], slack_ms=600)
    assert w["met_k2"].tolist() == [True, True, False]


def test_slo_join_rejects_template_mismatch():
    workflows = pd.DataFrame([_wf()])
    cp = pd.DataFrame({"grid": ["g"], "combo": ["c"], "seq": [0], "template": ["hybrid"],
                       "critical_path_ms": [1000.0]})
    with pytest.raises(SystemExit):
        slo.join(workflows, cp, [2])

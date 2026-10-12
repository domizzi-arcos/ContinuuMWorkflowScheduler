import hashlib
import json
import os
import re

import numpy as np

import latex
import run


def make_grid(root, n_topo=4, seeds=(1, 2), algos=("topsis", "min_cost", "round_robin", "topsis_u0")):
    rng = np.random.default_rng(7)
    g = os.path.join(root, "grid_test")
    for t in range(n_topo):
        for cap in ("small", "large"):
            for seed in seeds:
                combo = f"topo_t{chr(97 + t)}_cap_{cap}_seed{seed}"
                os.makedirs(os.path.join(g, combo))
                for algo in algos:
                    tasks = []
                    for i, tier in enumerate(("EDGE", "FOG", "CLOUD")):
                        tasks.append({"execution_id": f"e{i}", "server_type": tier, "cost_usd": rng.uniform(0.001, 0.01),
                                      "energy_wh": rng.uniform(1, 5), "effective_duration_ms": 1000,
                                      "server_power_watts": 100, "start_time": "2026-01-01T00:00:02Z",
                                      "ready_time": "2026-01-01T00:00:01Z"})
                    wfs = [{"id": f"w{i}", "template_name": "pipeline", "priority": 1, "status": "DONE",
                            "makespan_ms": float(rng.uniform(1000, 5000)), "response_ms": 6000.0,
                            "created_at": "2026-01-01T00:00:00Z", "seq": i} for i in range(3)]
                    data = {"meta": {"algorithm": algo, "timestamp": "2026-01-01T00:00:00Z",
                                     "num_servers": {"EDGE": t + 1, "FOG": 2, "CLOUD": 1}},
                            "summary": {"total_cost_usd": sum(x["cost_usd"] for x in tasks),
                                        "total_energy_wh": sum(x["energy_wh"] for x in tasks),
                                        "avg_workflow_makespan_ms": float(np.mean([w["makespan_ms"] for w in wfs])),
                                        "total_tasks_completed": 3, "total_workflows_completed": 3,
                                        "by_tier": {x["server_type"]: {"tasks_completed": 1} for x in tasks}},
                            "task_results": tasks, "workflow_results": wfs,
                            "servers": [{"id": "s", "server_type": "EDGE", "power_watts": 120}]}
                    with open(os.path.join(g, combo, f"{algo}.json"), "w") as f:
                        json.dump(data, f)
    return g


def digest(d):
    h = hashlib.sha256()
    for root, _, files in sorted(os.walk(d)):
        for name in sorted(files):
            if name.endswith(".pdf"):
                continue  # PDF bytes are checked for existence only
            with open(os.path.join(root, name), "rb") as f:
                h.update(name.encode() + f.read())
    return h.hexdigest()


def test_camel():
    assert latex.camel("topsis_u0") == "TopsisUZero"
    assert latex.camel("min_cost") == "MinCost"
    assert re.fullmatch(r"[A-Za-z]+", latex.camel("best_fit_norm"))


def test_end_to_end_is_deterministic(tmp_path):
    g = make_grid(str(tmp_path))
    out1, out2 = str(tmp_path / "a"), str(tmp_path / "b")
    assert run.main([g, "--out", out1]) == 0
    assert run.main([g, "--out", out2]) == 0
    assert digest(out1) == digest(out2)
    for fig in ("winner_matrix", "cost_vs_makespan", "tier_share", "per_task_vs_nodes"):
        assert os.path.getsize(os.path.join(out1, "figures", f"{fig}.pdf")) > 1000
    numbers = open(os.path.join(out1, "latex", "numbers.tex")).read()
    names = re.findall(r"\\newcommand\{\\([^}]*)\}", numbers)
    assert len(names) == len(set(names)), "duplicate macro names"
    assert all(re.fullmatch(r"[A-Za-z]+", n) for n in names), "macro names must be letters only"
    assert "\\newcommand{\\RatioCostMinCost}" in numbers
    assert "\\newcommand{\\NumRuns}{64}" in numbers
    for tab in ("tab_results", "tab_tiers", "tab_stats"):
        text = open(os.path.join(out1, "latex", f"{tab}.tex")).read()
        assert "\\toprule" in text and "\\bottomrule" in text

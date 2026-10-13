import json
import os

import pytest

import load


def write_run(grid, combo, algo, ts, cost=1.0, labels=None, seq=True, n_wf=3):
    d = os.path.join(grid, combo)
    os.makedirs(d, exist_ok=True)
    meta = {"algorithm": algo, "timestamp": ts, "num_servers": {"EDGE": 5, "FOG": 3, "CLOUD": 1}}
    if labels:
        meta["labels"] = labels
    wfs = []
    for i in range(n_wf):
        w = {"id": f"w{i}", "template_name": "pipeline", "priority": 1, "status": "DONE",
             "makespan_ms": 100 + i, "created_at": f"2026-01-01T00:00:0{n_wf - i}Z"}
        if seq:
            w["seq"] = i
        wfs.append(w)
    data = {
        "meta": meta,
        "summary": {"total_cost_usd": cost, "total_tasks_completed": 2,
                    "by_tier": {"EDGE": {"tasks_completed": 2, "total_cost_usd": cost}},
                    "end_state": {"pending": 0, "drained": True}},
        "task_results": [{"execution_id": "e1", "server_type": "EDGE", "cost_usd": cost,
                          "dataflow_path": ["x"]}],
        "workflow_results": wfs,
        "servers": [],
    }
    with open(os.path.join(d, f"{algo}_{ts}.json"), "w") as f:
        json.dump(data, f)


def test_parse_combo():
    assert load.parse_combo("topo_edge_heavy_cap_small_seed3") == {
        "topology": "edge_heavy", "capacity": "small", "weights": "balanced", "speed": "none", "seed": 3,
        "interval_ms": 2000}
    assert load.parse_combo("topo_uniform_m_cap_large_w_cost_seed1_iv500")["interval_ms"] == 500
    assert load.parse_combo("topo_uniform_m_cap_large_w_cost_seed1_iv500")["weights"] == "cost"
    sweep = load.parse_combo("topo_fog_heavy_cap_small_w_time80u25_sp_cores_seed2_iv250")
    assert (sweep["weights"], sweep["speed"], sweep["seed"], sweep["interval_ms"]) == ("time80u25", "cores", 2, 250)
    assert load.parse_combo("topo_cloud_med_cap_medium")["seed"] is None
    assert load.parse_combo("benchmark_20260101") == {}


def test_load_flattens_and_dedups(tmp_path):
    g = str(tmp_path / "grid_x")
    write_run(g, "topo_uniform_m_cap_medium_seed1", "topsis", "2026-01-01T00:00:00Z", cost=1)
    write_run(g, "topo_uniform_m_cap_medium_seed1", "topsis", "2026-01-02T00:00:00Z", cost=2)
    write_run(g, "topo_uniform_m_cap_medium_seed1", "random", "2026-01-01T00:00:00Z", cost=3)
    os.makedirs(os.path.join(g, "_configs"))
    with pytest.warns(UserWarning, match="duplicate"):
        runs, tasks, wfs = load.load_grids([g])
    assert len(runs) == 2
    top = runs[runs.algorithm == "topsis"].iloc[0]
    assert top.total_cost_usd == 2  # newest kept
    assert top["by_tier.EDGE.tasks_completed"] == 2
    assert top["end_state.drained"] == True  # noqa: E712
    assert top.topology == "uniform_m" and top.capacity == "medium" and top.seed == 1
    assert top.workflows_total == 3
    assert top.num_nodes == 9 and top["num_servers.CLOUD"] == 1
    assert len(tasks) == 2 and "dataflow_path" not in tasks.columns
    assert sorted(wfs.seq.unique()) == [0, 1, 2]


def test_labels_override_path(tmp_path):
    g = str(tmp_path / "grid_y")
    write_run(g, "topo_uniform_m_cap_medium_seed1", "topsis", "t",
              labels={"topology": "fog_heavy", "capacity": "large", "seed": "4", "interval_ms": "1000"})
    runs, _, _ = load.load_grids([g])
    r = runs.iloc[0]
    assert (r.topology, r.capacity, r.seed, r.interval_ms) == ("fog_heavy", "large", 4, 1000)


def test_v1_seq_from_created_at_rank(tmp_path):
    g = str(tmp_path / "grid_z")
    write_run(g, "topo_uniform_m_cap_medium_seed1", "topsis", "t", seq=False)
    with pytest.warns(UserWarning, match="without seq"):
        _, _, wfs = load.load_grids([g])
    # created_at decreases with i, so w2 was created first.
    assert dict(zip(wfs.id, wfs.seq)) == {"w2": 0, "w1": 1, "w0": 2}

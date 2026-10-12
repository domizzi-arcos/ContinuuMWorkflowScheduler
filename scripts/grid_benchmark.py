#!/usr/bin/env python3
"""Master orchestrator for the thesis grid benchmark.

This script drives the full experimental sweep used in Chapter 6 of the
distributed-workflow-scheduler thesis. It invokes the Go binary at
``cmd/benchmark`` once per (topology, capacity profile) cell, letting that
binary internally loop over all five scheduling algorithms, and then runs an
additional TOPSIS-only pass to study weight sensitivity.

Experimental grid
-----------------
* Main grid:   8 topologies × 3 capacity profiles × 5 algorithms
               = 120 algorithm runs delivered via 24 binary calls
               (each binary call cycles through the 5 algorithms internally).
* TOPSIS sweep: 3 weight variants (time-/cost-/energy-dominant) × 8 topologies
               × 3 profiles = 72 TOPSIS-only runs, each one its own binary call.
* Grand total: ~192 algorithm executions.

Inputs
------
* CLI flags (see ``parse_args``): duration, base config path, output dir,
  dry-run, resume dir, pre-built binary path, seed sweep list, and the
  ``--skip-topsis-weights`` toggle to drop the sensitivity pass.
* The base YAML config (default ``configs/campaign_base.yaml``) supplies the static
  fields (Redis/RabbitMQ endpoints, generator settings, etc.); the topology
  and capacity stanzas are mutated per combination by ``make_config``.

Outputs
-------
* A ``grid_<UTC_TIMESTAMP>/`` directory (configurable via ``--output-dir``)
  containing one subdirectory per combination with the Go binary's JSON
  results, a ``_configs/`` subdir with the exact YAML used for each run, a
  ``grid_progress.json`` checkpoint manifest written after every combo, and a
  final ``manifest.json`` summarising the whole sweep.

Checkpoint / resume semantics
-----------------------------
``grid_progress.json`` is rewritten atomically (write tmp + ``os.replace``)
after every binary call. On ``--resume <dir>``, ``load_progress`` reads the
file and skips any combo whose entry has ``status == "done"``; failed combos
are retried. Corrupt JSON triggers a warning and a fresh start so a partial
write cannot poison the resume path.

Loop semantics
--------------
``generate_main_grid`` yields one entry per (topology, capacity[, seed])
cell with ``algorithm = None`` so the Go binary cycles through all five
algorithms in a single process. ``generate_topsis_sensitivity`` yields one
entry per (topology, capacity, weight variant[, seed]) tuple with
``algorithm = "topsis"`` so only TOPSIS is executed per call.
"""

import argparse
import hashlib
import platform
import copy
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    print("Error: pyyaml not installed. Run: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Grid definitions
# ---------------------------------------------------------------------------

# The five algorithms are evaluated in this fixed order so that downstream
# plot scripts (grid_plot.py) get a stable category axis. TOPSIS is last so
# the sensitivity sweep that follows naturally extends the same series.
ALGORITHMS = ["round_robin", "random", "best_fit", "worst_fit", "topsis"]

# Drain deadline of cmd/benchmark (-drain default) and per-run fixed overhead,
# used only for the runtime estimate.
DRAIN_S = 180
RUN_OVERHEAD_S = 3

# Eight topologies cover three regimes:
#   - uniform_{s,m,l}: equal server counts per tier at increasing scale
#     (stress test for algorithms that don't differentiate by tier).
#   - edge_{heavy,med} and cloud_{heavy,med}: tier-skewed deployments that
#     expose locality/cost trade-offs (edge-heavy favours latency-aware
#     placement; cloud-heavy favours throughput-aware placement).
#   - fog_heavy: middle-tier dominant, the "fog computing" canonical case.
TOPOLOGIES = [
    {"label": "uniform_s",   "edge": 2, "fog": 2, "cloud": 2},
    {"label": "uniform_m",   "edge": 5, "fog": 5, "cloud": 5},
    {"label": "uniform_l",   "edge": 8, "fog": 8, "cloud": 8},
    {"label": "edge_heavy",  "edge": 8, "fog": 3, "cloud": 1},
    {"label": "edge_med",    "edge": 5, "fog": 2, "cloud": 1},
    {"label": "fog_heavy",   "edge": 2, "fog": 6, "cloud": 2},
    {"label": "cloud_heavy", "edge": 1, "fog": 3, "cloud": 8},
    {"label": "cloud_med",   "edge": 1, "fog": 2, "cloud": 5},
]

# Three capacity profiles span an order of magnitude in hardware specs.
# Each tier (edge/fog/cloud) declares ranges for cpu, memory, disk, latency,
# cost-per-hour, and power draw. The Go seeder picks per-server values
# uniformly from [min, max] in `step` increments. The ratio between tiers
# is intentionally preserved across profiles so that comparative metrics
# (placement ratio, energy/cost per task) remain dimensionally consistent.
CAPACITY_PROFILES = {
    "small": {
        "edge": {
            "cpu":          {"min": 1,   "max": 2,   "step": 1},
            "memory":       {"min": 128, "max": 512, "step": 128},
            "disk":         {"min": 0,   "max": 10,  "step": 1},
            "wan_latency":  {"min": 3,   "max": 12,  "step": 1},
            "cost_per_hour": 0.02,
            "power_watts":  {"min": 10,  "max": 25,  "step": 5},
        },
        "fog": {
            "cpu":          {"min": 2,   "max": 4,    "step": 1},
            "memory":       {"min": 512, "max": 2048, "step": 512},
            "disk":         {"min": 20,  "max": 80,   "step": 10},
            "wan_latency":  {"min": 20,  "max": 45,   "step": 5},
            "cost_per_hour": 0.10,
            "power_watts":  {"min": 80,  "max": 150,  "step": 25},
        },
        "cloud": {
            "cpu":          {"min": 4,    "max": 8,    "step": 2},
            "memory":       {"min": 2048, "max": 8192, "step": 2048},
            "disk":         {"min": 100,  "max": 400,  "step": 50},
            "wan_latency":  {"min": 80,   "max": 160,  "step": 10},
            "cost_per_hour": 0.60,
            "power_watts":  {"min": 200,  "max": 400,  "step": 50},
        },
    },
    "medium": {
        "edge": {
            "cpu":          {"min": 1,   "max": 2,   "step": 1},
            "memory":       {"min": 128, "max": 768, "step": 128},
            "disk":         {"min": 0,   "max": 20,  "step": 1},
            "wan_latency":  {"min": 3,   "max": 12,  "step": 1},
            "cost_per_hour": 0.02,
            "power_watts":  {"min": 10,  "max": 25,  "step": 5},
        },
        "fog": {
            "cpu":          {"min": 4,    "max": 8,    "step": 1},
            "memory":       {"min": 2048, "max": 8192, "step": 512},
            "disk":         {"min": 40,   "max": 140,  "step": 10},
            "wan_latency":  {"min": 20,   "max": 45,   "step": 5},
            "cost_per_hour": 0.25,
            "power_watts":  {"min": 100,  "max": 200,  "step": 25},
        },
        "cloud": {
            "cpu":          {"min": 16,    "max": 32,    "step": 2},
            "memory":       {"min": 16384, "max": 65536, "step": 4096},
            "disk":         {"min": 400,   "max": 1600,  "step": 100},
            "wan_latency":  {"min": 80,    "max": 160,   "step": 10},
            "cost_per_hour": 1.50,
            "power_watts":  {"min": 300,   "max": 600,   "step": 50},
        },
    },
    "large": {
        "edge": {
            "cpu":          {"min": 2,   "max": 4,    "step": 1},
            "memory":       {"min": 512, "max": 2048, "step": 512},
            "disk":         {"min": 0,   "max": 40,   "step": 2},
            "wan_latency":  {"min": 3,   "max": 12,   "step": 1},
            "cost_per_hour": 0.04,
            "power_watts":  {"min": 15,  "max": 40,   "step": 5},
        },
        "fog": {
            "cpu":          {"min": 8,    "max": 16,    "step": 2},
            "memory":       {"min": 8192, "max": 32768, "step": 2048},
            "disk":         {"min": 100,  "max": 400,   "step": 50},
            "wan_latency":  {"min": 20,   "max": 45,    "step": 5},
            "cost_per_hour": 0.50,
            "power_watts":  {"min": 150,  "max": 300,   "step": 25},
        },
        "cloud": {
            "cpu":          {"min": 32,    "max": 64,     "step": 4},
            "memory":       {"min": 65536, "max": 262144, "step": 16384},
            "disk":         {"min": 800,   "max": 3200,   "step": 200},
            "wan_latency":  {"min": 80,    "max": 160,    "step": 10},
            "cost_per_hour": 3.00,
            "power_watts":  {"min": 400,   "max": 800,    "step": 100},
        },
    },
}

# Balanced weights used for the main grid. make_config always writes these,
# so the base config's own topsis_weights never reach the main grid.
# 'util' weights the post-placement capacity-utilisation criterion, the
# saturation fix from Chapter 5: without it, TOPSIS keeps choosing the same
# top server until its resources are exhausted. Note: time/cost/energy sum
# to 1.0 here; util is an additive penalty on top, not part of the simplex.
BALANCED_WEIGHTS = {"time": 0.40, "cost": 0.30, "energy": 0.30, "util": 0.25}

# Single-criterion-dominant variants for the Chapter 6 weight-sensitivity
# study. Each variant puts 0.70 on one criterion and 0.15 on each of the
# other two. util=0 deliberately disables the saturation penalty so these
# numbers stay comparable to the pre-fix 3-criterion TOPSIS baseline in
# the literature (Hwang & Yoon 1981 formulation).
TOPSIS_WEIGHT_VARIANTS = [
    {"label": "time",   "time": 0.70, "cost": 0.15, "energy": 0.15, "util": 0.0},
    {"label": "cost",   "time": 0.15, "cost": 0.70, "energy": 0.15, "util": 0.0},
    {"label": "energy", "time": 0.15, "cost": 0.15, "energy": 0.70, "util": 0.0},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts() -> str:
    """Return an ISO-8601 UTC timestamp string used in progress and manifest files."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_text(cmd: list[str]) -> str | None:
    """Run ``cmd`` and return its stripped stdout, or None if it fails."""
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def hash_host(name: str) -> str:
    """First 16 hex chars of SHA-256 over ``name``.

    Must match ``provenance.HashString`` in Go so manifests and per-run
    ``meta.provenance`` can be joined. The raw host name is never stored.
    """
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def collect_provenance() -> dict:
    """Describe the code and machine that produced this grid.

    Records the git commit and dirty flag of the working tree, the Go
    toolchain version, a hashed host name and the orchestrator's CLI args.
    """
    porcelain = _run_text(["git", "status", "--porcelain"])
    return {
        "git_commit": _run_text(["git", "rev-parse", "HEAD"]) or "unknown",
        "git_dirty":  bool(porcelain) if porcelain is not None else None,
        "go_version": _run_text(["go", "env", "GOVERSION"]) or "unknown",
        "host_hash":  hash_host(platform.node()),
        "args":       sys.argv[1:],
    }


def load_base_config(path: str) -> dict:
    """Load the base YAML config that every per-combo config is derived from.

    Args:
        path: Filesystem path to the base YAML (e.g. ``configs/campaign_base.yaml``).

    Returns:
        Parsed dict representing the full config tree.
    """
    with open(path) as f:
        return yaml.safe_load(f)


def make_config(base: dict, topo: dict, cap: dict, duration: int,
                topsis_weights: dict | None = None,
                algorithm: str | None = None,
                seed: int | None = None) -> dict:
    """Produce a per-combo config by mutating a deep copy of the base.

    The base config is treated as immutable; this function is the single
    place where experimental knobs are tweaked, which keeps the audit trail
    obvious — anything not touched here is exactly as the user wrote it.

    Args:
        base: Parsed base YAML config (unmodified).
        topo: One entry from ``TOPOLOGIES`` (label + per-tier server counts).
        cap: One entry from ``CAPACITY_PROFILES`` (per-tier hardware specs).
        duration: Wall-clock seconds for the simulation (``experiment.duration_seconds``).
        topsis_weights: Optional weight dict; when None the ``BALANCED_WEIGHTS``
            preset is used (main grid). Pass one of ``TOPSIS_WEIGHT_VARIANTS``
            for the sensitivity sweep.
        algorithm: Optional algorithm name. When set (TOPSIS sensitivity case)
            the binary runs only that algorithm. When None the binary loops
            over all five algorithms internally.
        seed: Optional integer seed. When provided, every RNG source in the
            simulation (server seeder, scheduler tie-breaker, task generator,
            workflow shape) is set to the same value so that a single ``--seeds``
            flag controls full-stack reproducibility.

    Returns:
        A fresh dict ready to be YAML-dumped as a per-combo config file.
    """
    cfg = copy.deepcopy(base)

    # Override server counts per tier (topology dimension of the grid).
    cfg["servers"]["edge"]["count"] = topo["edge"]
    cfg["servers"]["fog"]["count"]  = topo["fog"]
    cfg["servers"]["cloud"]["count"] = topo["cloud"]

    # Capacity profile — splat every hardware spec key from the profile into
    # cfg["servers"][<tier>]. This intentionally overwrites whatever was in
    # the base config; the base only has to supply non-spec fields (counts
    # default values, scheduling hints, etc.).
    for tier in ("edge", "fog", "cloud"):
        for key, val in cap[tier].items():
            cfg["servers"][tier][key] = val

    # TOPSIS weights. The main grid uses BALANCED_WEIGHTS; the sensitivity
    # sweep replaces these with a single-criterion-dominant variant per run.
    w = topsis_weights if topsis_weights else BALANCED_WEIGHTS
    cfg["scheduler"]["topsis_weights"] = {
        "time":   w["time"],
        "cost":   w["cost"],
        "energy": w["energy"],
        "util":   w.get("util", 0.0),
    }

    # The Go binary requires `scheduler.algorithm` to be a valid name even
    # when -algorithm is passed on the CLI (config validation runs first).
    # When the multi-algorithm pass is used we set a safe placeholder; the
    # binary overrides it per inner iteration.
    if algorithm:
        cfg["scheduler"]["algorithm"] = algorithm
    else:
        cfg["scheduler"]["algorithm"] = "round_robin"  # placeholder; binary loops through all

    cfg["experiment"]["duration_seconds"] = duration

    # Seed injection — applied to every random source so a fresh seed shuffles
    # server specs, scheduler RNG, task sampling, and workflow shape together.
    # When seed is None the config's existing seeds are preserved (legacy mode).
    if seed is not None:
        cfg["servers"]["seed"]            = int(seed)
        cfg["scheduler"]["seed"]          = int(seed)
        cfg["generator"]["seed"]          = int(seed)
        cfg["generator"]["workflow_seed"] = int(seed)
    return cfg


def write_temp_config(cfg: dict, path: str):
    """Persist a per-combo config to disk as YAML.

    The parent directory is created on demand. The file is kept after the
    run so the exact config used for each result is auditable.

    Args:
        cfg: Config dict produced by ``make_config``.
        path: Destination YAML path inside ``<grid_dir>/_configs/``.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False, allow_unicode=True)


def build_binary(binary_path: str) -> bool:
    """Compile the Go benchmark binary once at the start of the run.

    Building eagerly (vs. ``go run`` per-combo) avoids paying the compile
    cost N times and removes Go toolchain availability as a per-run risk.

    Args:
        binary_path: Where to drop the compiled binary (typically inside the
            grid output directory so it's colocated with results).

    Returns:
        True on a successful build, False otherwise; stderr from ``go build``
        is forwarded to the user on failure.
    """
    print(f"Building benchmark binary → {binary_path} ...")
    result = subprocess.run(
        ["go", "build", "-o", binary_path, "./cmd/benchmark"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"Build failed:\n{result.stderr}", file=sys.stderr)
        return False
    print("Build successful.")
    return True


def run_binary(binary: str, config_path: str, output_dir: str,
               duration: int, algorithm: str | None = None) -> bool:
    """Invoke the Go benchmark binary for a single combination.

    Output is NOT captured so progress logs from the Go side stream live
    to the user's terminal — useful for catching wedged runs.

    Args:
        binary: Absolute or relative path to the compiled benchmark binary.
        config_path: YAML config the binary will read.
        output_dir: Directory the binary writes its JSON results into.
        duration: Seconds to simulate (matches ``experiment.duration_seconds``).
        algorithm: When given, restricts the binary to a single algorithm.
            Otherwise the binary cycles through all five.

    Returns:
        True iff the subprocess exited with code 0.
    """
    cmd = [
        binary,
        "-config", config_path,
        "-output", output_dir,
        "-duration", str(duration),
    ]
    if algorithm:
        cmd += ["-algorithm", algorithm]

    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, text=True)
    return result.returncode == 0


def run_per_config_plots(combo_dir: str):
    """Best-effort: generate per-combination comparison plots.

    Plots are nice-to-have, not load-bearing, so failures and timeouts are
    downgraded to warnings — the grid keeps running. Only triggered for
    multi-algorithm runs (where intra-combo comparison is meaningful).

    Args:
        combo_dir: Output dir of the combo just finished by the Go binary.
    """
    script = os.path.join(os.path.dirname(__file__), "plot_results.py")
    if not os.path.exists(script):
        return
    try:
        # 5-minute cap keeps a stuck matplotlib process from blocking the grid.
        result = subprocess.run(
            [sys.executable, script, combo_dir],
            capture_output=True, text=True,
            timeout=300,
        )
        if result.returncode != 0:
            print(f"  [warn] plot_results.py failed for {combo_dir}: {result.stderr[:200]}")
    except subprocess.TimeoutExpired:
        print(f"  [warn] plot_results.py timed out for {combo_dir}, skipping plots")


def save_progress(progress_path: str, runs: list):
    """Atomically write the progress manifest after every combo.

    Atomicity (write-temp + ``os.replace``) is the key invariant: if the
    process is killed mid-write, the on-disk file is still either the old
    valid snapshot or the new valid snapshot — never a truncated half-write
    that would force the user to discard hours of work.

    Args:
        progress_path: Path to ``grid_progress.json`` inside the grid dir.
        runs: List of run-entry dicts accumulated so far.
    """
    tmp = progress_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"updated": ts(), "runs": runs}, f, indent=2)
    os.replace(tmp, progress_path)


def load_progress(progress_path: str) -> set:
    """Return the set of combo IDs already completed in a prior session.

    Only entries with ``status == "done"`` count — failed combos are
    intentionally retried on resume. Corrupt JSON (truncation, partial
    write that somehow bypassed the atomic replace) downgrades to a clean
    restart with a stderr warning rather than crashing.

    Args:
        progress_path: Path to ``grid_progress.json``.

    Returns:
        Set of ``combo_id`` strings to skip on this session.
    """
    if not os.path.exists(progress_path):
        return set()
    try:
        with open(progress_path) as f:
            data = json.load(f)
        return {r["combo_id"] for r in data.get("runs", []) if r.get("status") == "done"}
    except (json.JSONDecodeError, KeyError):
        # Stale / corrupt progress file: warn but keep going so a bad file
        # never strands the user. They can still inspect the JSON manually.
        print(f"[warn] progress file corrupt, starting fresh: {progress_path}", file=sys.stderr)
        return set()


# ---------------------------------------------------------------------------
# Combination generation
# ---------------------------------------------------------------------------

def generate_main_grid(grid_dir: str, seeds: list[int] | None = None) -> list:
    """Enumerate the main (topology × capacity[, seed]) combinations.

    Iteration order is topology → capacity → seed (outer→inner). This is
    deliberate: it groups all capacity sweeps for the same topology
    together, which makes partial results readable while the run is still
    in flight. ``algorithm`` is left None so each binary call inside loops
    over all five algorithms in-process — that's where the 24 binary calls
    × 5 algorithms = 120 algorithm runs accounting comes from.

    Args:
        grid_dir: Top-level output directory; each combo's output dir is
            ``<grid_dir>/<combo_id>``.
        seeds: Optional list of integer seeds; when given, every (topo, cap)
            cell is repeated once per seed and combo_ids gain a ``_seed{N}``
            suffix. When None, a single un-seeded combo is generated per cell.

    Returns:
        List of combo dicts (each carries everything ``main()`` needs to
        materialise a config and dispatch a binary call).
    """
    combos = []
    # Sentinel [None] so the inner loop runs exactly once when --seeds is unset.
    seed_list = seeds if seeds else [None]
    for topo in TOPOLOGIES:
        for cap_name in CAPACITY_PROFILES:
            for seed in seed_list:
                base_id = f"topo_{topo['label']}_cap_{cap_name}"
                # base_combo_id is the un-seeded identity used for cross-seed
                # aggregation later; combo_id is the unique per-run id.
                combo_id = base_id if seed is None else f"{base_id}_seed{seed}"
                combos.append({
                    "combo_id":       combo_id,
                    "base_combo_id":  base_id,
                    "topology":       topo,
                    "capacity_name":  cap_name,
                    "topsis_weights": None,
                    "algorithm":      None,
                    "seed":           seed,
                    "output_dir":     os.path.join(grid_dir, combo_id),
                })
    return combos


def generate_topsis_sensitivity(grid_dir: str, seeds: list[int] | None = None) -> list:
    """Enumerate the TOPSIS weight-sensitivity combinations.

    For every (topology, capacity) cell, run TOPSIS three more times — once
    per weight variant in ``TOPSIS_WEIGHT_VARIANTS``. ``algorithm="topsis"``
    forces the Go binary to skip its internal algorithm loop and execute
    only TOPSIS, which is why this pass has a 1:1 binary-call-to-algo-run
    ratio (72 calls = 72 runs).

    Args:
        grid_dir: Top-level output directory.
        seeds: Optional list of integer seeds (same semantics as
            ``generate_main_grid``).

    Returns:
        List of combo dicts.
    """
    combos = []
    seed_list = seeds if seeds else [None]
    for topo in TOPOLOGIES:
        for cap_name in CAPACITY_PROFILES:
            for w in TOPSIS_WEIGHT_VARIANTS:
                for seed in seed_list:
                    # combo_id encodes the weight variant so the file system
                    # alone disambiguates the sensitivity runs from the
                    # balanced-weight main grid.
                    base_id = f"topo_{topo['label']}_cap_{cap_name}_w_{w['label']}"
                    combo_id = base_id if seed is None else f"{base_id}_seed{seed}"
                    combos.append({
                        "combo_id":       combo_id,
                        "base_combo_id":  base_id,
                        "topology":       topo,
                        "capacity_name":  cap_name,
                        "topsis_weights": w,
                        "algorithm":      "topsis",
                        "seed":           seed,
                        "output_dir":     os.path.join(grid_dir, combo_id),
                    })
    return combos


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    """Define and parse the CLI.

    Returns:
        ``argparse.Namespace`` with the validated flags consumed by ``main``.
    """
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--duration",             type=int, default=30,
                   help="generation window per algorithm run, in seconds (default: 30)")
    p.add_argument("--config",               default="configs/campaign_base.yaml",
                   help="base config file (default: configs/campaign_base.yaml)")
    p.add_argument("--output-dir",           default=None,
                   help="top-level results directory (default: results/grid_TIMESTAMP)")
    p.add_argument("--dry-run",              action="store_true",
                   help="print all combinations without executing")
    p.add_argument("--emit-configs-only",    action="store_true",
                   help="write _configs/<combo>.yaml for every combination and exit "
                        "without building or running anything (input for cmd/audit)")
    p.add_argument("--skip-topsis-weights",  action="store_true",
                   help="skip TOPSIS weight sensitivity runs")
    p.add_argument("--resume",               default=None, metavar="DIR",
                   help="resume interrupted run from existing grid output dir")
    p.add_argument("--binary",               default=None,
                   help="path to pre-built benchmark binary")
    p.add_argument("--seeds",                default=None,
                   help="comma-separated list of seeds (e.g. '1,2,3'). When set, every "
                        "combo runs once per seed and the output dir gets a _seed{N} suffix. "
                        "Multiplies the workload — for the main grid use 3 seeds × 24 combos.")
    return p.parse_args()


def main():
    """Drive the grid: parse args, enumerate combos, dispatch the binary, checkpoint.

    The function is intentionally linear (no helper splits) so the experiment
    audit trail reads top-to-bottom: validate inputs → resolve output dir →
    enumerate work → build binary → loop with checkpointing → write final manifest.
    """
    args = parse_args()

    if not os.path.exists(args.config):
        print(f"Error: base config not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    base_cfg = load_base_config(args.config)

    # Resolve the grid output directory. Two modes:
    #  - --resume: directory must exist; we'll append to its progress file.
    #  - fresh: derive a UTC-stamped dir under results/ unless overridden.
    if args.resume:
        grid_dir = args.resume
        if not os.path.isdir(grid_dir):
            print(f"Error: resume dir not found: {grid_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Resuming grid run in: {grid_dir}")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        grid_dir = args.output_dir or f"results/grid_{stamp}"
        os.makedirs(grid_dir, exist_ok=True)
        print(f"Grid output dir: {grid_dir}")

    # Standard sub-paths under the grid dir:
    #   _configs/        per-combo YAML configs (audit trail)
    #   grid_progress.json   live checkpoint, rewritten atomically per combo
    #   manifest.json    one-shot summary written at the end
    configs_dir  = os.path.join(grid_dir, "_configs")
    progress_path = os.path.join(grid_dir, "grid_progress.json")
    manifest_path = os.path.join(grid_dir, "manifest.json")
    os.makedirs(configs_dir, exist_ok=True)

    # Parse --seeds into a list[int]. Empty/whitespace-only entries are
    # silently dropped; an entirely-empty list collapses back to "no seed sweep".
    seeds: list[int] | None = None
    if args.seeds:
        try:
            seeds = [int(s.strip()) for s in args.seeds.split(",") if s.strip()]
        except ValueError:
            print(f"Error: --seeds must be a comma-separated list of integers, got {args.seeds!r}",
                  file=sys.stderr)
            sys.exit(1)
        if not seeds:
            seeds = None
        else:
            print(f"Seed sweep enabled: every combo will run with seeds {seeds}")

    # Materialise the full work list. --skip-topsis-weights drops the
    # 72-call sensitivity pass entirely (cuts ~37% of the runtime).
    all_combos = generate_main_grid(grid_dir, seeds)
    if not args.skip_topsis_weights:
        all_combos += generate_topsis_sensitivity(grid_dir, seeds)

    # Recompute the per-pass counts for the summary print. Regenerating is
    # cheap and avoids tracking lengths through the conditional above.
    n_main       = len(generate_main_grid(grid_dir, seeds))
    n_sensitivity = 0 if args.skip_topsis_weights else len(generate_topsis_sensitivity(grid_dir, seeds))
    n_total      = len(all_combos)
    # Main-grid binary calls expand to 5 algo runs each; sensitivity calls
    # are 1:1. The runtime estimate assumes algo runs are roughly equal length.
    est_algo_runs = n_main * len(ALGORITHMS) + n_sensitivity
    # Each algo run is the generation window plus up to DRAIN_S of drain
    # plus ~RUN_OVERHEAD_S of reset/seed/export; v1 runs always hit the drain
    # deadline, so this is the realistic upper bound.
    per_run_s    = args.duration + DRAIN_S + RUN_OVERHEAD_S
    est_hours    = est_algo_runs * per_run_s / 3600

    print(f"\nGrid summary:")
    print(f"  Main grid runs      : {n_main} binary calls × {len(ALGORITHMS)} algorithms = {n_main * len(ALGORITHMS)} algo runs")
    if not args.skip_topsis_weights:
        print(f"  TOPSIS sensitivity  : {n_sensitivity} binary calls × 1 algorithm = {n_sensitivity} algo runs")
    print(f"  Total binary calls  : {n_total}")
    print(f"  Total algo executions: {est_algo_runs}")
    print(f"  Estimated runtime   : <= {est_hours:.1f}h at {per_run_s}s/run "
          f"({args.duration}s window + {DRAIN_S}s drain + {RUN_OVERHEAD_S}s overhead)")
    print()

    if args.emit_configs_only:
        for c in all_combos:
            cfg = make_config(base_cfg, c["topology"], CAPACITY_PROFILES[c["capacity_name"]],
                              args.duration, topsis_weights=c["topsis_weights"],
                              algorithm=c["algorithm"], seed=c.get("seed"))
            write_temp_config(cfg, os.path.join(configs_dir, f"{c['combo_id']}.yaml"))
        print(f"Wrote {len(all_combos)} configs to {configs_dir}")
        return

    if args.dry_run:
        # Dry-run prints the work plan and exits with no side effects on
        # the filesystem beyond directory creation done above. Used to
        # sanity-check grid composition before committing a multi-hour run.
        print("DRY RUN — combinations that would execute:\n")
        for i, c in enumerate(all_combos, 1):
            topo = c["topology"]
            algo = c["algorithm"] or "all-5"
            w    = c["topsis_weights"]["label"] if c["topsis_weights"] else "balanced"
            print(f"  {i:3d}. {c['combo_id']:<55} algo={algo:<10} weights={w}")
        return

    # Resolve the binary. If the user pre-built one (e.g. with optimisations
    # or a specific Go version), respect it; otherwise build a fresh copy
    # inside the grid dir so the binary travels with its results.
    binary = args.binary
    if not binary:
        binary = os.path.join(grid_dir, "_benchmark_bin")
        if not build_binary(binary):
            sys.exit(1)
    elif not os.path.exists(binary):
        print(f"Error: binary not found: {binary}", file=sys.stderr)
        sys.exit(1)

    # `completed` is the resume-skip set; `runs_log` is the running history
    # rewritten to grid_progress.json after every combo.
    completed = load_progress(progress_path)
    runs_log  = []

    # Preserve prior log entries on --resume so the final manifest reflects
    # the full history, not just this session.
    if args.resume and os.path.exists(progress_path):
        with open(progress_path) as f:
            runs_log = json.load(f).get("runs", [])

    print(f"Starting grid benchmark. Press Ctrl-C to interrupt (resume with --resume {grid_dir})\n")

    start_time = time.time()

    for idx, combo in enumerate(all_combos, 1):
        combo_id = combo["combo_id"]

        # Resume fast-path: combos previously marked "done" are skipped
        # without touching the binary or filesystem.
        if combo_id in completed:
            print(f"[{idx}/{n_total}] SKIP (already done): {combo_id}")
            continue

        topo     = combo["topology"]
        cap_name = combo["capacity_name"]
        cap      = CAPACITY_PROFILES[cap_name]
        algorithm = combo["algorithm"]
        weights  = combo["topsis_weights"]

        print(f"[{idx}/{n_total}] {combo_id}")

        # Synthesise the per-combo YAML and write it under _configs/ so the
        # exact configuration used for this result is permanent and auditable.
        cfg_path = os.path.join(configs_dir, f"{combo_id}.yaml")
        cfg      = make_config(base_cfg, topo, cap, args.duration,
                               topsis_weights=weights, algorithm=algorithm,
                               seed=combo.get("seed"))
        write_temp_config(cfg, cfg_path)

        # The Go binary will write its JSON output here.
        out_dir = combo["output_dir"]
        os.makedirs(out_dir, exist_ok=True)

        # Time the subprocess so the manifest records elapsed wall-time per
        # combo — useful for spotting outliers or scheduling cost regressions.
        t0      = time.time()
        success = run_binary(binary, cfg_path, out_dir, args.duration, algorithm)
        elapsed = time.time() - t0

        status = "done" if success else "failed"
        print(f"  → {status} in {elapsed:.0f}s")

        # Per-combo comparison plots only make sense when multiple algorithms
        # ran in the same dir (i.e. the main grid). The TOPSIS-sensitivity
        # passes write a single-algorithm dir, so we skip plotting them here.
        if success and algorithm is None:
            run_per_config_plots(out_dir)

        # Append a structured record for this combo. base_combo_id is the
        # cross-seed grouping key; combo_id is unique per execution.
        runs_log.append({
            "combo_id":       combo_id,
            "base_combo_id":  combo.get("base_combo_id", combo_id),
            "topology":       topo,
            "capacity":       cap_name,
            "topsis_weights": weights,
            "algorithm":      algorithm,
            "seed":           combo.get("seed"),
            "output_dir":     out_dir,
            "status":         status,
            "duration_s":     args.duration,
            "elapsed_s":      round(elapsed, 1),
            "completed_at":   ts(),
        })
        if success:
            completed.add(combo_id)
        # Atomic checkpoint after every combo: even Ctrl-C between combos
        # leaves the on-disk progress file fully valid and resumable.
        save_progress(progress_path, runs_log)

        if not success:
            # Failed combos are logged but never abort the grid — they're
            # retried automatically on the next --resume.
            print(f"\n[WARN] run failed for {combo_id}, continuing.", file=sys.stderr)
            print(f"  Resume failed combos with: python scripts/grid_benchmark.py --resume {grid_dir}", file=sys.stderr)

    total_elapsed = time.time() - start_time

    # Write the final manifest. This is the canonical artifact downstream
    # plotting and statistical scripts consume; grid_progress.json is the
    # in-flight checkpoint, manifest.json is the immutable summary.
    manifest = {
        "grid_dir":       os.path.abspath(grid_dir),
        "created_at":     ts(),
        "duration_per_run": args.duration,
        "total_elapsed_s": round(total_elapsed, 1),
        "base_config":    os.path.abspath(args.config),
        "provenance":     collect_provenance(),
        "runs":           runs_log,
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    done   = sum(1 for r in runs_log if r["status"] == "done")
    failed = sum(1 for r in runs_log if r["status"] == "failed")
    print(f"\nGrid complete: {done} done, {failed} failed in {total_elapsed/3600:.1f}h")
    print(f"Results:  {grid_dir}/")
    print(f"Manifest: {manifest_path}")
    if failed:
        print(f"\nFailed combos ({failed}):")
        for r in runs_log:
            if r["status"] == "failed":
                print(f"  {r['combo_id']}")
        print(f"  Resume with: python scripts/grid_benchmark.py --resume {grid_dir}")
        sys.exit(1)
    print(f"\nGenerate comparative plots:")
    print(f"  python scripts/grid_plot.py {grid_dir}")


if __name__ == "__main__":
    main()

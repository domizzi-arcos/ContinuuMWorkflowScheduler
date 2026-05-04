"""
Enriches the Globus Compute dataset with inferred resource requirements
(cpu, memory_mb, disk_gb, wan_latency_ms, tier) needed by the distributed
workflow scheduler simulation.

Outputs: GlobusComputeDataset-v1-enriched.csv
"""

import pandas as pd
import numpy as np

# ── 1. Load ──────────────────────────────────────────────────────────────────

print("Loading CSVs…")
df_tasks     = pd.read_csv("GlobusComputeDataset-v1-tasks.csv")
df_endpoints = pd.read_csv("GlobusComputeDataset-v1-endpoints.csv")
df_functions = pd.read_csv("GlobusComputeDataset-v1-functions.csv")

print(f"  tasks:     {len(df_tasks):,}")
print(f"  endpoints: {len(df_endpoints):,}")
print(f"  functions: {len(df_functions):,}")

# ── 2. Join ───────────────────────────────────────────────────────────────────

df = df_tasks.merge(df_endpoints, on="endpoint_uuid", how="left")
df = df.merge(
    df_functions[["function_uuid", "loc", "cyc_complexity", "num_of_imports"]],
    on="function_uuid",
    how="left",
)

# ── 3. Compute execution duration (nanoseconds → milliseconds) ────────────────
# execution_end and execution_start are nanosecond Unix timestamps.
# total_time / waiting fields are not used here — they mix queuing with execution.

df["execution_duration_ms"] = (df["execution_end"] - df["execution_start"]) / 1e6

# ── 4. Clean ──────────────────────────────────────────────────────────────────

n_before = len(df)
df = df.dropna(subset=["execution_duration_ms", "loc", "cyc_complexity",
                        "num_of_imports", "argument_size"])
df = df[df["execution_duration_ms"] > 0]

# Cap extreme outliers at per-tier 99th percentile (applied after tier mapping)
print(f"  Rows after cleaning: {len(df):,} (dropped {n_before - len(df):,})")

# ── 5. Map endpoint_type → tier ───────────────────────────────────────────────
# Mapping rationale:
#   edge  — single-node, low-resource environments (LocalProvider, ThreadPoolEngine)
#   fog   — containerised or middleware clusters (Kubernetes, GlobusComputeEngine)
#   cloud — HPC batch schedulers with large shared resources (Slurm, PBS, Cobalt, LSF)

TIER_MAP = {
    "LocalProvider":      "edge",
    "ThreadPoolEngine":   "edge",
    "KubernetesProvider": "fog",
    "GlobusComputeEngine":"fog",
    "SlurmProvider":      "cloud",
    "PBSProProvider":     "cloud",
    "CobaltProvider":     "cloud",
    "LSFProvider":        "cloud",
}
df["tier"] = df["endpoint_type"].map(TIER_MAP).fillna("fog")  # unknown → fog

print("\nTier distribution:")
print(df["tier"].value_counts())

# ── 6. Per-tier 99th-percentile cap on duration ───────────────────────────────

for tier, group in df.groupby("tier"):
    cap = group["execution_duration_ms"].quantile(0.99)
    df.loc[df["tier"] == tier, "execution_duration_ms"] = \
        df.loc[df["tier"] == tier, "execution_duration_ms"].clip(upper=cap)

# ── 7. Derive proxy signals ───────────────────────────────────────────────────
# complexity_score — proxy for CPU demand (more complex code needs more cores)
# argument_size    — proxy for memory / disk demand (bigger payloads need more RAM/disk)

df["complexity_score"] = df["loc"] * df["cyc_complexity"]

# ── 8. Resource bucketing (quantile-based, per tier) ─────────────────────────
# Resource ranges come directly from config.yaml.
#
# CPU (cores)     edge: 1-2    fog: 4-8    cloud: 16-32
# Memory (MB)     edge: 128-768  fog: 2048-8192  cloud: 16384-65536
# Disk (GB)       edge: 0-20   fog: 40-140  cloud: 400-1600
# Latency (ms)    representative midpoint of each tier's wan_latency range

TIER_RESOURCES = {
    #           cpu_levels        mem_levels              disk_levels       latency
    "edge":  ([1, 1, 2, 2],     [128, 256, 512, 768],   [0, 5, 10, 20],   7),
    "fog":   ([4, 4, 6, 8],     [2048, 4096, 6144, 8192],[40, 80, 110, 140], 32),
    "cloud": ([16, 20, 24, 32], [16384, 32768, 49152, 65536],[400, 800, 1200, 1600], 120),
}


def assign_by_quantile(series: pd.Series, levels: list) -> pd.Series:
    """Map values to one of len(levels) buckets using quartile boundaries."""
    q = [0.25, 0.50, 0.75]
    cuts = series.quantile(q).values  # 3 cut-points → 4 bins
    result = pd.Series(index=series.index, dtype=int)
    result[series <= cuts[0]] = levels[0]
    result[(series > cuts[0]) & (series <= cuts[1])] = levels[1]
    result[(series > cuts[1]) & (series <= cuts[2])] = levels[2]
    result[series > cuts[2]] = levels[3]
    return result


cpu_col    = pd.Series(index=df.index, dtype=int)
memory_col = pd.Series(index=df.index, dtype=int)
disk_col   = pd.Series(index=df.index, dtype=int)
latency_col= pd.Series(index=df.index, dtype=int)

for tier, (cpu_lvl, mem_lvl, disk_lvl, latency) in TIER_RESOURCES.items():
    mask = df["tier"] == tier
    group = df[mask]
    if group.empty:
        continue
    cpu_col[mask]    = assign_by_quantile(group["complexity_score"], cpu_lvl).values
    memory_col[mask] = assign_by_quantile(group["argument_size"],    mem_lvl).values
    disk_col[mask]   = assign_by_quantile(group["argument_size"],    disk_lvl).values
    latency_col[mask]= latency

df["cpu"]            = cpu_col
df["memory_mb"]      = memory_col
df["disk_gb"]        = disk_col
df["wan_latency_ms"] = latency_col

# ── 9. Round execution_duration_ms to integer ms ─────────────────────────────

df["execution_duration_ms"] = df["execution_duration_ms"].round().astype(int)
df["execution_duration_ms"] = df["execution_duration_ms"].clip(lower=1)

# ── 10. Save ──────────────────────────────────────────────────────────────────

output_cols = [
    "task_uuid", "function_uuid", "endpoint_uuid",
    "endpoint_type", "tier",
    "execution_duration_ms",
    "cpu", "memory_mb", "disk_gb", "wan_latency_ms",
    "argument_size", "loc", "cyc_complexity", "num_of_imports",
]

out = df[output_cols].reset_index(drop=True)
out.to_csv("GlobusComputeDataset-v1-enriched.csv", index=False)
print(f"\nSaved {len(out):,} rows → GlobusComputeDataset-v1-enriched.csv")

# ── 11. Summary stats ─────────────────────────────────────────────────────────

print("\nEnriched dataset summary:")
print(out.groupby("tier")[["execution_duration_ms","cpu","memory_mb","disk_gb","wan_latency_ms"]]
        .describe(percentiles=[.25,.5,.75])
        .round(1))

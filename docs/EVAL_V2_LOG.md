# eval-v2 log

Running log of the evaluation upgrade. One section per work package; gate
decisions are recorded verbatim.

## Environment (2026-10-12)

- Go 1.27.1 (`go.mod` requires 1.25). Python 3.14.4, pandas 2.3.3, numpy 2.3.5.
- Baseline on `main` (9ac5f9c): build, vet, `go test ./...` and `gofmt -l .`
  all clean.

### Dataset (rebuilt, approved by the user)

The enriched CSV was not present and was regenerated with
`globus dataset/enrich_dataset.py` (no randomness) from the raw Globus
Compute v1 files.

| File | SHA-256 |
|---|---|
| `GlobusComputeDataset-v1-tasks.csv` (input) | `8b60f0f4874f7ad93453cdd82434f2470a1334a03711584078d1f4d727208b67` |
| `GlobusComputeDataset-v1-endpoints.csv` (input) | `037161bf12b6b6441c80433203056ac0a491f59d1aae4c1da08e1426e21a7876` |
| `GlobusComputeDataset-v1-functions.csv` (input) | `2b7a13214cf869281d7ec9ba4aed44704bfae9a1cf4def8314fb5222a59ea9b4` |
| `GlobusComputeDataset-v1-enriched.csv` (output) | `c5f02b084810f9a21598ca6fc280a9d5fa238df3920ead9e90bd72a56a160b2b` |

Output: 1,533,771 rows, 262,873,957 bytes (docs say 262 MB). Tiers: edge
1,334,290, cloud 168,683, fog 30,798. The original enriched file is not
available, so it cannot be checked byte-for-byte. `LoadTasksFromCSV`
reservoir-samples in file order, so any row-order difference would change
the sampled templates. This is one more reason v2 cannot be mixed with v1.

## WP0 — Campaign runner

### 0.1 / GATE G0

- `git log --all -- cmd/benchmark`: `cmd/benchmark/main.go` existed
  (7f6d172, 3a58277) and was moved to `benchmark/main.go` in 8355f81 with
  only a log message and the `NewAlgorithm` util argument changed.
- Neither version accepts `-output` or `-algorithm`; both take only
  `-duration -servers -config`. The runner `scripts/grid_benchmark.py`
  calls was never committed. The sibling checkout named in
  `WHERE-IS-THE-DATA.md` is not on this machine, and no v1 `results/grid_*`
  exist locally.
- **Decision (user): Track B.** All policies are re-run; v2 results are not
  mixed with the v1 campaign. WP7 validation against the paper table cannot
  run locally (no v1 JSON).
- `config.local.yaml` was recovered from git (`3a58277:config.local.yaml`).
- TOPSIS util weight (user did not know): `make_config` in
  `grid_benchmark.py` always writes `BALANCED_WEIGHTS` = (0.4, 0.3, 0.3,
  util 0.25), so the main grid used util 0.25 whatever the base config said.
  This assumes the published campaign used this version of the script.

### 0.2–0.4 Changes

- `internal/runner`: session + drain logic shared by both runners.
  `benchmark/main.go` behaviour unchanged (still `-servers`, own output dir,
  3-minute drain, export errors logged not returned).
- `internal/algos`: leaf package with the canonical policy list (created in
  WP0 rather than WP2 because `cmd/benchmark` needs the default list; WP2
  wires it into config/scheduler/scripts).
- `cmd/benchmark`: `-config -output -duration(30) -drain(180) -algorithm
  -algorithms`; counts from config; validates names (the scheduler factory
  silently falls back to round robin on unknown names); exits non-zero on
  any failure.
- `results.ExportRun(..., RunInfo)`; `Export` is a thin wrapper. New
  optional `meta.provenance` {git_commit, git_dirty, go_version, host_hash,
  args}; host hash = first 16 hex of SHA-256(hostname), same in Go and
  Python. Schema test pins the legacy key set.
- `configs/campaign_base.yaml`: verbatim recovered `config.local.yaml` plus a
  header listing what the grid script overrides.
- `grid_benchmark.py`: defaults `--config configs/campaign_base.yaml`,
  `--duration 30`; manifest gains `provenance`; runtime estimate now
  `runs × (duration + 180 + 3)` (pulled forward from WP6).
- Makefile `reproduce-grid` passes `--duration 30`.
- `.gitignore`: `results/` also matched `internal/results/`, hiding new
  source files there; added `!internal/results/`.

### Notes for later WPs

- A failed combo leaves earlier policies' JSON in its output dir, and
  `--resume` re-runs the whole combo into the same dir. WP7 `load.py` must
  keep the newest file per (combo, algorithm) and warn on duplicates.
- Both `git status --porcelain` (manifest) and Go's `vcs.modified`
  (per-run meta) count untracked files, so the untracked `Claude.md` marks
  every run dirty unless it is excluded locally (`.git/info/exclude`).

### Acceptance

- `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .`: clean.
- `grid_benchmark.py --dry-run --seeds 1 --skip-topsis-weights`: 24 combos
  (96 without `--skip-topsis-weights`: +72 TOPSIS weight-sensitivity calls).
  Estimate for the v1 campaign (seeds 1–3, 5 policies): ≤ 21.3 h.
- Pilot: `results/pilot_wp0_20261012_101920/` (uniform_m × medium × seed 1,
  round_robin + topsis, `cmd/benchmark` built at 476b0d0, dirty only because
  of the untracked `Claude.md`). Exit 0, two JSON files, legacy key set plus
  `meta.provenance`. 7 min 6 s total.

| Policy | Tasks | Workflows done | Cost ($) | Energy (Wh) | Avg makespan (ms) | Edge/Fog/Cloud tasks | Drained |
|---|---|---|---|---|---|---|---|
| round_robin | 55 | 6/15 | 0.0090 | 2.05 | 3,568 | 14/15/26 | no (180.2 s) |
| topsis | 55 | 6/15 | 0.0081 | 1.82 | 3,556 | 0/49/6 | no (180.9 s) |

### Finding: two causes of non-draining, not one

Redis end state after the topsis pilot run: 55 DONE, 10 BLOCKED, 6 PENDING,
4 RUNNING.

1. **Unschedulable tasks, even in the medium profile.** All 6 PENDING are
   cloud-tier tasks needing 1,200–1,600 GB disk and/or 65,536 MB memory.
   With seed 1 the largest cloud node has 49,152 MB and 500 GB disk. The
   binding resources here are disk and memory, not CPU. §4 of the plan only
   anticipated the small profile's CPU shortfall.
2. **Tasks longer than window + drain.** All 4 RUNNING are edge tasks with
   durations of 396–404 s, near the per-tier 99th-percentile cap applied by
   `enrich_dataset.py`. Started ~20 s before the drain began, they cannot
   finish inside the 180 s deadline whatever the policy does.

Implication for WP1: the audit must also flag templates whose duration
exceeds what fits in the run (≥ drain deadline), and report both causes
separately. For WP4, `demand_scale` / `drop_unschedulable` fix cause 1 only;
cause 2 needs a separate decision at GATE G1 (e.g. a duration cap or a
longer drain).

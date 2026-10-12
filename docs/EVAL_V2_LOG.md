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

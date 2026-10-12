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
   cloud-tier tasks. Per-resource maxima of the seed-1 cloud tier are 32
   cores, 49,152 MB and 1,300 GB, on different nodes. Tasks needing
   65,536 MB fit no node; tasks needing (24 cores, 49,152 MB, 1,200 GB) fit
   each resource somewhere but no single node has all three ("joint"). (An
   earlier version of this entry quoted the CPU-largest node's values as the
   maxima; corrected during WP1.)
2. **Tasks longer than window + drain.** All 4 RUNNING are edge tasks with
   durations of 396–404 s, near the per-tier 99th-percentile cap applied by
   `enrich_dataset.py`. Started ~20 s before the drain began, they cannot
   finish inside the 180 s deadline whatever the policy does.

Implication for WP1: the audit must also flag templates whose duration
exceeds what fits in the run (≥ drain deadline), and report both causes
separately. For WP4, `demand_scale` / `drop_unschedulable` fix cause 1 only;
cause 2 needs a separate decision at GATE G1 (e.g. a duration cap or a
longer drain).

### Finding: `IntRange` min 0 is treated as unset

`config.IntRange.ApplyDefaults` (`internal/config/config.go:73`) replaces a
configured `min: 0` with `max`. Every capacity profile gives edge disk
`min: 0`, so every edge node gets the profile's maximum disk (10 / 20 / 40 GB
for small / medium / large). This held in the v1 campaign too. Not changed
(hard rule 3: no default behaviour changes); the seeder golden test goes
through `config.Load` so it pins this behaviour.

## WP1 — Feasibility audit

### Changes

- Golden tests recorded **before** refactoring (029b580):
  `internal/seeder/testdata/servers.golden` (3 profiles × 4 topologies ×
  seeds {1,2,3,42}, via `config.Load`) and
  `internal/generator/testdata/draws.golden` (7 seed/priority/pool cases ×
  20 workflows through the live `instantiateWorkflow`).
- `seeder.BuildServers(cfg)`: pure; `SeedServers` stores its output. Test:
  equal to what `SeedServers` stores for every golden config. Side effect: a
  tier error now aborts before anything is stored (before, earlier tiers
  were already in Redis; the run aborted either way).
- `generator.drawWorkflow` + `ReplayWorkflows(tasks, seed, prioMin,
  prioMax, n)`; `WorkflowDraw.Seq` = creation index. Test: replay equals
  live draws for every golden case. Signature deviation: `drawWorkflow`
  also takes the template list. `generator.SubmissionInterval()` exported
  for the audit.
- `grid_benchmark.py --emit-configs-only`.
- `internal/audit` (pure, tested) + `cmd/audit`. Beyond the plan, it also
  applies a **time bound**: a workflow is time-infeasible when, with every
  server idle and each task on its fastest feasible server, submission time
  (seq × 2 s) + critical path > window + drain. Both bounds are
  policy-independent: a flagged workflow cannot complete under any policy.
  Outputs: per-combo CSV, per-template CSV, optional per-run join (`-grid`).

### Validation on the pilot

`results/pilot_wp0_20261012_101920/audit_pilot_join.csv`: both runs have
9/15 incomplete workflows; the audit predicts exactly those 9 (7 capacity,
2 time). 0 predicted-infeasible workflows completed; replayed templates
match the runs' creation order (0 mismatches).

### Audit results (`results/audit_20261012_103506/`)

24 combos × seeds 1–5 = 120 configs, 120 templates each, deadline 210 s.
Means per combo, first 15 workflows (what a 30 s window creates;
`audit.csv` has the plan's 16):

| Capacity | Unschedulable templates | of which edge / fog / cloud trace tier | Joint-only | Too-long templates | WF infeasible: capacity | time | any |
|---|---|---|---|---|---|---|---|
| small | 16.1 | 0.0 / 0.5 / 15.6 | 0.1 | 9.1 | 9.1 | 1.8 | 10.9 / 15 |
| medium | 6.4 | 0.0 / 0.0 / 6.4 | 0.7 | 9.2 | 5.2 | 3.3 | 8.5 / 15 |
| large | 0.0 | 0 / 0 / 0 | 0.0 | 9.2 | 0.0 | 4.6 | 4.6 / 15 |

By topology, means range from 8.2 to 8.8 infeasible of 16 workflows.
Topologies with the same cloud-node count give identical audits (e.g.
uniform_l and cloud_heavy, both 8 cloud nodes), because the cloud tier's
maxima set feasibility.

- Binding resources, summed over the 120 combos: CPU 714, memory 810,
  disk 666 template-level exceedances (a template can exceed several);
  30 joint-only cases.
- Too-long templates are edge (1,056) and fog (42) trace-tier tasks with
  durations of 210–963 s (the per-tier p99 clip in `enrich_dataset.py`).
- Variation across seeds is large: infeasible workflows per run range from
  2 (large, seed 4) to 13 (small, seed 5).

### Share of v1 incompleteness explained

No v1 run JSON exists locally, so no per-run join is possible. Indirect
check against the paper's figures: for seeds 1–3, the audit's upper bound on
completable workflows is **6.69 of 15 per run** (small 3.83, medium 6.58,
large 9.67). The paper reports 6.5–6.6 completed per run. If the rebuilt
dataset matches the original, capacity + time infeasibility account for
≈ 98% of incomplete workflows (8.31 of ≈ 8.45). Without the time bound,
capacity alone explains 4.28 per run (≈ 51%); time alone 4.03 (≈ 48%).

### GATE G1 decisions (user, 2026-10-12)

- Track: **B** (unchanged from G0).
- Capacity: **`generator.demand_scale`** (WP4 preferred option).
- Time (new, not in the original plan): **cap task durations** via a new
  option `generator.max_duration_ms` (default 0 = off), applied after
  sampling so the RNG stream is unchanged. The cap value (proposed 30 s) is
  confirmed at GATE G2 with the audit and a pilot. Rationale: durations are
  bimodal (90% of sampled templates < 5.5 s, ~8% at 3.5–16 min), so a cap
  touches ~8% of templates and makes every workflow completable, letting
  runs drain early.

## WP2 — Stronger baselines

### Changes

- Golden test first (`internal/scheduler/testdata/policies.golden`): selections of the five v1
  policies (campaign weights, random seed 42) on 500 seeded (task,
  candidate set) cases with deliberate ties. Still passes after every WP2
  change, so the v1 policies are unchanged.
- `criteria.go`: `computeCriteria` (time, cost, energy, util) extracted
  from TOPSIS with identical arithmetic; `sortedByID`, `lexLess`.
- New policies, one file each: `min_cost`, `min_energy`, `eft`,
  `weighted_sum`, `topsis_u0`, `best_fit_norm` (optional). Each copies and
  ID-sorts the candidates and never mutates the caller's slice.
- **Deviation — tie tolerance.** Ties are scores equal within a relative
  1e-9 (`tieTolerance`). Without it, mathematically equal scores (e.g.
  best_fit_norm's mean free fractions) were split by floating-point
  rounding instead of the documented tie-break. TOPSIS is untouched.
- `eft` doc comment: online EFT greedy (HEFT's server-selection step), not
  HEFT. With immediate starts and equal d, it is the lowest-latency server.
- Registry: `internal/algos` now has `Names()` (10 core, v2 default),
  `Legacy()` (v1 five; `benchmark/main.go` uses it, so its behaviour is
  unchanged) and `Optional()` (`best_fit_norm`). `config.Validate` and
  `scheduler.NewAlgorithm` use it. `cmd/benchmark` defaults to the 10.
- `scripts/algorithms.py`: names, labels, abbreviations, colours, markers,
  line styles. Used by `grid_benchmark.py`, `grid_plot.py`,
  `plot_results.py`, `generate_report.py`, `benchmark/analysis/plot.py`
  (its image now builds from the repo root). Go test
  `internal/algos/sync_test.go` fails on any divergence (mutation-checked).
- Palette: the 8 validated hues of the dataviz reference palette, assigned
  in policy order (round_robin red, best_fit violet, worst_fit green, topsis
  blue, min_cost orange, min_energy aqua, eft yellow, weighted_sum magenta);
  validator on that order: adjacent CVD ΔE ≥ 9.1, normal-vision ΔE ≥ 19.6,
  contrast WARN for aqua/yellow/magenta (labels/markers required). random is
  neutral grey; topsis_u0 / best_fit_norm share their parent's hue with
  dashed lines and hollow markers; every policy has a unique marker.
  All-pairs separation fails beyond 3 hues, so scatter plots must use
  markers (WP7).
- `heatmap` cell labels now use unique abbreviations (two-letter prefixes
  collided: TOPSIS / TOPSIS no-util, Min-Cost / Min-Energy).

### Tests

- Per policy: name, registration, nil on empty, single candidate unchanged,
  agreement with an independent oracle on 500 cases × 3 input orders
  (original, reversed, shuffled), input slice unmodified, explicit
  tie-break cases.
- `weighted_sum` ≠ `topsis` on a constructed 3-server case (b vs c) and on
  95 / 500 golden cases: the aggregation changes decisions.
- `topsis_u0` == `NewTOPSIS(0.4, 0.3, 0.3, 0)` on all 500 cases, and differs
  from `topsis` on some (the ablation is not vacuous).
- End-to-end smoke run of the five new policies in the emulator (4 s
  window): all exit cleanly and export.

### Micro-benchmark

`results/bench_20261012_104141.txt` (5 repeats, all 11 policies, n ∈ {6, 15,
24, 100}). At n = 24 every policy decides in < 5 µs (random 13 ns, TOPSIS
2.9–4.0 µs); the scheduler polls every 500 ms. Repeats vary by up to ~40%
on this laptop; WP7 should report medians.

## WP3 — Instrumentation

All JSON changes are additive; `internal/results/schema_test.go` (committed
before any field was added) pins every v1 key of every object.

- Execution: `ready_at` (entry slots at creation; others when promoted
  BLOCKED→PENDING; `CreateExecution` too), `dispatched_at` (set by
  `AssignExecution` for QUEUED, cleared by `RevertExecutionAssignment`).
- Task results: `ready_time`, `dispatch_time`, `server_power_watts`,
  `server_cost_per_hour`, `server_latency_ms` (worker → message →
  collector).
- Workflows: `seq` (= draw index of `ReplayWorkflows`, so the audit and WP7
  can match workflows across policies exactly), `completed_at`,
  `response_ms` (last completion − creation). `makespan_ms` unchanged
  (test pins first start → last completion).
- Server snapshots: `max_cpu`, `max_memory`, `max_disk`, `latency`,
  `cost_per_hour`, `power_watts`.
- `summary.end_state`: `pending`, `blocked`, `queued`, `running`,
  `pending_unschedulable` (task fits no server, `audit.Classify`), and from
  the campaign runner `drained`, `drain_seconds` (`RunInfo`). Files from
  `Export` (legacy runner, interactive app) get the counts but no drain
  fields.
- `experiment.labels` → `meta.labels`; the grid script sets `topology`,
  `capacity`, `seed`, `interval_ms` (and `weights` for sensitivity combos).
- End-to-end check (topsis, 6 s window, 20 s drain): every new field is
  populated; ready → dispatch ≈ 0.5 s (scheduler poll).

## WP4 — Benchmark-validity options

### Changes

- `generator.demand_scale` (per trace tier: cpu, memory, disk factors;
  ceil, ≥ 1 core), `generator.max_duration_ms` (new, from G1),
  `generator.drop_unschedulable`. All default off; `PrepareTasks` is the
  identity then (test) and never draws random numbers, so the dataset RNG
  stream is unchanged. Validation rejects unknown tiers, factors ≤ 0 and a
  negative cap. `models.Server.FitsWhenIdle` is the shared fit rule
  (audit, drop option).
- `grid_benchmark.py --demand-scale --max-duration-ms N
  --drop-unschedulable`; options are written to the config and
  `meta.labels` only when on (options-off configs equal the WP3 ones except
  labels).

### Audit of each option (`results/audit_wp4_20261012_110026/`)

Seeds 1–5 × 24 combos; mean infeasible workflows of the first 15:

| Option set | small | medium | large |
|---|---|---|---|
| none (v1) | 10.9 | 8.5 | 4.6 |
| demand_scale | 8.2 | 8.5 | 8.5 |
| cap 30 s | 9.1 | 5.2 | 0.0 |
| **demand_scale + cap 30 s (G1 choice)** | **5.0** | **5.2** | **5.2** |
| drop_unschedulable + cap 30 s | 0.0 | 0.0 | 0.0 |
| demand_scale + drop + cap 30 s | 0.0 | 0.0 | 0.0 |

- The 30 s cap removes all time-infeasibility; it changes 8.0% of templates
  (edge and fog trace-tier tasks).
- **demand_scale does not fix capacity infeasibility.** Factors use each
  profile's *range* maximum, but the sampled fleets rarely reach it (e.g.
  medium seed 1: largest cloud memory 49,152 of 65,536 MB), and some tasks
  fail only jointly. With factor 1.0 the medium profile is untouched, and
  the large profile gets *worse* (demands × 2–4).
- Templates dropped (means of 120) and cloud trace-tier templates kept:
  drop + cap: small 16.0 dropped (0% of cloud-class kept), medium 6.4
  (58%), large 0 (100%). scale + drop + cap: 7.2 / 6.4 / 6.4 dropped
  (55% / 58% / 58% kept).

### GATE G2 decision (user, 2026-10-12)

**demand_scale + drop_unschedulable + max_duration_ms 30000.** Every
workflow is completable in all 120 audited configs; 6–7 of 120 templates
dropped per profile, 55–58% of cloud trace-tier templates kept. Grid flags:
`--demand-scale --drop-unschedulable --max-duration-ms 30000`.

### G2 pilot (`results/pilot_g2_20261012_110402/`)

uniform_m × small × seed 1, round_robin + topsis, `--demand-scale
--drop-unschedulable --max-duration-ms 30000`, binary at 1c6b9e3. 4 of 120
templates dropped. Exit 0; total 2 min 35 s.

| Policy | Tasks | Workflows | Cost ($) | Energy (Wh) | Avg makespan (ms) | Avg response (ms) | Edge/Fog/Cloud | Drained |
|---|---|---|---|---|---|---|---|---|
| round_robin | 75 | 15/15 | 0.0246 | 13.27 | 16,180 | 16,669 | 20/23/32 | yes, 46.1 s |
| topsis | 75 | 15/15 | 0.0075 | 6.29 | 16,141 | 16,631 | 28/42/5 | yes, 46.2 s |

End state empty for both (0 pending/blocked/queued/running). Audit join:
0 incomplete, 0 predicted infeasible, 0 template mismatches. Per-run wall
time ≈ 30 + 46 + 3 ≈ 79 s here (v1: 213 s); other combos will differ, so
WP6 estimates should use measured times from C1.

## WP5 — Load axis

**Skipped** (user, 2026-10-12): cuttable; time goes to WP6/C1. Campaign C4
is therefore not available. Labels already carry `interval_ms` = 2000 and
`load.py` parses `_iv<ms>`, so WP5 can be added later without schema changes.

## WP6 — Orchestration

- `grid_benchmark.py`: `--algorithms` (→ `-algorithms`), `--topologies`,
  `--capacities`, `--drain` (→ `-drain`), `--tag` (dir
  `results/grid_<UTC>_<tag>`); `--emit-configs-only` from WP1; workload
  flags from WP4. Combo IDs unchanged. `--interval-ms` not added (WP5
  skipped); `--parallel` not implemented (optional).
- Options that change what runs are saved to `grid_options.json`; a
  `--resume` with different options is refused (tested).
- Estimate = runs × (duration + drain + 3 s), an upper bound now that runs
  drain early.

### C1 pilot (`results/grid_20261012_112405_c1/`)

```
python3 scripts/grid_benchmark.py --seeds 1 --skip-topsis-weights \
  --topologies uniform_m --capacities medium \
  --demand-scale --drop-unschedulable --max-duration-ms 30000 --tag c1
```

10 policies, exit 0, 10 min 41 s (≈ 64 s per run; every run drained in
31–33 s). All 15 workflows and 75 tasks completed under every policy.

| Policy | Cost ($) | Energy (Wh) | Makespan (ms) | Edge/Fog/Cloud tasks |
|---|---|---|---|---|
| round_robin | 0.0371 | 13.30 | 13,902 | 18/27/30 |
| random | 0.0370 | 11.43 | 12,252 | 21/23/31 |
| best_fit | 0.0108 | 5.24 | 12,207 | 63/9/3 |
| worst_fit | 0.0716 | 19.07 | 12,332 | 0/0/75 |
| topsis | 0.0149 | 7.69 | 12,192 | 2/70/3 |
| min_cost | 0.0108 | 4.54 | 12,207 | 63/9/3 |
| min_energy | 0.0108 | 4.33 | 12,176 | 63/9/3 |
| eft | 0.0108 | 4.75 | 12,208 | 63/9/3 |
| weighted_sum | 0.0148 | 7.61 | 12,185 | 33/39/3 |
| topsis_u0 | 0.0108 | 4.33 | 12,207 | 63/9/3 |

Descriptive only (one configuration, one seed, n = 1). In this cell TOPSIS
costs ≈ 38% more and uses ≈ 78% more energy than min_cost / min_energy and
its own no-utilisation ablation; the utilisation criterion moves 70 of 75
tasks to fog. Makespan differs by < 1.5% except round_robin (+14%). No
policy reproduces any v1 figure here; Track B has no v1 comparison.

Determinism: the analysis run twice on C1 gives byte-identical CSV, LaTeX
and PDF outputs.

## WP7 — Analysis pipeline

`scripts/analysis/{load,stats,figures,latex,run}.py`, `make analysis
GRID_DIRS=... [AUDIT=...]`, `make pytest` (14 tests). Outputs:
`runs/tasks/workflows.csv`, `tables/*.csv` (grand mean, wins with explicit
ties, tier share, normalised ratios with bootstrap CIs, paired Wilcoxon +
Holm, matched-workflow makespan/response, completion, per-tier, power
choice, queueing delay, feasibility join), four PDFs, `numbers.tex` +
`tab_results/tab_tiers/tab_stats.tex` (into `paper/generated/` when
`paper/` exists).

**Validation against the v1 paper table could not run**: no v1 result
JSON exists locally (G0). The table in the plan remains unchecked; if the
v1 grid directories are found, run `make analysis GRID_DIRS=<v1 grid>` and
compare.

### Behaviour check before C3 (`results/grid_20261012_114019_check/`)

Requested by the user to test the C1 finding (TOPSIS beaten on cost and
energy) on more cells before the full campaign.

```
python3 scripts/grid_benchmark.py --seeds 2 --skip-topsis-weights \
  --topologies edge_heavy,cloud_heavy \
  --algorithms topsis,topsis_u0,weighted_sum,min_cost,min_energy \
  --demand-scale --drop-unschedulable --max-duration-ms 30000 --tag check
```

30 runs, exit 0, 44 min; every run drained (≈ 52 s) and completed 15/15
workflows. Analysed together with C1 (7 configurations, `analysis_out/check`):

| Cost ratio to TOPSIS | topsis_u0 | weighted_sum | min_cost | min_energy |
|---|---|---|---|---|
| cloud_heavy / large | 0.44 | 0.55 | 0.44 | 0.44 |
| cloud_heavy / medium | 0.40 | 0.50 | 0.40 | 0.40 |
| cloud_heavy / small | 0.92 | 0.92 | 0.91 | 0.92 |
| edge_heavy / large | 0.93 | 0.93 | 0.93 | 0.93 |
| edge_heavy / medium | 1.00 | 1.00 | 1.00 | 1.00 |
| edge_heavy / small | 0.80 | 0.80 | 0.97 | 0.80 |
| uniform_m / medium (C1) | 0.73 | 0.99 | 0.73 | 0.73 |

- Geometric-mean ratio to TOPSIS over the 7 configurations (95% bootstrap
  CI): cost — topsis_u0 0.70 [0.53, 0.89], min_energy 0.70 [0.53, 0.89],
  min_cost 0.72 [0.54, 0.93], weighted_sum 0.79 [0.63, 0.94]; energy —
  min_energy 0.75 [0.65, 0.86], topsis_u0 0.76 [0.65, 0.87], min_cost 0.85
  [0.72, 1.00], weighted_sum 0.89 [0.81, 0.96]; makespan — all 0.996–1.000.
- TOPSIS was never cheaper than any of the four in any configuration
  (0 of 7). Wilcoxon p = 0.016 (the smallest possible with n = 7); not
  significant after Holm (0.141) at this n. Descriptive only.
- Placement: TOPSIS puts 0–3% of tasks on edge and 79–93% on fog in
  cloud_heavy and uniform_m, where the others put 47–84% on edge.
  topsis_u0 places like min_cost/min_energy, so the shift comes from the
  utilisation criterion. weighted_sum (same criteria and weights, with
  utilisation) also shifts to fog in cloud_heavy, so the criteria rather
  than the aggregation drive it there. Likely mechanism (not verified in
  code): edge nodes have 1–2 cores, so their post-placement utilisation is
  high, and vector normalisation over a column dominated by cloud prices
  compresses the edge–fog cost difference. The gap is largest when cloud
  nodes dominate the candidate set.

### TOPSIS weight check (`results/weights_check_20261012_122759/`)

Requested by the user. 15 TOPSIS runs (5 weight variants × 3
configurations, G2 workload options, 30 s window), generated by
`results/weights_check_20261012_122759/weights_check.py` (configs in `_configs/`); compared
with the default TOPSIS, topsis_u0 and min_cost runs of C1 and the check
grid. All runs drained and completed 15/15 workflows. Cost relative to
min_cost (time/cost/energy/util weights as configured, before
renormalisation):

| Variant | cloud_heavy/medium/s2 | uniform_m/medium/s1 | edge_heavy/small/s2 |
|---|---|---|---|
| 0.4/0.3/0.3, util 0 (topsis_u0) | 1.00 | 1.00 | 0.82 |
| util 0.05 | 1.00 | 1.00 | 1.00 |
| util 0.10 | 1.01 | 1.01 | 1.00 |
| **util 0.25 (default)** | **2.53** | **1.38** | **1.03** |
| util 0.50 | 3.55 | 1.38 | 1.03 |
| cost-dominant 0.15/0.70/0.15, util 0 | 1.00 | 1.00 | 1.00 |
| cost-dominant, util 0.25 | 1.01 | 1.38 | 0.85 |

- The cost penalty appears between util 0.10 and 0.25 and grows with the
  utilisation weight. At util ≤ 0.10 TOPSIS matches min_cost on cost even
  though, in cloud_heavy, it already moves most tasks from edge to fog
  (edge 0–3%); the large penalty comes with the share sent to cloud nodes
  (21% at 0.25, 36% at 0.50).
- A dominant cost weight offsets the utilisation penalty in cloud_heavy and
  edge_heavy (in edge_heavy/small it is 15% cheaper than min_cost: greedy
  per-task cost minimisation is not optimal over a run) but not in
  uniform_m/medium.
- Energy follows cost; makespan varies by < 3% across all variants.
- One seed per configuration: descriptive only.

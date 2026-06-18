# scripts/ Internals Reference

Per-function input/output/aggregation reference for every script in `scripts/`. Generated from the `bench-notes.*.json` files emitted by the multi-agent cleanup pass; those JSON files live alongside this document and can be regenerated whenever a script changes.

For the *narrative* explanation of the benchmark pipeline see [../BENCHMARK_VALIDATION.md](../BENCHMARK_VALIDATION.md).

---

## Live monitoring + utility scripts (BENCH-D bundle)

### `dashboard.py`

Live HTTP dashboard on :8765 monitoring grid benchmark progress, health (Go binary / Redis / RabbitMQ), current combo, ETA, stall detection, per-combo plot gallery, and final artifact downloads. Browser meta-refreshes every 5s.

#### `latest_grid_dir`

- **Inputs**: none (reads RESULTS_ROOT constant)
- **Outputs**: Path | None (newest results/grid_* by mtime, or None when no run exists)
- **Aggregations**: argmax over Path.stat().st_mtime for results/grid_* glob

#### `safe_load_json`

- **Inputs**: path: Path
- **Outputs**: parsed dict | list | None (None on FileNotFound/JSONDecodeError/OSError)
- **Aggregations**: none; defensive single-file read

#### `ps_grid_processes`

- **Inputs**: none (shells out to `ps -ax -o pid=,etime=,pcpu=,command=`)
- **Outputs**: list[{pid, etime, pcpu, cmd}] for rows containing 'grid_benchmark.py' or '_benchmark_bin'
- **Aggregations**: filter ps rows by substring match; split each row into 4 fields

#### `docker_health`

- **Inputs**: none (three `docker exec`/`docker ps` subprocess calls)
- **Outputs**: {redis: status_str|None, rabbitmq: status_str|None, redis_keys: int|None, rabbit_msgs: int|None}
- **Aggregations**: sum of message counts across all RabbitMQ queues (parsed from rabbitmqctl list_queues -q output); int cast of redis-cli DBSIZE

#### `collect_state`

- **Inputs**: grid_dir: Path
- **Outputs**: flat state dict for renderers: grid metadata, runs[], combos_done, algo_files[], algos_done, active_combo, latest_file/mtime/age_sec, per_algo aggregates, combos_with_plots, auto_status, report_pdf_exists, comparative_pdfs, procs, docker, total_combos, total_algos, eta_seconds, avg_combo_sec
- **Aggregations**: count of runs with status=='done'; rglob and sort of result JSONs by mtime excluding skip-list; per-algorithm sums of total_tasks_completed and lists of throughput/makespan; max-mtime selection of newest topo_* dir; average elapsed_s over done runs for ETA; fallback ETA from cadence between first and last algo files when no combo is yet done

#### `fmt_duration`

- **Inputs**: seconds: float | None
- **Outputs**: 'Xs' | 'Xm Ys' | 'Xh Ym' | '—' (em-dash for None)
- **Aggregations**: divmod for hours/minutes split

#### `fmt_age`

- **Inputs**: seconds: float | None
- **Outputs**: alias of fmt_duration; returns em-dash for None
- **Aggregations**: none

#### `render_pipeline`

- **Inputs**: state: dict
- **Outputs**: HTML string for the auto_plot watcher status panel + final artifact download links
- **Aggregations**: lookup of status into label_map and color_map; conditional sub-text per status

#### `render_plot_gallery`

- **Inputs**: state: dict, selected_combo: str | None
- **Outputs**: HTML for combo-selection chip row + thumbnail grid for the selected combo (or placeholder text)
- **Aggregations**: iterates state['combos_with_plots'] and PLOT_NAMES to emit thumbnails

#### `render_html`

- **Inputs**: state: dict, selected_combo: str | None
- **Outputs**: full HTML page string (UTF-8 encodable)
- **Aggregations**: computes pct_combos = combos_done/total_combos*100, pct_algos similarly; computes avg throughput and avg makespan per algorithm via list-mean over per_algo['throughput']/['makespan']; reverses runs[] and slices last 8 for recent table; sets stalled = latest_age_sec > 300

#### `Handler.log_message`

- **Inputs**: format: str, *args
- **Outputs**: None (suppresses default access log)
- **Aggregations**: none

#### `Handler.do_GET`

- **Inputs**: self.path (HTTP request path + query string)
- **Outputs**: writes HTTP response: dashboard HTML, plot file, artifact file, or 404
- **Aggregations**: routes by URL prefix; parses ?combo= query; calls collect_state + render_html

#### `Handler._serve_artifact`

- **Inputs**: relpath: str (after /artifact/ prefix)
- **Outputs**: HTTP response with PDF/log file body or 404
- **Aggregations**: whitelist match against THESIS_REPORT.pdf, auto_plot.log, comparative/<name>.pdf

#### `Handler._serve_plot`

- **Inputs**: relpath: str (after /plots/ prefix; expects <combo>/<name>.{png,pdf})
- **Outputs**: HTTP response with image/PDF body or 404
- **Aggregations**: regex validates filename; Path.relative_to(grid_dir) check prevents traversal

#### `main`

- **Inputs**: sys.argv[1] (optional grid dir), DASHBOARD_HOST env var
- **Outputs**: process exit code: 0 (clean shutdown), 1 (grid dir missing), 2 (port in use)
- **Aggregations**: best-effort LAN IP discovery via UDP socket trick; starts ThreadingHTTPServer.serve_forever

### `auto_plot.py`

Background daemon that polls grid_progress.json every 30s and fires scripts/grid_plot.py + scripts/generate_report.py once all combos reach status=='done'. Writes AUTO_PLOT_STATUS.json for the dashboard and posts macOS notifications on phase transitions.

#### `write_status`

- **Inputs**: grid_dir: Path, **kv (arbitrary key/value pairs merged into payload)
- **Outputs**: atomically replaces <grid_dir>/AUTO_PLOT_STATUS.json with {updated, **kv}
- **Aggregations**: write-tmp-then-os.replace pattern for atomic publish

#### `read_status`

- **Inputs**: grid_dir: Path
- **Outputs**: parsed dict | {} (empty on missing or malformed)
- **Aggregations**: none; defensive read

#### `grid_complete`

- **Inputs**: grid_dir: Path, total: int
- **Outputs**: (done_flag: bool, done_count: int)
- **Aggregations**: count of grid_progress.json runs[] with status=='done'; comparison done_count >= total

#### `notify`

- **Inputs**: title: str, msg: str
- **Outputs**: best-effort macOS notification (silent no-op elsewhere)
- **Aggregations**: string sanitization: double-quote -> single-quote to keep AppleScript literal valid; subprocess osascript fallback

#### `log`

- **Inputs**: grid_dir: Path, line: str
- **Outputs**: appends '[timestamp] line\n' to <grid_dir>/auto_plot.log
- **Aggregations**: none

#### `main`

- **Inputs**: argv: grid_dir (positional), --total N (default 96)
- **Outputs**: exit code: 0 (success), 1 (arg/setup error), 2 (plot script timeout), 3 (unexpected exception); side effects: status file updates, log entries, osascript notifications, two subprocess invocations of grid_plot.py and generate_report.py
- **Aggregations**: 30s file-polling loop until done; phase timing via time.time() deltas; final status derived from (plot_ok, report_ok) -> 'done'/'plots_only'/'partial'/'failed'/'timeout'/'error'

### `progress.py`

One-shot CLI progress watcher. Re-renders a unicode block bar + ETA + currently-running combo every 15s by reading the newest results/grid_*/progress.json and scanning `ps aux` for the Go _benchmark_bin process.

#### `bar`

- **Inputs**: done: int, total: int, width: int = 40
- **Outputs**: string like '[██░░] done/total (pct%)'
- **Aggregations**: integer floor(width * done/total) for filled-cell count; pct = 100 * done/total

#### `read_progress`

- **Inputs**: none (reads GRID/progress.json)
- **Outputs**: (done_runs: list, failed_runs: list); ([], []) when missing/malformed
- **Aggregations**: filter runs[] into status=='done' and status=='failed' lists

#### `current_combo`

- **Inputs**: none (shells out to `ps aux`)
- **Outputs**: combo id string (e.g. 'topo_uniform_s_cap_small') or None
- **Aggregations**: scan ps output for lines containing '_benchmark_bin' and '-config'; extract token matching 'topo_*.yaml' and strip the .yaml suffix to recover the combo id

#### `eta`

- **Inputs**: done_count: int, elapsed_s: float
- **Outputs**: 'Xh Ym' or 'calculating...' (while done_count == 0)
- **Aggregations**: per_combo = elapsed_s/done_count; remaining = (TOTAL - done_count) * per_combo; divmod into hours and minutes

### `compare_topsis_fix.py`

Pre-fix vs post-fix TOPSIS validation utility. Loads a frozen OLD grid result and a NEW grid result for the same combo, prints side-by-side throughput/makespan/cost/energy tables, and computes TOPSIS throughput as a fraction of the best non-TOPSIS algorithm. Acceptance threshold is 85% (within 15 pp of best-other).

#### `latest_json_per_algo`

- **Inputs**: directory: str (combo dir containing per-algorithm result JSONs)
- **Outputs**: dict mapping algo slug -> newest parsed JSON for that algo
- **Aggregations**: argmax over meta.timestamp per algorithm prefix; ISO timestamps compare lexically

#### `fmt_row`

- **Inputs**: algo: str, s: dict (the 'summary' sub-object)
- **Outputs**: fixed-width text row showing throughput_tasks_per_min, total_tasks_completed, avg_workflow_makespan_ms, total_cost_usd, total_energy_wh
- **Aggregations**: none; pure formatting

#### `main`

- **Inputs**: argv: combo_id (positional), new_dir (positional)
- **Outputs**: stdout report with OLD and NEW tables and PASS/FAIL verdict; exits non-zero on missing dir args
- **Aggregations**: delegates to topsis_vs_best for ratio calculation; prints delta (n_ratio - o_ratio) in percentage points; PASS iff n_ratio >= 0.85

#### `main.topsis_vs_best`

- **Inputs**: grid: dict (output of latest_json_per_algo)
- **Outputs**: (topsis_tpm: float|None, best_other_tpm: float|None, ratio: float|None)
- **Aggregations**: max() over throughput_tasks_per_min of all non-topsis algorithms; ratio = topsis_tpm / best_other_tpm

### `launch_seed_sweep.sh`

Overnight 3-seed main-grid launcher. Verifies Docker + Redis are up, starts caffeinate (macOS no-sleep) detached, then launches grid_benchmark.py (24 combos x 3 seeds = 72 runs) and auto_plot.py watcher (armed at 72/72) both via nohup so the run survives terminal disconnects.

---

## `scripts/generate_report.py`

**Description.** Builds the multi-section THESIS_REPORT PDF for a completed grid run. Reads ExperimentResult JSONs (one per algorithm execution) plus grid_progress.json, aggregates seed sweeps, and emits a narrative PDF.

### Functions

#### `_aggregate_summary`

- **Inputs**: list of parsed ExperimentResult dicts (one per seed) for the same (combo, algorithm) pair
- **Outputs**: single aggregated ExperimentResult dict with seed-mean values and __stdev companions
- **Aggregations**: mean = statistics.mean(per-seed scalars) for every numeric summary field; stdev = statistics.stdev(per-seed scalars) (sample stdev, n-1 denominator); 0.0 when n==1; summary.__n_seeds = len(results); applied to top-level summary scalars AND to nested summary.by_tier.<TIER>.<field> scalars; booleans excluded explicitly (Python bool is int subclass)

#### `load_grid`

- **Inputs**: grid_dir (Path) - results/grid_<stamp>/ or results/grid_seedsweep_<stamp>/; grid_dir/grid_progress.json (orchestrator manifest); grid_dir/topo_<topology>_cap_<small|medium|large>[_w_<weight>][_seed<N>]/*.json (one per algorithm execution)
- **Outputs**: tuple (progress_dict, main_runs_list, weight_runs_list); main_runs: combos without _w_ suffix (all 5 algorithms compared); weight_runs: _w_<weight> combos (TOPSIS-only sensitivity sweep)
- **Aggregations**: bucket per-seed result files by base combo name (strip _seed<N> suffix); regex-parse combo dir: ^topo_(.+?)_cap_(small|medium|large)(?:_w_(time|cost|energy))?(?:_seed(-?\d+))?$; regex-parse algorithm filename: ^([a-z_]+)_edge\d+_fog\d+_cloud\d+_; collapse via _aggregate_summary so each (combo, algo) becomes a seed-mean record

#### `summary`

- **Inputs**: run record (dict); algorithm name (str)
- **Outputs**: summary sub-dict for the given algorithm (already seed-aggregated if multi-seed)

#### `page_text`

- **Inputs**: PdfPages handle; title (str); body (str, dedented before rendering); optional subtitle (str)
- **Outputs**: one A4 portrait text page appended to the PDF (figsize 8.27 x 11.69 inches)

#### `page_figure`

- **Inputs**: PdfPages handle; pre-built matplotlib Figure
- **Outputs**: figure appended to PDF with tight bbox; figure closed afterwards

#### `cover_page`

- **Inputs**: PdfPages handle; grid_progress dict (runs[*].elapsed_s, runs[0/-1].completed_at, runs[0].output_dir); main_runs list; weight_runs list
- **Outputs**: cover page titled 'Grid Benchmark Report' with run metadata and section TOC
- **Aggregations**: total_runs = sum(len(run['algos']) for run in main_runs + weight_runs); total_elapsed = sum(run.elapsed_s) across grid_progress.runs[*]

#### `section_methodology`

- **Inputs**: PdfPages handle; grid_progress dict (runs[*].elapsed_s, runs[*].combo_id)
- **Outputs**: Section 1 page titled '1. Methodology'
- **Aggregations**: mean(elapsed_s), stdev(elapsed_s), min(elapsed_s), max(elapsed_s) per partition (main vs _w_ combos); stdev is sample stdev (statistics.stdev, n-1 denominator); 'n/a' shown when len(xs) <= 1

#### `section_algo_comparison`

- **Inputs**: PdfPages handle; main_runs list; per-algorithm summary fields: throughput_tasks_per_min, avg_workflow_makespan_ms, total_cost_usd, total_energy_wh
- **Outputs**: Primary 3-metric win-count heatmap (Throughput, Makespan, Efficiency=Cost+Energy rank-aggregated); Section 2 page titled '2. Algorithm comparison'; Appendix 4-metric heatmap (Throughput, Makespan, Cost, Energy split); Appendix narrative page titled '2. Algorithm comparison - appendix'
- **Aggregations**: winner_for(run, metric, direction): max or min of {algo: summary[metric]}; efficiency_winner(run): average of cost-rank and energy-rank position (sorted ascending), min wins; Counter.update across all main_runs per metric, plotted as win-count matrix; primary win share = 100 * wins / total_decisions, where total_decisions = len(main_runs) * 3

#### `section_tier_routing`

- **Inputs**: PdfPages handle; main_runs list; per-algorithm summary fields: total_tasks_completed, by_tier.<EDGE|FOG|CLOUD>.tasks_completed
- **Outputs**: Stacked bar chart 'Average tier-routing distribution per algorithm (main grid)'; Section 3 page titled '3. Tier-routing fingerprints'
- **Aggregations**: per-combo tier_pct = 100 * by_tier[tier].tasks_completed / total_tasks_completed (denominator max(1, ...) to avoid zero-division); chart values = arithmetic mean of tier_pct across all combos; narrative line = 'mean +/- stdev%' per tier (sample stdev, n-1); labels suppressed for slices <= 4% to reduce clutter

#### `section_capacity_scaling`

- **Inputs**: PdfPages handle; main_runs list; per-algorithm summary fields: throughput_tasks_per_min, avg_workflow_makespan_ms, total_cost_usd, total_energy_wh
- **Outputs**: 2x2 panel figure 'Capacity scaling: small -> medium -> large (averaged over topologies)'; Section 4 page titled '4. Capacity scaling'
- **Aggregations**: for each (algorithm, capacity) compute arithmetic mean of metric across topologies; log y-axis applied to cost and energy panels (multiplicative gap up to 30-100x)

#### `section_topology`

- **Inputs**: PdfPages handle; main_runs list; per-algorithm summary fields: throughput_tasks_per_min, avg_workflow_makespan_ms, total_cost_usd, total_energy_wh
- **Outputs**: Grouped bar chart 'Throughput per topology x algorithm'; Section 5 page titled '5. Topology effects' including per-(topology, capacity) winner table
- **Aggregations**: bar chart values = arithmetic mean of throughput per (topology, algorithm) across capacity buckets; winner table: per (topology, capacity) -> max for throughput, min for makespan/cost/energy; winner table uses the FIRST run per (topology, capacity); assumes upstream seed aggregation already collapsed duplicates

#### `section_weight_sensitivity`

- **Inputs**: PdfPages handle; weight_runs list; TOPSIS summary fields: throughput_tasks_per_min, avg_workflow_makespan_ms, total_cost_usd, total_energy_wh
- **Outputs**: 2x2 boxplot figure 'TOPSIS weight sensitivity (boxplots across topology x capacity)'; Section 6 page titled '6. TOPSIS weight sensitivity' including per-metric spread table; Empty-data fallback: 'No weight-sensitivity runs found - section skipped.' if weight_runs is empty
- **Aggregations**: per weight vector: distribution of metric values across (topology, capacity) configs; boxplot defaults: IQR box, 1.5xIQR whiskers, mean line overlaid; spread = 100 * (max(per-weight means) - min(per-weight means)) / min(per-weight means); threshold rules of thumb: <5% nominal, 5-15% mild, >15% strong sensitivity

#### `section_anomalies`

- **Inputs**: PdfPages handle; main_runs list; weight_runs list (unused, kept for signature symmetry); grid_progress dict (runs[*].combo_id, runs[*].elapsed_s); per-algorithm summary fields: p95_workflow_makespan_ms, throughput_tasks_per_min
- **Outputs**: Section 7 page titled '7. Anomalies and edge cases'
- **Aggregations**: random outliers: top-5 random p95_workflow_makespan_ms across main_runs (descending sort); fast-drain baseline = median(elapsed_s of main-grid combos); fast-drain set = combos with elapsed_s < 0.85 * baseline; TOPSIS misses = combos where topsis throughput < 0.85 * max(throughput across other algorithms)

#### `section_conclusions`

- **Inputs**: PdfPages handle; main_runs list (unused); weight_runs list (unused)
- **Outputs**: Section 8 page titled '8. Conclusions for the thesis' - static narrative text

#### `main`

- **Inputs**: CLI positional arg: grid_dir; CLI flag: --output / -o (optional override; directory or file path)
- **Outputs**: PDF written to <grid_dir>/THESIS_REPORT_<grid_dir.name>.pdf by default; or to --output path (if a directory is passed, default filename is dropped inside it); stdout: 'Loaded N main-grid runs, M weight-sensitivity runs' + 'Wrote <out_path>'; exit code: 0 on success, 1 if grid_dir is not a directory

---

## `scripts/grid_benchmark.py`

**Description.** Master orchestrator for the thesis grid benchmark: 8 topologies x 3 capacity profiles x 5 algorithms (main grid, 24 binary calls -> 120 algo runs) plus 3 TOPSIS weight variants x 8 topologies x 3 profiles (sensitivity, 72 binary calls -> 72 algo runs).

### Functions

#### `ts`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=[]
- **Outputs**: `files_written`=[], `stdout`=none (returns string), `return_shape`=ISO-8601 UTC timestamp string 'YYYY-MM-DDTHH:MM:SSZ'

#### `load_base_config`

- **Inputs**: `files_read`=['<path arg> (base YAML, default config.local.yaml)'], `env_vars`=[], `cli_flags`=['--config']
- **Outputs**: `files_written`=[], `stdout`=none, `return_shape`=dict (parsed YAML config tree)

#### `make_config`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=['--duration (via duration arg)']
- **Outputs**: `files_written`=[], `stdout`=none, `return_shape`=dict (mutated deep-copy of base config: servers.{edge,fog,cloud}.count, per-tier hardware specs, scheduler.topsis_weights {time,cost,energy,util}, scheduler.algorithm, experiment.duration_seconds, optional servers.seed/scheduler.seed/generator.seed/generator.workflow_seed)

#### `write_temp_config`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=[]
- **Outputs**: `files_written`=['<grid_dir>/_configs/<combo_id>.yaml'], `stdout`=none, `return_shape`=None

#### `build_binary`

- **Inputs**: `files_read`=['./cmd/benchmark/*.go (via go build)'], `env_vars`=['GO* (Go toolchain env, implicit)'], `cli_flags`=['--binary (negative trigger: only builds when --binary unset)']
- **Outputs**: `files_written`=['<binary_path> (compiled Go binary, default <grid_dir>/_benchmark_bin)'], `stdout`=build status messages; stderr forwarded on failure, `return_shape`=bool (True on success)

#### `run_binary`

- **Inputs**: `files_read`=['<config_path> (per-combo YAML)', '<binary> (compiled benchmark)'], `env_vars`=[], `cli_flags`=['--duration (via duration arg)', '--binary (via binary arg)']
- **Outputs**: `files_written`=['<output_dir>/*.json (per-algorithm result JSONs written by Go binary)'], `stdout`=live-streamed binary stdout/stderr (not captured), `return_shape`=bool (True iff exit code 0)

#### `run_per_config_plots`

- **Inputs**: `files_read`=['scripts/plot_results.py', '<combo_dir>/*.json (read by plot_results.py)'], `env_vars`=[], `cli_flags`=[]
- **Outputs**: `files_written`=['<combo_dir>/*.png (plot images written by plot_results.py)'], `stdout`=warnings only on failure or timeout, `return_shape`=None

#### `save_progress`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=[]
- **Outputs**: `files_written`=['<progress_path>.tmp (then atomically renamed to <progress_path>, default <grid_dir>/grid_progress.json)'], `stdout`=none, `return_shape`=None

#### `load_progress`

- **Inputs**: `files_read`=['<progress_path> (grid_progress.json)'], `env_vars`=[], `cli_flags`=['--resume (indirectly: only consulted on resume path)']
- **Outputs**: `files_written`=[], `stdout`=stderr warning if file is corrupt, `return_shape`=set[str] of combo_ids with status == 'done'
- **Aggregations**: filter: runs where status == 'done' -> set of combo_id

#### `generate_main_grid`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=['--seeds (via seeds arg)']
- **Outputs**: `files_written`=[], `stdout`=none, `return_shape`=list[dict] of combo descriptors with combo_id, base_combo_id, topology, capacity_name, topsis_weights=None, algorithm=None, seed, output_dir
- **Aggregations**: cartesian product: TOPOLOGIES x CAPACITY_PROFILES x (seeds or [None]), order topo->cap->seed

#### `generate_topsis_sensitivity`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=['--seeds (via seeds arg)', '--skip-topsis-weights (negative trigger upstream)']
- **Outputs**: `files_written`=[], `stdout`=none, `return_shape`=list[dict] of combo descriptors with combo_id, base_combo_id, topology, capacity_name, topsis_weights (one of TOPSIS_WEIGHT_VARIANTS), algorithm='topsis', seed, output_dir
- **Aggregations**: cartesian product: TOPOLOGIES x CAPACITY_PROFILES x TOPSIS_WEIGHT_VARIANTS x (seeds or [None])

#### `parse_args`

- **Inputs**: `files_read`=[], `env_vars`=[], `cli_flags`=['--duration', '--config', '--output-dir', '--dry-run', '--skip-topsis-weights', '--resume', '--binary', '--seeds']
- **Outputs**: `files_written`=[], `stdout`=argparse help/usage on --help, `return_shape`=argparse.Namespace with attributes duration:int, config:str, output_dir:str|None, dry_run:bool, skip_topsis_weights:bool, resume:str|None, binary:str|None, seeds:str|None

#### `main`

- **Inputs**: `files_read`=['<args.config> (base YAML)', '<args.resume>/grid_progress.json (on resume)', '<args.binary> (when supplied)'], `env_vars`=[], `cli_flags`=['--duration', '--config', '--output-dir', '--dry-run', '--skip-topsis-weights', '--resume', '--binary', '--seeds']
- **Outputs**: `files_written`=['<grid_dir>/ (created if not resuming, default results/grid_<UTC_TIMESTAMP>/)', '<grid_dir>/_configs/<combo_id>.yaml (one per combo)', '<grid_dir>/_benchmark_bin (compiled Go binary, when --binary not supplied)', '<grid_dir>/<combo_id>/ (per-combo output dirs populated by run_binary)', '<grid_dir>/<combo_id>/*.png (when run_per_config_plots fires)', '<grid_dir>/grid_progress.json (rewritten atomically after every combo)', '<grid_dir>/manifest.json (final summary)'], `stdout`=progress lines [idx/total] combo_id -> status in Xs; grid summary header; dry-run table; final 'done/failed in Yh' summary, `return_shape`=None; sys.exit(1) on failed-combo aggregate or on early validation errors
- **Aggregations**: len(generate_main_grid) and len(generate_topsis_sensitivity) for grid sizing; estimated algo runs = n_main * len(ALGORITHMS) + n_sensitivity; estimated runtime hours = est_algo_runs * duration / 3600; per-combo elapsed = time.time() - t0; total_elapsed = time.time() - start_time; final counts: sum(status == 'done') and sum(status == 'failed') across runs_log

---

## `scripts/grid_plot.py`

**Role.** cross-configuration comparative analysis for a full grid sweep

**Input Format.** `<grid_dir>/manifest.json (from cmd/grid_benchmark) + per-combo ExperimentResult JSONs (same exporter format as plot_results.py)`

**Output Dir.** `<grid_dir>/comparative/`

**Output Count.** 10

**Output Formats.** ["pdf", "png"]

**Multi Seed Strategy.** `per-seed runs are bucketed by (topology, capacity, algorithm[, weight]) and collapsed via _aggregate_results into mean +/- stdev`

### Functions

#### `load_manifest`

- **Inputs**: grid_dir
- **Outputs**: parsed manifest.json dict (runs[], topology, capacity, algorithm, topsis_weights, seed, output_dir, status)
- **Aggregations**: none; raw JSON load

#### `load_run_jsons`

- **Inputs**: per-combo output_dir containing *.json ExperimentResults
- **Outputs**: dict {meta.algorithm -> latest ExperimentResult}
- **Aggregations**: for duplicate algorithms within a single dir, keep the entry with the latest meta.timestamp (lexicographic string compare on ISO timestamps)

#### `_aggregate_results`

- **Inputs**: list of ExperimentResult dicts (one per seed for the same combo)
- **Outputs**: single ExperimentResult with summary fields = seed means and summary[<field>__stdev] = sample stdev; summary[__n_seeds] = N
- **Aggregations**: arithmetic mean across seeds; statistics.stdev (n-1 denominator) when N>1, else 0.0; applied to top-level summary numeric fields AND nested by_tier[tier][field] numeric fields; non-numeric fields preserved from the first result via deep-copy; if N==1 returns input untouched

#### `_is_seeded`

- **Inputs**: manifest run dict
- **Outputs**: bool — True iff run carries an explicit seed value
- **Aggregations**: none

#### `load_grid_data`

- **Inputs**: parsed manifest
- **Outputs**: tuple (main_data[topo][cap][algo], sens_data[topo][cap][weight_label]) of aggregated results
- **Aggregations**: two parallel buckets accumulate per-seed result lists, then each list collapsed via _aggregate_results; runs with status != 'done' are skipped; weight variants come from manifest's topsis_weights.label (NOT parsed from filenames)

#### `save`

- **Inputs**: Figure; out_dir; name stem
- **Outputs**: writes <name>.pdf and <name>.png; creates out_dir if missing
- **Aggregations**: none (I/O helper)

#### `plot_scalability`

- **Inputs**: main_data[uniform_s|m|l][medium][algo].summary.throughput_tasks_per_min; main_data[...].summary.avg_workflow_makespan_ms; main_data[...].summary.total_cost_usd; main_data[...].summary.total_energy_wh; main_data[...].summary.total_tasks_completed
- **Outputs**: 01_scalability_throughput, 02_scalability_makespan, 03_scalability_cost, 04_scalability_energy — line plots vs total node count
- **Aggregations**: no curve fitting; piecewise-linear segments between 3 uniform topologies (6/15/24 nodes); cost/task = total_cost_usd/max(tasks,1)*1000 (mUSD); energy/task = total_energy_wh/max(tasks,1)*1000 (mWh); makespan ms->s

#### `_topo_short`

- **Inputs**: topology label string
- **Outputs**: same label with underscores replaced by newlines
- **Aggregations**: none (formatting helper)

#### `plot_topology_comparison`

- **Inputs**: main_data[every_topo][medium][algo].summary.throughput_tasks_per_min; main_data[every_topo][medium][algo].summary.avg_workflow_makespan_ms
- **Outputs**: 05_topology_throughput, 06_topology_makespan — grouped bars (algorithm groups within each topology)
- **Aggregations**: raw seed-aggregated values; makespan scaled ms->s; medium capacity only

#### `plot_capacity_effect`

- **Inputs**: main_data[uniform_m][small|medium|large][algo].summary.throughput_tasks_per_min; ...summary.avg_workflow_makespan_ms; ...summary.total_cost_usd; ...summary.total_energy_wh
- **Outputs**: 07a_capacity_throughput, 07b_capacity_makespan, 07c_capacity_cost, 07d_capacity_energy — grouped bars (capacity groups within each algorithm)
- **Aggregations**: raw seed-aggregated values; uniform_m topology fixed; makespan scaled ms->s

#### `plot_topsis_sensitivity`

- **Inputs**: main_data[uniform_m][medium][topsis] (balanced baseline); sens_data[uniform_m][medium][time|cost|energy] (TOPSIS skewed variants); summary.throughput_tasks_per_min, summary.avg_workflow_makespan_ms, summary.total_cost_usd
- **Outputs**: 08_topsis_sensitivity — 3-panel bar chart (throughput, makespan, cost) under 4 weight configs
- **Aggregations**: weight order fixed [balanced, time, cost, energy]; weights detected from manifest's topsis_weights.label (NOT filenames); makespan scaled ms->s; skipped if fewer than 2 weight variants available

#### `plot_winner_matrix`

- **Inputs**: main_data[every_topo][every_cap][every_algo].summary.throughput_tasks_per_min; ...summary.avg_workflow_makespan_ms; ...summary.total_cost_usd; ...summary.total_energy_wh
- **Outputs**: 09_algorithm_winner_matrix — 4-panel heatmap (one per metric), rows=topology cols=capacity, cell color/text = winning algorithm
- **Aggregations**: per-cell argmax/argmin: linear scan tracking running extremum; throughput higher-is-better, others lower-is-better; ties keep first-seen algorithm (deterministic via Python dict insertion order); -1 sentinel for missing data, left unannotated

#### `plot_tier_ratio_effect`

- **Inputs**: main_data[edge_heavy|uniform_m|cloud_heavy][medium][topsis].summary.by_tier[EDGE|FOG|CLOUD].tasks_completed
- **Outputs**: 10_tier_ratio_effect — stacked bar (EDGE/FOG/CLOUD) per topology, TOPSIS algorithm only
- **Aggregations**: stacking via running bottom accumulator; raw counts (no normalization); algorithm fixed to TOPSIS to isolate topology-ratio effect; no-op when fewer than 2 target topologies have medium-capacity data

#### `run_per_config_plots_if_missing`

- **Inputs**: manifest
- **Outputs**: (re)generates per-config plots under each combo's <combo>/plots/ dir by shelling out to plot_results.py
- **Aggregations**: skips sensitivity runs (those with non-null algorithm field); skips combos whose plots/ already has >= 9 files

#### `main`

- **Inputs**: sys.argv[1] = grid_dir
- **Outputs**: 10 comparative figures in <grid_dir>/comparative/ plus per-config plots in each combo's plots/
- **Aggregations**: orchestration: loads manifest, calls load_grid_data, sequences all plot_* functions with per-step error logging

---

## `scripts/plot_results.py`

**Role.** per-configuration visualization for a single benchmark run

**Input Format.** `directory of *.json ExperimentResult files (one per algorithm), schema produced by internal/results/exporter.go (ExperimentResult -> ExperimentSummary -> ByTier)`

**Output Dir.** `<input_dir>/plots/`

**Output Count.** 9

**Output Formats.** ["pdf", "png"]

### Functions

#### `find_latest_benchmark`

- **Outputs**: path to latest results/benchmark_* directory
- **Aggregations**: lexicographic sort of glob results (YYYYMMDD_HHMMSS suffix == chronological)

#### `load_results`

- **Inputs**: directory path containing per-algorithm ExperimentResult JSONs
- **Outputs**: dict {meta.algorithm -> parsed JSON}
- **Aggregations**: none; one JSON per algorithm, keyed by meta.algorithm

#### `present_algos`

- **Inputs**: data dict from load_results
- **Outputs**: list of algorithms (subset of ALGORITHMS) in canonical order
- **Aggregations**: filter + reorder

#### `save`

- **Inputs**: matplotlib Figure; out_dir; name stem
- **Outputs**: writes <name>.pdf and <name>.png to out_dir
- **Aggregations**: none (I/O helper)

#### `plot_throughput`

- **Inputs**: summary.throughput_tasks_per_min
- **Outputs**: throughput.pdf/png — bar chart (tasks/minute per algorithm)
- **Aggregations**: none; raw scalar per algorithm

#### `plot_tasks_completed`

- **Inputs**: summary.total_tasks_completed
- **Outputs**: tasks_completed.pdf/png — bar chart (absolute tasks done per algorithm)
- **Aggregations**: none; raw scalar per algorithm

#### `plot_makespan`

- **Inputs**: summary.avg_workflow_makespan_ms; summary.p50_workflow_makespan_ms; summary.p95_workflow_makespan_ms
- **Outputs**: makespan.pdf/png — grouped bars: Avg / P50 / P95 in seconds
- **Aggregations**: unit conversion ms -> s; mean/percentiles already computed upstream by Go exporter

#### `plot_response_time_cdf`

- **Inputs**: task_results[*].effective_duration_ms (filtered > 0)
- **Outputs**: response_time_cdf.pdf/png — empirical CDF line per algorithm
- **Aggregations**: empirical CDF: sort + uniform ranks in [0,1]; no smoothing or binning; x converted ms -> s

#### `plot_cost`

- **Inputs**: summary.total_cost_usd; summary.total_tasks_completed
- **Outputs**: cost.pdf/png — two panels: total USD and per-task mUSD
- **Aggregations**: per-task = total_cost_usd / max(total_tasks_completed, 1) * 1000 (mUSD)

#### `plot_energy`

- **Inputs**: summary.total_energy_wh; summary.total_tasks_completed
- **Outputs**: energy.pdf/png — two panels: total Wh and per-task mWh
- **Aggregations**: per-task = total_energy_wh / max(total_tasks_completed, 1) * 1000 (mWh)

#### `plot_load_balance`

- **Inputs**: servers[*].completed_tasks (filtered > 0)
- **Outputs**: load_balance.pdf/png — boxplot of per-server task counts, one box per algorithm
- **Aggregations**: boxplot quartiles per algorithm; idle servers (completed_tasks==0) excluded

#### `plot_tier_distribution`

- **Inputs**: summary.by_tier[EDGE|FOG|CLOUD].tasks_completed
- **Outputs**: tier_distribution.pdf/png — stacked bar per algorithm, EDGE/FOG/CLOUD layers
- **Aggregations**: stacking via running bottom accumulator; raw counts (no normalization)

#### `plot_tier_duration_heatmap`

- **Inputs**: summary.by_tier[EDGE|FOG|CLOUD].avg_effective_duration_ms
- **Outputs**: tier_duration_heatmap.pdf/png — heatmap rows=algorithm, cols=tier, values in seconds
- **Aggregations**: unit conversion ms -> s; mean already computed upstream by Go exporter; annotation text color flips at 70% of max

#### `main`

- **Inputs**: sys.argv[1] (optional) or latest benchmark dir
- **Outputs**: 9 figures written to <bench_dir>/plots/
- **Aggregations**: orchestration only

---

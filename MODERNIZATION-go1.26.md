# Go 1.25 → 1.26 modernization review

Scope: the `distributed-workflow-scheduler-minimal` Go app. The module is already
`go 1.25.0` and the scalability/correctness pass (thread-safe channel, ACK/NACK,
QoS prefetch, SCAN, pipelined deps, backoff) is done. This is a separate axis:
what the Go 1.25/1.26 features from the I/O talk actually buy *this* code.

Verdict up front: two changes are clear wins (Green Tea GC, `go fix`), two are
worth measuring before adopting (jsonv2, synctest tests), one is a free
reproducibility note (container GOMAXPROCS), and one is hype for this workload
(SIMD). Don't adopt anything you can't show a number for — that's why the first
thing this branch adds is benchmarks; there were zero `func Benchmark` in the repo.

---

## 0. Honest framing of the workload

The simulation is **sleep-bound**: a worker's cost is `time.Sleep(task.Duration +
latency)`, not CPU. So none of these features will move end-to-end wall-clock much.
What they move is **GC overhead, allocation rate, and serialization cost** — which
is exactly the kind of thing a thesis *measures and reports*, not the thing that
makes the demo finish faster. Frame any result that way and it's defensible;
claim "40% faster scheduler" and a reviewer will correctly push back.

---

## 1. Green Tea GC — ADOPT (headline)

What it is: redesigned mark/scan with better locality; ~10–40% less GC overhead on
GC-heavy programs, plus it uses vector instructions internally on Ice Lake / Zen 4+.
Experimental in 1.25 (`GOEXPERIMENT=greenteagc`), **default in 1.26**.

Why it fits: the state layer churns small objects constantly — every reserve/release
re-marshals a `Server`, every tick allocates `pending` + an ID snapshot, every task
emits a `ResultMessage`. That's the small-object profile Green Tea targets.

Do this:

```bash
# 1.25, no version bump — A/B it:
go test -run '^$' -bench . -benchmem ./...
GOEXPERIMENT=greenteagc go test -run '^$' -bench . -benchmem ./...

# Or bump to 1.26 and get it by default:
#   go.mod:            go 1.26.0
#   Dockerfile:        FROM golang:1.26-alpine AS builder
#   benchmark/Dockerfile: FROM golang:1.26-alpine AS build
```

For the thesis: run the full benchmark (`benchmark/main.go`) under both GCs with
`GODEBUG=gctrace=1`, and report total GC CPU and pause distribution. That's a clean
"effect of GC choice on a fog-scheduling workload" subsection.

Risk: low. Pure runtime change, no API surface.

---

## 2. `go fix` modernizers — ADOPT (low-risk cleanup)

What it is: `go fix` is now the home of Go's modernizers — dozens of fixers that
rewrite old idioms to current ones (`min`/`max` builtins, `slices`/`maps`,
`for range n`, `any`, etc.), plus a source-level inliner driven by `//go:fix inline`.

Do this (it's safe — it's `gofmt`-style mechanical, and you review the diff):

```bash
go fix ./...
git diff          # read every hunk before committing
go test ./...
```

Likely hits in this repo: the manual `minMaxSlice` in `topsis.go` and the hand-rolled
min/max comparisons in `postPlacementUtil`/`FreeCapacity` can collapse to the `min`/
`max` builtins; a few `interface{}` → `any`. Small, but it's free correctness-preserving
modernization and a legitimate "tooling" line in the methodology chapter.

Don't bother with `//go:fix inline` — it's for migrating your own deprecated APIs
across a codebase; you don't have that situation here.

---

## 3. JSON v2 — MEASURE, then decide

What it is: `encoding/json/v2`, substantially faster encode/decode, opt-in via
`GOEXPERIMENT=jsonv2` (1.25; matured in 1.26).

Why it's plausible here: every Redis value and every broker message is JSON
(`internal/store/redis.go` alone marshals/unmarshals in ~20 places; scheduler and
worker each (un)marshal on the hot path). After the sleep, this is the single
busiest CPU path in the system.

Do this — the benchmarks added in this branch isolate exactly this path:

```bash
go test -run '^$' -bench BenchmarkJSON -benchmem ./internal/models/
GOEXPERIMENT=jsonv2 go test -run '^$' -bench BenchmarkJSON -benchmem ./internal/models/
```

`GOEXPERIMENT=jsonv2` flips the existing `encoding/json` calls to the new engine
with **no code change**, so you get the speedup for free if the numbers justify it.
Caveat worth stating in the thesis: v2 has minor behavioral differences (e.g.
nil-slice and duplicate-key handling). Your payloads are flat structs with no map
keys in question, so the risk is low — but verify `go test ./...` stays green under
the flag before claiming it.

---

## 4. `testing/synctest` — ADOPT for test quality

What it is: GA in 1.25. Runs concurrent code in a "bubble" with a virtual clock —
fake time jumps forward instantly once all goroutines block.

Why it fits: `worker.execute` is built around `control.Sleep(done, effectiveDuration)`
and the consumer/semaphore dance. Today there's no test that exercises timing or the
drain/shutdown path — `WaitWithTimeout`, pause/resume, the done-channel races all go
untested. `synctest` lets you assert "task takes effectiveDuration, drain completes,
no goroutine leak" deterministically and in microseconds instead of real seconds.

Concretely: add `internal/worker/worker_synctest_test.go` using `synctest.Test` with a
fake store/broker, advance virtual time past `effectiveDuration`, assert the
`ResultMessage` is published and `WaitWithTimeout` returns true. This is the most
valuable *correctness* addition of the bunch and reads well in a testing chapter.

---

## 5. Container-aware GOMAXPROCS — free, but pin it for reproducibility

1.25 makes `GOMAXPROCS` default to the container's CPU limit (CFS quota) instead of
host cores. You run under Docker Compose, so you already get this. The catch for a
**thesis**: results become a function of whatever CPU limit the host scheduler hands
the container, which hurts reproducibility across machines.

Recommendation: pin it explicitly for benchmark runs so every run is comparable —
set `GOMAXPROCS` in `docker-compose.yml` (or `cpus:`/`cpuset:`) and record the value
in the results metadata. One line, removes a confound.

---

## 6. SIMD (`simd`/`archsimd`) — REJECT for this code

What it is: experimental `GOEXPERIMENT=simd`, amd64 128/256/512-bit vector types.

Why it does **not** fit: the only vectorizable math is TOPSIS (`vecNorm`,
`weightedNorm`, the d+/d− loops). Those loops run over the **candidate pool**, which
in your experiments is 3–9 servers per tier. Vectorizing a 9-element `float64` loop
is a net loss — setup/spill cost dwarfs the arithmetic, and the whole `Select` is
already dominated by the `sort.SliceStable` and per-call slice allocations, not the
FLOPs. The included `BenchmarkTOPSISSelect` sweeps N=3…1000 precisely to show the
arithmetic only becomes the bottleneck in the thousands, which your topology never hits.

Correct thesis framing: "TOPSIS arithmetic is not the bottleneck at realistic pool
sizes; SIMD would only pay off for pool sizes ≥ O(10³), so it's left as future work.
Note that Green Tea already exploits vector instructions internally, so the workload
benefits from SIMD where it actually matters — GC scanning — without app-level code."

That's the honest call. Don't add a SIMD dependency to look modern.

---

## What this branch already added

- `internal/scheduler/topsis_bench_test.go` — TOPSIS decision cost vs candidate count.
- `internal/models/serialization_bench_test.go` — JSON marshal/unmarshal of the
  hot-path payloads (`Server`, `ResultMessage`), ready for the jsonv2 A/B.

Run them:

```bash
go test -run '^$' -bench . -benchmem ./internal/scheduler/ ./internal/models/
```

## Suggested order

1. `go fix ./...`, review, test, commit. (free cleanup)
2. Run the new benchmarks; record baseline.
3. Re-run under `GOEXPERIMENT=greenteagc` and `GOEXPERIMENT=jsonv2`; keep the tables.
4. Bump to `go 1.26` (Green Tea becomes default) once the numbers are in hand.
5. Add the `synctest` worker test.
6. Pin `GOMAXPROCS` for the final benchmark sweep.
7. Write SIMD up as future work with the benchmark as evidence.

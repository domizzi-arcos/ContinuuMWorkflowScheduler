# Distributed Workflow Scheduler — development & reproduction targets

GO        ?= go
GRID_DIR  ?= results

.PHONY: all build vet fmt test bench up down reproduce-grid plots analysis pytest help

all: build vet test

build:            ## Compile every package
	$(GO) build ./...

vet:              ## Static analysis
	$(GO) vet ./...

fmt:              ## Fail if any file is not gofmt-clean
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

test:             ## Unit tests (no external services required; store tests use miniredis)
	$(GO) test ./...

bench:            ## Micro-benchmarks (TOPSIS selection, model serialization)
	$(GO) test -bench=. -benchmem -run=^$$ ./internal/scheduler/ ./internal/models/

up:               ## Full interactive stack (Redis + RabbitMQ + simulator + dashboard)
	docker-compose up --build

down:             ## Tear down the stack and volumes
	docker-compose down -v

reproduce-grid:   ## Reproduce the thesis campaign: 24-cell grid x seeds {1,2,3} (~21.7 h)
	python3 scripts/grid_benchmark.py --skip-topsis-weights --seeds 1,2,3 --duration 30

plots:            ## Regenerate thesis figures from a campaign directory: make plots GRID_DIR=results/grid_...
	python3 scripts/grid_plot.py $(GRID_DIR)
	python3 scripts/generate_report.py $(GRID_DIR)

ANALYSIS_OUT ?= analysis_out

analysis:         ## Regenerate tables, figures and LaTeX numbers: make analysis GRID_DIRS="results/grid_a results/grid_b"
	@test -n "$(GRID_DIRS)" || { echo 'set GRID_DIRS="results/grid_..."'; exit 1; }
	python3 scripts/analysis/run.py $(GRID_DIRS) --out $(ANALYSIS_OUT) $(if $(AUDIT),--audit $(AUDIT))

pytest:           ## Analysis-pipeline unit tests
	python3 -m pytest scripts/analysis

help:             ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

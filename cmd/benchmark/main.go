// Command benchmark is the campaign runner invoked by
// scripts/grid_benchmark.py. For each requested policy it resets Redis,
// purges the queues, re-seeds servers and tasks from the config, generates
// workflows for -duration seconds, waits up to -drain seconds for in-flight
// work, and writes one JSON result into -output.
//
// Unlike benchmark/main.go, server counts come from the config file; there is
// no per-tier override. Any failure exits non-zero.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/provenance"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/results"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/runner"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("benchmark failed", "err", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to the run config (required)")
	outDir := fs.String("output", "", "directory for the JSON results (default results/benchmark_<UTC timestamp>)")
	duration := fs.Int("duration", 30, "generation window in seconds")
	drain := fs.Int("drain", int(runner.DefaultDrain/time.Second), "drain deadline in seconds")
	algorithm := fs.String("algorithm", "", "run a single policy (kept for compatibility with -algorithms)")
	algorithmList := fs.String("algorithms", "", "comma-separated policies to run (default: all registered, in canonical order)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	if *configPath == "" {
		return fmt.Errorf("-config is required")
	}
	if *duration <= 0 {
		return fmt.Errorf("-duration must be > 0, got %d", *duration)
	}
	if *drain < 0 {
		return fmt.Errorf("-drain must be >= 0, got %d", *drain)
	}
	policies, err := selectPolicies(*algorithm, *algorithmList)
	if err != nil {
		return err
	}

	baseCfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if *outDir == "" {
		*outDir = fmt.Sprintf("results/benchmark_%s", time.Now().UTC().Format("20060102_150405"))
	}
	if err := os.MkdirAll(*outDir, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	redisStore, err := store.NewRedisStore(baseCfg.Redis.Address, baseCfg.Redis.DB)
	if err != nil {
		return fmt.Errorf("connect to Redis: %w", err)
	}

	prov := provenance.Collect(args)
	if prov.GitDirty {
		slog.Warn("binary was built from a dirty working tree", "commit", prov.GitCommit)
	}
	slog.Info("benchmark started",
		"algorithms", policies,
		"duration_s", *duration,
		"drain_s", *drain,
		"servers", fmt.Sprintf("edge=%d fog=%d cloud=%d", baseCfg.Servers.Edge.Count, baseCfg.Servers.Fog.Count, baseCfg.Servers.Cloud.Count),
		"output", *outDir,
		"commit", prov.GitCommit,
	)

	runDuration := time.Duration(*duration) * time.Second
	drainDeadline := time.Duration(*drain) * time.Second
	for i, algo := range policies {
		cfg := *baseCfg
		cfg.Scheduler.Algorithm = algo
		cfg.Experiment.DurationSeconds = *duration
		if err := cfg.Validate(); err != nil {
			return fmt.Errorf("config for %s: %w", algo, err)
		}

		slog.Info("starting algorithm run", "algorithm", algo, "run", fmt.Sprintf("%d/%d", i+1, len(policies)))
		err := runner.Run(redisStore, &cfg, runDuration, drainDeadline, func(c *config.Config, out runner.Outcome) error {
			slog.Info("drain finished", "algorithm", algo, "drained", out.Drained, "drain_s", fmt.Sprintf("%.1f", out.DrainSeconds))
			_, err := results.ExportRun(redisStore, c, out.Wall, *outDir, results.RunInfo{Provenance: prov})
			return err
		})
		if err != nil {
			return fmt.Errorf("algorithm %s: %w", algo, err)
		}
		slog.Info("algorithm run complete", "algorithm", algo)
	}

	slog.Info("benchmark complete", "output", *outDir)
	return nil
}

// selectPolicies resolves -algorithm / -algorithms into an ordered list of
// validated policy names. Duplicates are rejected so a run never silently
// overwrites a sibling result.
func selectPolicies(single, list string) ([]string, error) {
	if single != "" && list != "" {
		return nil, fmt.Errorf("use either -algorithm or -algorithms, not both")
	}
	var names []string
	switch {
	case single != "":
		names = []string{strings.TrimSpace(single)}
	case list != "":
		for _, n := range strings.Split(list, ",") {
			if n = strings.TrimSpace(n); n != "" {
				names = append(names, n)
			}
		}
		if len(names) == 0 {
			return nil, fmt.Errorf("-algorithms is empty")
		}
	default:
		return algos.Names(), nil
	}
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		if !algos.Valid(n) {
			return nil, fmt.Errorf("unknown algorithm %q (registered: %s)", n, strings.Join(algos.Names(), ", "))
		}
		if seen[n] {
			return nil, fmt.Errorf("algorithm %q listed twice", n)
		}
		seen[n] = true
	}
	return names, nil
}

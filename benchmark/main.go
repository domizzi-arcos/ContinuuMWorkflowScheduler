package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/results"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/runner"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

var algorithms = algos.Legacy()

func main() {
	duration := flag.Int("duration", 60, "seconds each algorithm runs before generation stops")
	serversPerTier := flag.Int("servers", 5, "number of servers per tier (EDGE/FOG/CLOUD)")
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	redisStore, err := store.NewRedisStore(cfg.Redis.Address, cfg.Redis.DB)
	if err != nil {
		slog.Error("failed to connect to Redis", "err", err)
		os.Exit(1)
	}

	outDir := fmt.Sprintf("results/benchmark_%s", time.Now().UTC().Format("20060102_150405"))
	if err := os.MkdirAll(outDir, 0755); err != nil {
		slog.Error("failed to create output directory", "err", err)
		os.Exit(1)
	}
	slog.Info("benchmark started",
		"algorithms", algorithms,
		"duration_s", *duration,
		"servers_per_tier", *serversPerTier,
		"output", outDir,
	)

	for i, algo := range algorithms {
		slog.Info("starting algorithm run",
			"algorithm", algo,
			"run", fmt.Sprintf("%d/%d", i+1, len(algorithms)),
		)
		if err := runAlgorithm(redisStore, cfg, algo, *serversPerTier, time.Duration(*duration)*time.Second, outDir); err != nil {
			slog.Error("algorithm run failed", "algorithm", algo, "err", err)
			os.Exit(1)
		}
		slog.Info("algorithm run complete", "algorithm", algo)
	}

	slog.Info("benchmark complete", "output", outDir)
	fmt.Printf("\nResults saved to: %s\nGenerate plots:   python analysis/plot.py %s\n", outDir, outDir)
}

func runAlgorithm(
	s *store.RedisStore,
	baseCfg *config.Config,
	algoName string,
	serversPerTier int,
	runDuration time.Duration,
	outDir string,
) error {
	cfg := *baseCfg
	cfg.Scheduler.Algorithm = algoName
	cfg.Servers.Edge.Count = serversPerTier
	cfg.Servers.Fog.Count = serversPerTier
	cfg.Servers.Cloud.Count = serversPerTier
	cfg.Experiment.DurationSeconds = int(runDuration.Seconds())

	return runner.Run(s, &cfg, runDuration, runner.DefaultDrain, func(c *config.Config, out runner.Outcome) error {
		results.Export(s, c, out.Wall, outDir)
		return nil
	})
}

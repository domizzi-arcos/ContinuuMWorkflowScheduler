package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/generator"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/results"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/scheduler"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/seeder"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/worker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/workflow"
)

var algorithms = []string{"round_robin", "random", "best_fit", "worst_fit", "topsis"}

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

	b, err := connectWithRetry(cfg.RabbitMQ.URL)
	if err != nil {
		slog.Error("failed to connect to RabbitMQ", "err", err)
		os.Exit(1)
	}
	defer b.Close()

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
		if err := runAlgorithm(redisStore, b, cfg, algo, *serversPerTier, time.Duration(*duration)*time.Second, outDir); err != nil {
			slog.Error("algorithm run failed", "algorithm", algo, "err", err)
			os.Exit(1)
		}
		slog.Info("algorithm run complete", "algorithm", algo)
	}

	slog.Info("benchmark complete", "output", outDir)
	fmt.Printf("\nResults saved to: %s\nGenerate plots:   python scripts/plot_results.py %s\n", outDir, outDir)
}

type benchSession struct {
	done        chan struct{}
	genDone     chan struct{}
	genFinished chan struct{}
	execIDs     chan string
	resultIDs   chan string
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stopGenOnce sync.Once
}

func (s *benchSession) stopGeneration() {
	s.stopGenOnce.Do(func() { close(s.genDone) })
}

func (s *benchSession) stopAll() {
	s.stopOnce.Do(func() { close(s.done) })
	s.wg.Wait()
}

func runAlgorithm(
	s *store.RedisStore,
	b *broker.Broker,
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

	if err := b.PurgeQueues(); err != nil {
		return fmt.Errorf("purge queues: %w", err)
	}
	if err := s.ResetSimulationData(); err != nil {
		return fmt.Errorf("reset redis: %w", err)
	}
	if err := seeder.SeedServers(s, &cfg); err != nil {
		return fmt.Errorf("seed servers: %w", err)
	}
	tasks, err := generator.SeedTasks(s, cfg.Generator)
	if err != nil {
		return fmt.Errorf("seed tasks: %w", err)
	}

	sess := &benchSession{
		done:        make(chan struct{}),
		genDone:     make(chan struct{}),
		genFinished: make(chan struct{}),
		execIDs:     make(chan string, 256),
		resultIDs:   make(chan string, 256),
	}

	pauseCtrl := control.New()
	tw := cfg.Scheduler.TOPSISWeights
	algoImpl := scheduler.NewAlgorithm(cfg.Scheduler.Algorithm, cfg.Scheduler.Seed, tw.Time, tw.Cost, tw.Energy)
	sched := scheduler.New(s, b, algoImpl, sess.execIDs, pauseCtrl)
	gen := generator.New(s, tasks, sess.execIDs, pauseCtrl, cfg.Generator.WorkflowSeed, cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	w := worker.New(s, b, cfg.Scheduler.Algorithm, pauseCtrl)
	col := results.New(s, b, pauseCtrl, sess.resultIDs)
	tracker := workflow.NewTracker(s, sess.resultIDs, sess.execIDs, pauseCtrl)

	sess.wg.Add(1)
	go func() {
		defer sess.wg.Done()
		sched.Start(sess.done)
	}()
	sess.wg.Add(1)
	go func() {
		defer sess.wg.Done()
		defer close(sess.genFinished)
		gen.Start(sess.genDone)
	}()
	w.StartAll(sess.done)
	sess.wg.Add(1)
	go func() {
		defer sess.wg.Done()
		col.Start(sess.done)
	}()
	sess.wg.Add(1)
	go func() {
		defer sess.wg.Done()
		tracker.Start(sess.done)
	}()

	startedAt := time.Now()

	// Run for the configured duration, then stop generation
	time.Sleep(runDuration)
	sess.stopGeneration()
	<-sess.genFinished

	// Drain: wait for all in-flight tasks to complete (up to 3 minutes)
	drainDeadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(drainDeadline) {
		if drained, _ := isDrained(s); drained {
			break
		}
		time.Sleep(1 * time.Second)
	}

	wall := time.Since(startedAt)
	results.Export(s, &cfg, wall, outDir)
	sess.stopAll()
	return nil
}

func isDrained(s *store.RedisStore) (bool, error) {
	executions, err := s.GetAllExecutions()
	if err != nil {
		return false, err
	}
	taskResults, err := s.GetAllTaskResults()
	if err != nil {
		return false, err
	}
	var blocked, pending, queued, running, done int
	for _, ex := range executions {
		switch ex.Status {
		case "BLOCKED":
			blocked++
		case "PENDING":
			pending++
		case "QUEUED":
			queued++
		case "RUNNING":
			running++
		case "DONE":
			done++
		}
	}
	return blocked == 0 && pending == 0 && queued == 0 && running == 0 && len(taskResults) >= done, nil
}

func connectWithRetry(url string) (*broker.Broker, error) {
	deadline := time.Now().Add(45 * time.Second)
	for {
		b, err := broker.NewBroker(url)
		if err == nil {
			return b, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(2 * time.Second)
	}
}

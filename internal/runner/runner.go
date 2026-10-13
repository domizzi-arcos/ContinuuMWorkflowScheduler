// Package runner holds the per-policy session and drain logic shared by the
// batch runners (cmd/benchmark and benchmark/main.go). One call to Run resets
// Redis, purges the queues, re-seeds servers and tasks, generates workflows
// for a fixed window, waits for the system to drain (bounded by a deadline)
// and hands control to an export callback before tearing the session down.
package runner

import (
	"fmt"
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

// DefaultDrain is the drain deadline used by the original benchmark runner.
const DefaultDrain = 3 * time.Minute

// drainPoll is how often the drain loop checks Redis.
const drainPoll = 1 * time.Second

// Outcome describes how one policy run ended.
type Outcome struct {
	// Wall is the time from session start to the end of the drain phase
	// (generation window + drain). It is what results.Export receives.
	Wall time.Duration
	// Drained is true when the drain condition held before the deadline.
	Drained bool
	// DrainSeconds is the time spent in the drain phase, measured from the
	// moment the generator exited.
	DrainSeconds float64
}

// ExportFunc is called after the drain phase and before the session is torn
// down, so the pipeline is still live while results are read from Redis.
type ExportFunc func(cfg *config.Config, out Outcome) error

type session struct {
	done        chan struct{}
	genDone     chan struct{}
	genFinished chan struct{}
	execIDs     chan string
	resultIDs   chan string
	wg          sync.WaitGroup
	stopOnce    sync.Once
	stopGenOnce sync.Once
}

func (s *session) stopGeneration() {
	s.stopGenOnce.Do(func() { close(s.genDone) })
}

func (s *session) stopAll() {
	s.stopOnce.Do(func() { close(s.done) })
	s.wg.Wait()
}

// Run executes one policy run with cfg exactly as given (the caller sets
// cfg.Scheduler.Algorithm, server counts, etc.). runDuration is the
// generation window; drain bounds the wait for in-flight work afterwards.
// export is invoked once, after draining; its error is returned.
func Run(s *store.RedisStore, cfg *config.Config, runDuration, drain time.Duration, export ExportFunc) error {
	b, err := ConnectWithRetry(cfg.RabbitMQ.URL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer b.Close()

	if err := b.PurgeQueues(); err != nil {
		return fmt.Errorf("purge queues: %w", err)
	}
	if err := s.ResetSimulationData(); err != nil {
		return fmt.Errorf("reset redis: %w", err)
	}
	if err := seeder.SeedServers(s, cfg); err != nil {
		return fmt.Errorf("seed servers: %w", err)
	}
	tasks, err := generator.SeedTasks(s, cfg.Generator)
	if err != nil {
		return fmt.Errorf("seed tasks: %w", err)
	}

	sess := &session{
		done:        make(chan struct{}),
		genDone:     make(chan struct{}),
		genFinished: make(chan struct{}),
		execIDs:     make(chan string, 256),
		resultIDs:   make(chan string, 256),
	}

	pauseCtrl := control.New()
	tw := cfg.Scheduler.TOPSISWeights
	algoImpl := scheduler.NewAlgorithm(cfg.Scheduler.Algorithm, cfg.Scheduler.Seed, tw.Time, tw.Cost, tw.Energy, tw.Util)
	sched := scheduler.New(s, b, algoImpl, sess.execIDs, pauseCtrl)
	gen := generator.New(s, tasks, sess.execIDs, pauseCtrl, cfg.Generator.WorkflowSeed, cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	gen.SetInterval(generator.SubmissionInterval(cfg.Generator))
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

	time.Sleep(runDuration)
	sess.stopGeneration()
	<-sess.genFinished

	drainStart := time.Now()
	drainDeadline := drainStart.Add(drain)
	drained := false
	for time.Now().Before(drainDeadline) {
		if ok, _ := IsDrained(s); ok {
			drained = true
			break
		}
		time.Sleep(drainPoll)
	}

	out := Outcome{
		Wall:         time.Since(startedAt),
		Drained:      drained,
		DrainSeconds: time.Since(drainStart).Seconds(),
	}
	exportErr := export(cfg, out)
	sess.stopAll()
	return exportErr
}

// IsDrained reports whether no execution is BLOCKED, PENDING, QUEUED or
// RUNNING and the results collector has persisted a result for every DONE
// execution. A single unplaceable task therefore keeps the run alive until
// the drain deadline.
func IsDrained(s *store.RedisStore) (bool, error) {
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

// ConnectWithRetry dials RabbitMQ every 2 s for up to 45 s.
func ConnectWithRetry(url string) (*broker.Broker, error) {
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

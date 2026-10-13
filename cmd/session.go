package main

import (
	"fmt"
	"log/slog"
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

// simulationSession owns the goroutines and channels for one experiment run.
// The session is recreated from scratch on every restart so each run starts
// with empty queues and freshly seeded data; the parent main goroutine holds
// the only reference.
type simulationSession struct {
	store  *store.RedisStore
	broker *broker.Broker
	worker *worker.Worker
	// done is closed to signal scheduler, results collector and tracker to
	// exit. Worker shutdown is handled separately via WaitWithTimeout so we
	// can bound how long the drain takes.
	done chan struct{}
	// generatorDone is closed independently so the generator can be stopped
	// (drain mode) without tearing down the rest of the pipeline.
	generatorDone chan struct{}
	// generationFinished is closed by the generator goroutine on exit so
	// callers can observe that no further executions will be produced.
	generationFinished chan struct{}
	// executionIDs streams newly created execution IDs from generator and
	// tracker to the scheduler. doneExecIDs streams completed execution IDs
	// from the results collector to the workflow tracker for DAG follow-up.
	executionIDs chan string
	doneExecIDs  chan string

	wg                sync.WaitGroup
	stopAllOnce       sync.Once
	stopGeneratorOnce sync.Once
}

// rabbitMQConnectTimeout bounds how long startSimulationSession will keep
// retrying the broker handshake before giving up. rabbitMQConnectRetryDelay
// is the pause between attempts. Both exist because docker-compose can bring
// the app up before RabbitMQ is ready to accept connections.
const (
	rabbitMQConnectTimeout    = 45 * time.Second
	rabbitMQConnectRetryDelay = 2 * time.Second
)

// startSimulationSession wires up a fresh experiment run: it connects to the
// broker, purges queues, resets simulation keys in Redis, seeds servers and
// tasks, then spawns the scheduler, generator, workers, results collector and
// workflow tracker goroutines. The returned session is the sole handle the
// caller needs to drive shutdown via StopGeneration / StopAll.
func startSimulationSession(s *store.RedisStore, cfg *config.Config, pauseCtrl *control.Controller) (*simulationSession, error) {
	b, err := connectBrokerWithRetry(cfg.RabbitMQ.URL)
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	if err := b.PurgeQueues(); err != nil {
		b.Close()
		return nil, fmt.Errorf("purge RabbitMQ queues: %w", err)
	}
	if err := s.ResetSimulationData(); err != nil {
		b.Close()
		return nil, fmt.Errorf("reset Redis data: %w", err)
	}
	if err := seeder.SeedServers(s, cfg); err != nil {
		b.Close()
		return nil, fmt.Errorf("seed servers: %w", err)
	}

	tasks, err := generator.SeedTasks(s, cfg.Generator)
	if err != nil {
		b.Close()
		return nil, fmt.Errorf("seed tasks: %w", err)
	}

	session := &simulationSession{
		store:              s,
		broker:             b,
		done:               make(chan struct{}),
		generatorDone:      make(chan struct{}),
		generationFinished: make(chan struct{}),
		executionIDs:       make(chan string, 256),
		doneExecIDs:        make(chan string, 256),
	}

	tw := cfg.Scheduler.TOPSISWeights
	algo := scheduler.NewAlgorithm(cfg.Scheduler.Algorithm, cfg.Scheduler.Seed, tw.Time, tw.Cost, tw.Energy, tw.Util)
	sched := scheduler.New(s, b, algo, session.executionIDs, pauseCtrl)
	gen := generator.New(s, tasks, session.executionIDs, pauseCtrl, cfg.Generator.WorkflowSeed, cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	gen.SetInterval(generator.SubmissionInterval(cfg.Generator))
	session.worker = worker.New(s, b, cfg.Scheduler.Algorithm, pauseCtrl)
	col := results.New(s, b, pauseCtrl, session.doneExecIDs)
	tracker := workflow.NewTracker(s, session.doneExecIDs, session.executionIDs, pauseCtrl)

	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		sched.Start(session.done)
	}()

	// Generator owns generationFinished: closing it on exit is the signal the
	// drain loop watches to know no more executions will ever be produced.
	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		defer close(session.generationFinished)
		gen.Start(session.generatorDone)
	}()

	session.worker.StartAll(session.done)

	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		col.Start(session.done)
	}()

	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		tracker.Start(session.done)
	}()

	slog.Info("simulation session started", "component", "session", "task_count", len(tasks))
	return session, nil
}

// connectBrokerWithRetry dials RabbitMQ until it succeeds or the timeout
// elapses, so the app can start in parallel with the broker container
// during docker-compose up.
func connectBrokerWithRetry(url string) (*broker.Broker, error) {
	deadline := time.Now().Add(rabbitMQConnectTimeout)
	attempt := 1
	var lastErr error

	for {
		b, err := broker.NewBroker(url)
		if err == nil {
			if attempt > 1 {
				slog.Info("RabbitMQ became available", "component", "session", "attempts", attempt)
			}
			return b, nil
		}

		lastErr = err
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		wait := rabbitMQConnectRetryDelay
		if wait > remaining {
			wait = remaining
		}
		slog.Warn("RabbitMQ not ready, retrying", "component", "session", "attempt", attempt, "err", err, "retry_in", wait.Round(time.Second))
		time.Sleep(wait)
		attempt++
	}

	return nil, fmt.Errorf("timed out after %s: %w", rabbitMQConnectTimeout, lastErr)
}

// StopGeneration signals the generator to stop creating new executions while
// leaving the scheduler, workers, results collector and tracker running so
// in-flight work can drain. Safe to call multiple times.
func (s *simulationSession) StopGeneration() {
	if s == nil {
		return
	}
	s.stopGeneratorOnce.Do(func() {
		close(s.generatorDone)
	})
}

// StopAll tears the whole session down: stops the generator, signals every
// other goroutine via done, waits for the worker to drain (bounded by 5s) and
// then closes the broker connection. Safe to call multiple times.
func (s *simulationSession) StopAll() {
	if s == nil {
		return
	}
	s.StopGeneration()
	s.stopAllOnce.Do(func() {
		close(s.done)
		if s.worker != nil {
			if !s.worker.WaitWithTimeout(5 * time.Second) {
				slog.Warn("worker drain timed out", "component", "session", "timeout", "5s")
			}
		}
		s.wg.Wait()
		if s.broker != nil {
			s.broker.Close()
		}
		slog.Info("simulation session stopped", "component", "session")
	})
}

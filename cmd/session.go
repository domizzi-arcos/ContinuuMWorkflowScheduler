package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/broker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/generator"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/results"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/scheduler"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/worker"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/workflow"
)

type simulationSession struct {
	store              *store.RedisStore
	broker             *broker.Broker
	worker             *worker.Worker
	done               chan struct{}
	generatorDone      chan struct{}
	generationFinished chan struct{}
	executionIDs       chan string
	doneExecIDs        chan string

	wg                sync.WaitGroup
	stopAllOnce       sync.Once
	stopGeneratorOnce sync.Once
}

const (
	rabbitMQConnectTimeout    = 45 * time.Second
	rabbitMQConnectRetryDelay = 2 * time.Second
)

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
	if err := seedServers(s, cfg); err != nil {
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
	algo := scheduler.NewAlgorithm(cfg.Scheduler.Algorithm, cfg.Scheduler.Seed, tw.Time, tw.Cost, tw.Energy)
	sched := scheduler.New(s, b, algo, session.executionIDs, pauseCtrl)
	gen := generator.New(s, tasks, session.executionIDs, pauseCtrl, cfg.Generator.WorkflowSeed, cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	session.worker = worker.New(s, b, cfg.Scheduler.Algorithm, pauseCtrl)
	col := results.New(s, b, pauseCtrl, session.doneExecIDs)
	tracker := workflow.NewTracker(s, session.doneExecIDs, session.executionIDs, pauseCtrl)

	session.wg.Add(1)
	go func() {
		defer session.wg.Done()
		sched.Start(session.done)
	}()

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

	log.Printf("[Session] Simulation session started with %d seeded tasks", len(tasks))
	return session, nil
}

func connectBrokerWithRetry(url string) (*broker.Broker, error) {
	deadline := time.Now().Add(rabbitMQConnectTimeout)
	attempt := 1
	var lastErr error

	for {
		b, err := broker.NewBroker(url)
		if err == nil {
			if attempt > 1 {
				log.Printf("[Session] RabbitMQ became available after %d attempts", attempt)
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
		log.Printf("[Session] RabbitMQ not ready (attempt %d): %v. Retrying in %s", attempt, err, wait.Round(time.Second))
		time.Sleep(wait)
		attempt++
	}

	return nil, fmt.Errorf("timed out after %s: %w", rabbitMQConnectTimeout, lastErr)
}

func (s *simulationSession) StopGeneration() {
	if s == nil {
		return
	}
	s.stopGeneratorOnce.Do(func() {
		close(s.generatorDone)
	})
}

func (s *simulationSession) StopAll() {
	if s == nil {
		return
	}
	s.StopGeneration()
	s.stopAllOnce.Do(func() {
		close(s.done)
		if s.worker != nil {
			if !s.worker.WaitWithTimeout(5 * time.Second) {
				log.Println("[Session] Warning: worker drain timed out after 5s")
			}
		}
		s.wg.Wait()
		if s.broker != nil {
			s.broker.Close()
		}
		log.Println("[Session] Simulation session stopped")
	})
}

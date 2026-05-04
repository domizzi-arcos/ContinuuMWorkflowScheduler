package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/control"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/results"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/web"
)

func main() {
	logCloser, logPath, err := setupExperimentLogging()
	if err != nil {
		log.Fatalf("Failed to configure logging: %v", err)
	}
	defer logCloser()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	experimentDurationSeconds := cfg.Experiment.DurationSeconds
	progressLogSeconds := cfg.Experiment.ProgressLogSeconds
	if progressLogSeconds <= 0 {
		progressLogSeconds = 10
	}

	log.Printf(
		"Config loaded: algorithm=%s task_count=%d edge<=%dms fog<=%dms",
		cfg.Scheduler.Algorithm,
		cfg.Generator.TaskCount,
		cfg.Generator.EdgeMaxDurationMS,
		cfg.Generator.FogMaxDurationMS,
	)
	if logPath != "" {
		log.Printf("[Experiment] Log file=%s", logPath)
	}
	if cfg.Experiment.DurationSeconds > 0 {
		log.Printf("[Experiment] Window=%ds progress_log=%ds", cfg.Experiment.DurationSeconds, cfg.Experiment.ProgressLogSeconds)
	} else {
		log.Printf("[Experiment] Window=manual-stop progress_log=%ds", cfg.Experiment.ProgressLogSeconds)
	}

	redisStore, err := store.NewRedisStore(cfg.Redis.Address, cfg.Redis.DB)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Connected to Redis")

	appDone := make(chan struct{})
	pauseCtrl := control.New()
	session, err := startSimulationSession(redisStore, cfg, pauseCtrl)
	if err != nil {
		log.Fatalf("Failed to start simulation session: %v", err)
	}

	var stateMu sync.RWMutex
	sessionActive := true
	durationStopRequested := false

	runtimeState := func() web.RuntimeState {
		stateMu.RLock()
		active := sessionActive
		stateMu.RUnlock()

		if !active {
			return web.RuntimeState{
				Status:           "finished",
				Active:           false,
				RestartAvailable: true,
			}
		}
		if pauseCtrl.IsPaused() {
			return web.RuntimeState{Status: "paused", Active: true}
		}
		return web.RuntimeState{Status: "running", Active: true}
	}

	restartRequests := make(chan chan error, 1)
	restartFunc := func() error {
		reply := make(chan error, 1)
		restartRequests <- reply
		return <-reply
	}

	webSrv := web.New(redisStore, pauseCtrl, runtimeState, restartFunc, cfg.Web.Port)
	go webSrv.Start(appDone)

	log.Printf("Distributed Task Scheduler ready. Dashboard -> http://localhost:%d", cfg.Web.Port)

	startedAt := time.Now().UTC()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	progressTicker := time.NewTicker(time.Duration(progressLogSeconds) * time.Second)
	defer progressTicker.Stop()
	drainTicker := time.NewTicker(1 * time.Second)
	defer drainTicker.Stop()

	finishSession := func(reason string) {
		wall := pauseCtrl.ActiveElapsed(startedAt)
		logExperimentSummary(redisStore, wall, reason)
		session.StopAll()
		results.Export(redisStore, cfg, wall)
		pauseCtrl.Reset()
		stateMu.Lock()
		sessionActive = false
		durationStopRequested = false
		stateMu.Unlock()
		log.Printf("[Experiment] Run finished (%s). Dashboard remains available for restart.", reason)
	}

	shutdownProcess := func(reason string) {
		stateMu.RLock()
		active := sessionActive
		stateMu.RUnlock()
		wall := pauseCtrl.ActiveElapsed(startedAt)
		if active {
			logExperimentSummary(redisStore, wall, reason)
			session.StopAll()
			results.Export(redisStore, cfg, wall)
		}
		close(appDone)
		log.Printf("[Experiment] Shutdown complete (%s)", reason)
	}

	for {
		select {
		case reply := <-restartRequests:
			stateMu.RLock()
			active := sessionActive
			stateMu.RUnlock()
			if active {
				reply <- fmt.Errorf("restart is only available after the current run has finished")
				continue
			}

			nextSession, err := startSimulationSession(redisStore, cfg, pauseCtrl)
			if err != nil {
				reply <- err
				log.Printf("[Experiment] Restart failed: %v", err)
				continue
			}

			session = nextSession
			startedAt = time.Now().UTC()
			stateMu.Lock()
			sessionActive = true
			durationStopRequested = false
			stateMu.Unlock()
			reply <- nil
			log.Printf("[Experiment] Restart complete. New run started at %s", startedAt.Format(time.RFC3339))
		case <-progressTicker.C:
			stateMu.RLock()
			active := sessionActive
			stateMu.RUnlock()
			if !active {
				continue
			}
			snapshot, err := collectExperimentSnapshot(redisStore)
			if err != nil {
				log.Printf("[Experiment] Progress snapshot failed: %v", err)
				continue
			}
			phase := "running"
			if pauseCtrl.IsPaused() {
				phase = "paused"
			}
			logExperimentProgress(pauseCtrl.ActiveElapsed(startedAt), phase, snapshot)
		case <-drainTicker.C:
			stateMu.RLock()
			active := sessionActive
			stopped := durationStopRequested
			stateMu.RUnlock()
			if !active {
				continue
			}

			if !stopped && experimentDurationSeconds > 0 &&
				pauseCtrl.ActiveElapsed(startedAt) >= time.Duration(experimentDurationSeconds)*time.Second {
				stateMu.Lock()
				durationStopRequested = true
				stateMu.Unlock()
				session.StopGeneration()
				log.Printf("[Experiment] Generation stop requested after %s", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
			}

			snapshot, err := collectExperimentSnapshot(redisStore)
			if err != nil {
				log.Printf("[Experiment] Drain snapshot failed: %v", err)
				continue
			}
			if stopped && experimentDrained(snapshot) {
				log.Printf("[Experiment] Drain complete after %s", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
				finishSession("completed")
			}
		case <-sig:
			log.Printf("[Experiment] Interrupt received after %s", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
			shutdownProcess("interrupted")
			return
		}
	}
}

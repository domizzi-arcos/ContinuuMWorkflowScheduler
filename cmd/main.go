package main

import (
	"fmt"
	"log/slog"
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
		slog.Error("failed to configure logging", "err", err)
		os.Exit(1)
	}
	defer logCloser()

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	experimentDurationSeconds := cfg.Experiment.DurationSeconds
	progressLogSeconds := cfg.Experiment.ProgressLogSeconds
	if progressLogSeconds <= 0 {
		progressLogSeconds = 10
	}

	slog.Info("config loaded",
		"algorithm", cfg.Scheduler.Algorithm,
		"task_count", cfg.Generator.TaskCount,
		"edge_max_duration_ms", cfg.Generator.EdgeMaxDurationMS,
		"fog_max_duration_ms", cfg.Generator.FogMaxDurationMS,
	)
	if logPath != "" {
		slog.Info("experiment log file", "component", "experiment", "path", logPath)
	}
	if cfg.Experiment.DurationSeconds > 0 {
		slog.Info("experiment window", "component", "experiment",
			"duration_s", cfg.Experiment.DurationSeconds,
			"progress_log_s", cfg.Experiment.ProgressLogSeconds,
		)
	} else {
		slog.Info("experiment window", "component", "experiment",
			"duration_s", "manual-stop",
			"progress_log_s", cfg.Experiment.ProgressLogSeconds,
		)
	}

	redisStore, err := store.NewRedisStore(cfg.Redis.Address, cfg.Redis.DB)
	if err != nil {
		slog.Error("failed to connect to Redis", "err", err)
		os.Exit(1)
	}
	slog.Info("connected to Redis")

	appDone := make(chan struct{})
	pauseCtrl := control.New()
	session, err := startSimulationSession(redisStore, cfg, pauseCtrl)
	if err != nil {
		slog.Error("failed to start simulation session", "err", err)
		os.Exit(1)
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

	slog.Info("scheduler ready", "dashboard", fmt.Sprintf("http://localhost:%d", cfg.Web.Port))

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
		results.Export(redisStore, cfg, wall, "results")
		pauseCtrl.Reset()
		stateMu.Lock()
		sessionActive = false
		durationStopRequested = false
		stateMu.Unlock()
		slog.Info("run finished", "component", "experiment", "reason", reason)
	}

	shutdownProcess := func(reason string) {
		stateMu.RLock()
		active := sessionActive
		stateMu.RUnlock()
		wall := pauseCtrl.ActiveElapsed(startedAt)
		if active {
			logExperimentSummary(redisStore, wall, reason)
			session.StopAll()
			results.Export(redisStore, cfg, wall, "results")
		}
		close(appDone)
		slog.Info("shutdown complete", "component", "experiment", "reason", reason)
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
				slog.Error("restart failed", "component", "experiment", "err", err)
				continue
			}

			session = nextSession
			startedAt = time.Now().UTC()
			stateMu.Lock()
			sessionActive = true
			durationStopRequested = false
			stateMu.Unlock()
			reply <- nil
			slog.Info("restart complete", "component", "experiment", "started_at", startedAt.Format(time.RFC3339))
		case <-progressTicker.C:
			stateMu.RLock()
			active := sessionActive
			stateMu.RUnlock()
			if !active {
				continue
			}
			snapshot, err := collectExperimentSnapshot(redisStore)
			if err != nil {
				slog.Error("progress snapshot failed", "component", "experiment", "err", err)
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
				slog.Info("generation stop requested", "component", "experiment", "elapsed", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
			}

			snapshot, err := collectExperimentSnapshot(redisStore)
			if err != nil {
				slog.Error("drain snapshot failed", "component", "experiment", "err", err)
				continue
			}
			if stopped && experimentDrained(snapshot) {
				slog.Info("drain complete", "component", "experiment", "elapsed", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
				finishSession("completed")
			}
		case <-sig:
			slog.Info("interrupt received", "component", "experiment", "elapsed", pauseCtrl.ActiveElapsed(startedAt).Round(time.Second))
			shutdownProcess("interrupted")
			return
		}
	}
}

package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func setupExperimentLogging() (func(), string, error) {
	logDir := strings.TrimSpace(os.Getenv("EXPERIMENT_LOG_DIR"))
	if logDir == "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
		return func() {}, "", nil
	}

	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, "", fmt.Errorf("create log dir %s: %w", logDir, err)
	}

	logPath := filepath.Join(logDir, fmt.Sprintf("experiment-%s.log", time.Now().UTC().Format("20060102-150405")))
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, "", fmt.Errorf("open log file %s: %w", logPath, err)
	}

	handler := slog.NewTextHandler(io.MultiWriter(os.Stdout, file), &slog.HandlerOptions{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(handler))

	return func() {
		file.Close()
	}, logPath, nil
}

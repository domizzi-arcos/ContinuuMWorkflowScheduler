package generator

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

type Options = config.GeneratorConfig

// SeedTasks loads task templates from the Globus dataset CSV, stores them in
// Redis, and returns the slice so the Generator can build its task pool.
func SeedTasks(s *store.RedisStore, cfg Options) ([]*models.Task, error) {
	tasks, err := LoadTasksFromCSV(cfg.DatasetPath, cfg.TaskCount, cfg.Seed)
	if err != nil {
		return nil, fmt.Errorf("load tasks from dataset: %w", err)
	}
	for _, task := range tasks {
		if err := s.StoreTask(task); err != nil {
			return nil, fmt.Errorf("storing task %s: %w", task.ID, err)
		}
	}
	return tasks, nil
}

// LoadTasksFromCSV reservoir-samples count task records from the enriched
// Globus dataset CSV in a single sequential pass (O(count) memory regardless
// of file size). seed=0 gives a non-deterministic sample.
func LoadTasksFromCSV(path string, count int, seed int64) ([]*models.Task, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open dataset %q: %w", path, err)
	}
	defer f.Close()

	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	r := csv.NewReader(f)
	r.ReuseRecord = true

	header, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	idx, err := buildColIndex(header, "tier", "execution_duration_ms", "cpu", "memory_mb", "disk_gb")
	if err != nil {
		return nil, err
	}

	type rawRow struct {
		tier     string
		duration int
		cpu      int
		memory   int
		disk     int
	}

	reservoir := make([]rawRow, 0, count)
	rowNum := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowNum+1, err)
		}

		tier := strings.ToUpper(strings.TrimSpace(rec[idx["tier"]]))
		if tier == "" {
			tier = "FOG"
		}
		duration := parseRound(rec[idx["execution_duration_ms"]], 1)
		if duration <= 0 {
			duration = 1
		}
		cpu := parseRound(rec[idx["cpu"]], 1)
		if cpu <= 0 {
			cpu = 1
		}
		memory := parseRound(rec[idx["memory_mb"]], 128)
		if memory <= 0 {
			memory = 128
		}
		disk := parseRound(rec[idx["disk_gb"]], 0)

		row := rawRow{tier, duration, cpu, memory, disk}
		rowNum++
		if len(reservoir) < count {
			reservoir = append(reservoir, row)
		} else {
			j := rng.Intn(rowNum)
			if j < count {
				reservoir[j] = row
			}
		}
	}

	if len(reservoir) == 0 {
		return nil, fmt.Errorf("dataset %q contains no data rows", path)
	}

	now := time.Now().UTC()
	tasks := make([]*models.Task, len(reservoir))
	tierCounts := map[string]int{}
	for i, row := range reservoir {
		tasks[i] = &models.Task{
			ID:         fmt.Sprintf("task_%04d", i+1),
			Name:       fmt.Sprintf("task_%04d", i+1),
			CPU:        row.cpu,
			Memory:     row.memory,
			Disk:       row.disk,
			Duration:   row.duration,
			TargetTier: row.tier,
			CreatedAt:  now,
		}
		tierCounts[row.tier]++
	}
	log.Printf("[Seeder] Loaded %d tasks from dataset — EDGE:%d FOG:%d CLOUD:%d (scanned %d rows)",
		len(tasks), tierCounts["EDGE"], tierCounts["FOG"], tierCounts["CLOUD"], rowNum)
	return tasks, nil
}

func buildColIndex(header []string, required ...string) (map[string]int, error) {
	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(h)] = i
	}
	for _, col := range required {
		if _, ok := idx[col]; !ok {
			return nil, fmt.Errorf("column %q not found in dataset header", col)
		}
	}
	return idx, nil
}

// parseRound parses a decimal string (e.g. "768.0") and rounds to the nearest
// integer. Returns defaultVal on parse error or negative result.
func parseRound(s string, defaultVal int) int {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || f < 0 {
		return defaultVal
	}
	return int(f + 0.5)
}

package generator

import (
	"log/slog"
	"math"
	"strings"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// PrepareTasks applies the workload options of cfg to sampled templates and
// returns new Task values (the input slice and its tasks are not modified):
//
//  1. demand_scale: CPU, memory and disk are multiplied by the factors of
//     the task's trace tier and rounded up; CPU is at least 1;
//  2. max_duration_ms: durations above the cap are set to the cap;
//  3. drop_unschedulable: tasks that fit no server in servers even when it
//     is idle are removed (servers may be nil when this option is off).
//
// With every option off the result equals the input value for value. No
// option draws random numbers.
func PrepareTasks(tasks []*models.Task, cfg Options, servers []*models.Server) []*models.Task {
	out := make([]*models.Task, 0, len(tasks))
	dropped := 0
	for _, t := range tasks {
		c := *t
		if f, ok := cfg.DemandScale[strings.ToLower(c.TargetTier)]; ok {
			c.CPU = max(1, scaleUp(c.CPU, f.CPU))
			c.Memory = scaleUp(c.Memory, f.Memory)
			c.Disk = scaleUp(c.Disk, f.Disk)
		}
		if cfg.MaxDurationMS > 0 && c.Duration > cfg.MaxDurationMS {
			c.Duration = cfg.MaxDurationMS
		}
		if cfg.DropUnschedulable && !fitsAny(&c, servers) {
			dropped++
			continue
		}
		out = append(out, &c)
	}
	if cfg.DropUnschedulable {
		slog.Info("dropped unschedulable templates", "component", "generator", "dropped", dropped, "kept", len(out))
	}
	return out
}

// scaleUp returns ceil(v*f). A tiny epsilon absorbs float error so exact
// products (e.g. 768 × 2/3 = 512) are not pushed to the next integer.
func scaleUp(v int, f float64) int {
	return int(math.Ceil(float64(v)*f - 1e-9))
}

func fitsAny(t *models.Task, servers []*models.Server) bool {
	for _, s := range servers {
		if s.FitsWhenIdle(t.CPU, t.Memory, t.Disk) {
			return true
		}
	}
	return false
}

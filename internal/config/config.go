// Package config parses the YAML configuration file (default: config.yaml)
// into typed structs consumed by the rest of the simulation. It exposes Load
// and Validate, plus the IntRange helper used for randomized range sampling.
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
)

// IntRange describes a discrete inclusive integer range [Min, Max] with the
// given Step. It accepts either a bare scalar (treated as Min==Max) or a
// {min, max, step} mapping in YAML.
type IntRange struct {
	Min  int `yaml:"min"`
	Max  int `yaml:"max"`
	Step int `yaml:"step"` // grid spacing between sampleable values; ApplyDefaults clamps to >=1

	// configured tracks whether the value was explicitly set in YAML so
	// ApplyDefaults can distinguish "user wrote 0" from "user omitted it".
	configured bool
}

// UnmarshalYAML decodes an IntRange from either a scalar int (Min=Max=value,
// Step=1) or a mapping with explicit min/max/step keys.
func (r *IntRange) UnmarshalYAML(value *yaml.Node) error {
	r.configured = true
	switch value.Kind {
	case yaml.ScalarNode:
		var v int
		if err := value.Decode(&v); err != nil {
			return err
		}
		r.Min = v
		r.Max = v
		r.Step = 1
		return nil
	case yaml.MappingNode:
		type raw IntRange
		var decoded raw
		if err := value.Decode(&decoded); err != nil {
			return err
		}
		r.Min = decoded.Min
		r.Max = decoded.Max
		r.Step = decoded.Step
		return nil
	default:
		return fmt.Errorf("expected integer or {min,max,step} range, got YAML kind %d", value.Kind)
	}
}

// ApplyDefaults fills in any unset bounds or step using the provided fallback
// values, and normalizes the range (swaps Min/Max if inverted, mirrors a
// single specified bound onto the other, clamps Step > 0).
func (r *IntRange) ApplyDefaults(min, max, step int) {
	if !r.configured {
		r.Min = min
		r.Max = max
		r.Step = step
		return
	}
	if r.Max == 0 && r.Min != 0 {
		r.Max = r.Min
	}
	if r.Min == 0 && r.Max != 0 {
		r.Min = r.Max
	}
	if r.Step <= 0 {
		r.Step = step
	}
	if r.Min > r.Max {
		r.Min, r.Max = r.Max, r.Min
	}
}

// ServerTierConfig describes the capacity envelope for one tier (Edge, Fog, or
// Cloud). Count servers will be sampled from the cross-product of the ranges.
type ServerTierConfig struct {
	Count       int      `yaml:"count"`
	CPU         IntRange `yaml:"cpu"`
	Memory      IntRange `yaml:"memory"`
	Disk        IntRange `yaml:"disk"`
	WANLatency  IntRange `yaml:"wan_latency"`   // round-trip latency in ms from this tier to the orchestrator
	CostPerHour float64  `yaml:"cost_per_hour"` // USD per server-hour
	PowerWatts  IntRange `yaml:"power_watts"`   // power draw in W
}

// ServersConfig groups the three tier specs and a shared RNG seed used by the
// seeder package to randomize server capacities.
type ServersConfig struct {
	Seed  int64            `yaml:"seed"` // 0 = non-deterministic (time-based)
	Edge  ServerTierConfig `yaml:"edge"`
	Fog   ServerTierConfig `yaml:"fog"`
	Cloud ServerTierConfig `yaml:"cloud"`
}

// TOPSISWeights holds the per-criterion weights used by the TOPSIS scheduler.
// Time+Cost+Energy must sum to 1.0 (validated); Util is an independent fourth
// criterion left out of that sum so the §6 weight-sensitivity analysis stays
// valid.
type TOPSISWeights struct {
	Time   float64 `yaml:"time"`   // weight on execution time (lower is better)
	Cost   float64 `yaml:"cost"`   // weight on $-cost per task (lower is better)
	Energy float64 `yaml:"energy"` // weight on energy draw (lower is better)
	// Util weights a fourth criterion: post-placement capacity utilisation
	// (lower is better). Independent of the time/cost/energy budget so the
	// existing 3-weight semantics in §6 weight sensitivity remain valid;
	// set to 0 to disable. Sensible non-zero default is 0.25.
	Util float64 `yaml:"util"`
}

// SchedulerConfig selects the placement algorithm and parameterizes it.
type SchedulerConfig struct {
	Algorithm     string        `yaml:"algorithm"`      // any name registered in internal/algos
	Seed          int64         `yaml:"seed"`           // 0 = non-deterministic (time-based)
	TOPSISWeights TOPSISWeights `yaml:"topsis_weights"` // only consulted when Algorithm == "topsis"
}

// GeneratorConfig controls how the synthetic workload is produced.
type GeneratorConfig struct {
	TaskCount         int    `yaml:"task_count"`
	Seed              int64  `yaml:"seed"`
	DatasetPath       string `yaml:"dataset_path"`
	EdgeMaxDurationMS int    `yaml:"edge_max_duration_ms"`
	FogMaxDurationMS  int    `yaml:"fog_max_duration_ms"`
	WorkflowSeed      int64  `yaml:"workflow_seed"`
	PriorityMin       int    `yaml:"priority_min"`
	PriorityMax       int    `yaml:"priority_max"`

	// Workload options (eval-v2). All default to off, which reproduces the
	// v1 workload exactly. They transform the sampled templates after
	// sampling, so the dataset RNG stream is unchanged; see
	// generator.PrepareTasks.

	// DemandScale multiplies each template's CPU, memory and disk by the
	// factors of its trace tier (keys edge, fog, cloud), rounding up, with
	// at least 1 core. Tiers without an entry are left unchanged.
	DemandScale map[string]DemandScale `yaml:"demand_scale"`
	// MaxDurationMS caps every template's duration (0 = no cap).
	MaxDurationMS int `yaml:"max_duration_ms"`
	// DropUnschedulable removes templates that fit no server even when it
	// is idle. This changes the workflow draws (identically for every
	// policy of a seed).
	DropUnschedulable bool `yaml:"drop_unschedulable"`
}

// DemandScale holds per-resource multipliers for one trace tier.
type DemandScale struct {
	CPU    float64 `yaml:"cpu"`
	Memory float64 `yaml:"memory"`
	Disk   float64 `yaml:"disk"`
}

// RedisConfig points the store package at a Redis instance.
type RedisConfig struct {
	Address string `yaml:"address"`
	DB      int    `yaml:"db"`
}

// RabbitMQConfig points the worker/results goroutines at an AMQP broker.
type RabbitMQConfig struct {
	URL string `yaml:"url"`
}

// WebConfig configures the HTTP/WebSocket dashboard server.
type WebConfig struct {
	Port int `yaml:"port"`
}

// ExperimentConfig bounds the simulation lifetime and logging cadence.
type ExperimentConfig struct {
	DurationSeconds    int  `yaml:"duration_seconds"`
	ProgressLogSeconds int  `yaml:"progress_log_seconds"`
	ExitOnComplete     bool `yaml:"exit_on_complete"` // exit process when experiment drains (for batch runs)
	// Labels are free-form run descriptors (topology, capacity, seed, ...)
	// copied verbatim into each exported result's meta.labels.
	Labels map[string]string `yaml:"labels"`
}

// Config is the root object decoded from the YAML file.
type Config struct {
	Servers    ServersConfig    `yaml:"servers"`
	Scheduler  SchedulerConfig  `yaml:"scheduler"`
	Generator  GeneratorConfig  `yaml:"generator"`
	Redis      RedisConfig      `yaml:"redis"`
	RabbitMQ   RabbitMQConfig   `yaml:"rabbitmq"`
	Web        WebConfig        `yaml:"web"`
	Experiment ExperimentConfig `yaml:"experiment"`
}

// Load reads the YAML file at path, applies defaults, validates the result,
// and returns the populated Config. Any error wraps the underlying cause so
// the caller can present it to the user.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	applyDefaults(&cfg)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &cfg, nil
}

// Validate checks that required fields are set, that the chosen scheduler
// algorithm is one of the known names, that the TOPSIS time/cost/energy
// weights sum to 1.0 (within a small tolerance), and that the auxiliary Util
// weight is non-negative.
func (cfg *Config) Validate() error {
	if cfg.Redis.Address == "" {
		return errors.New("redis.address is required")
	}
	if cfg.RabbitMQ.URL == "" {
		return errors.New("rabbitmq.url is required")
	}
	if cfg.Web.Port < 1 || cfg.Web.Port > 65535 {
		return fmt.Errorf("web.port %d is out of range [1, 65535]", cfg.Web.Port)
	}
	if !algos.Valid(cfg.Scheduler.Algorithm) {
		return fmt.Errorf("scheduler.algorithm %q is not valid (must be one of: %s)", cfg.Scheduler.Algorithm, strings.Join(algos.All(), ", "))
	}
	if cfg.Generator.TaskCount <= 0 {
		return fmt.Errorf("generator.task_count must be > 0, got %d", cfg.Generator.TaskCount)
	}
	if cfg.Generator.PriorityMin > cfg.Generator.PriorityMax {
		return fmt.Errorf("generator.priority_min (%d) > priority_max (%d)", cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	}
	for tier, f := range cfg.Generator.DemandScale {
		if tier != "edge" && tier != "fog" && tier != "cloud" {
			return fmt.Errorf("generator.demand_scale: unknown tier %q (use edge, fog, cloud)", tier)
		}
		if f.CPU <= 0 || f.Memory <= 0 || f.Disk <= 0 {
			return fmt.Errorf("generator.demand_scale.%s: cpu, memory and disk factors must all be > 0, got %+v", tier, f)
		}
	}
	if cfg.Generator.MaxDurationMS < 0 {
		return fmt.Errorf("generator.max_duration_ms must be >= 0, got %d", cfg.Generator.MaxDurationMS)
	}
	tw := cfg.Scheduler.TOPSISWeights
	// Time/Cost/Energy form a probability simplex; allow a 1e-3 slack for
	// human-written YAML that says 0.4/0.3/0.3.
	if sum := tw.Time + tw.Cost + tw.Energy; math.Abs(sum-1.0) > 0.001 {
		return fmt.Errorf("scheduler.topsis_weights time+cost+energy must sum to 1.0, got %.3f", sum)
	}
	if tw.Util < 0 {
		return fmt.Errorf("scheduler.topsis_weights.util must be >= 0, got %.3f", tw.Util)
	}
	return nil
}

// applyDefaults fills in sensible values for any field the user left blank.
// It is called from Load before Validate so the user only sees errors about
// values they explicitly set wrong, not about missing defaults.
func applyDefaults(cfg *Config) {
	if cfg.Scheduler.Algorithm == "" {
		cfg.Scheduler.Algorithm = "round_robin"
	}
	// Default TOPSIS weights are only applied when the user supplied no
	// weights at all (all four zero). Any non-zero entry means the user
	// intentionally specified their own simplex and we should not overwrite.
	if cfg.Scheduler.TOPSISWeights.Time == 0 &&
		cfg.Scheduler.TOPSISWeights.Cost == 0 &&
		cfg.Scheduler.TOPSISWeights.Energy == 0 &&
		cfg.Scheduler.TOPSISWeights.Util == 0 {
		cfg.Scheduler.TOPSISWeights = TOPSISWeights{Time: 0.4, Cost: 0.3, Energy: 0.3, Util: 0.25}
	}
	applyServerTierDefaults(&cfg.Servers.Edge, 10, IntRange{Min: 1, Max: 2, Step: 1}, IntRange{Min: 128, Max: 768, Step: 128}, IntRange{Min: 0, Max: 10, Step: 1}, IntRange{Min: 3, Max: 12, Step: 1}, 0.02, IntRange{Min: 10, Max: 25, Step: 5})
	applyServerTierDefaults(&cfg.Servers.Fog, 4, IntRange{Min: 4, Max: 8, Step: 1}, IntRange{Min: 2048, Max: 8192, Step: 512}, IntRange{Min: 40, Max: 140, Step: 10}, IntRange{Min: 20, Max: 45, Step: 5}, 0.25, IntRange{Min: 100, Max: 200, Step: 25})
	applyServerTierDefaults(&cfg.Servers.Cloud, 5, IntRange{Min: 16, Max: 32, Step: 2}, IntRange{Min: 16384, Max: 65536, Step: 4096}, IntRange{Min: 400, Max: 1600, Step: 100}, IntRange{Min: 80, Max: 160, Step: 10}, 1.50, IntRange{Min: 300, Max: 600, Step: 50})
	if cfg.Generator.TaskCount <= 0 {
		cfg.Generator.TaskCount = 120
	}
	if cfg.Generator.EdgeMaxDurationMS <= 0 {
		cfg.Generator.EdgeMaxDurationMS = 2500
	}
	if cfg.Generator.FogMaxDurationMS <= 0 {
		cfg.Generator.FogMaxDurationMS = 6500
	}
	if cfg.Generator.FogMaxDurationMS <= cfg.Generator.EdgeMaxDurationMS {
		cfg.Generator.FogMaxDurationMS = cfg.Generator.EdgeMaxDurationMS + 2000
	}
	if cfg.Generator.PriorityMin <= 0 {
		cfg.Generator.PriorityMin = 1
	}
	if cfg.Generator.PriorityMax <= 0 {
		cfg.Generator.PriorityMax = 10
	}
	if cfg.Web.Port == 0 {
		cfg.Web.Port = 8080
	}
	if cfg.Experiment.ProgressLogSeconds <= 0 {
		cfg.Experiment.ProgressLogSeconds = 10
	}
}

// applyServerTierDefaults fills in count, ranges, and cost for one tier from
// the supplied fallbacks, delegating to IntRange.ApplyDefaults for the ranges
// so partially-configured ranges are normalized consistently.
func applyServerTierDefaults(tier *ServerTierConfig, defaultCount int, cpu, memory, disk, latency IntRange, defaultCost float64, power IntRange) {
	if tier.Count <= 0 {
		tier.Count = defaultCount
	}
	tier.CPU.ApplyDefaults(cpu.Min, cpu.Max, cpu.Step)
	tier.Memory.ApplyDefaults(memory.Min, memory.Max, memory.Step)
	tier.Disk.ApplyDefaults(disk.Min, disk.Max, disk.Step)
	tier.WANLatency.ApplyDefaults(latency.Min, latency.Max, latency.Step)
	tier.PowerWatts.ApplyDefaults(power.Min, power.Max, power.Step)
	if tier.CostPerHour == 0 {
		tier.CostPerHour = defaultCost
	}
}

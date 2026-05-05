package config

import (
	"errors"
	"fmt"
	"math"
	"os"

	"gopkg.in/yaml.v3"
)

type IntRange struct {
	Min  int `yaml:"min"`
	Max  int `yaml:"max"`
	Step int `yaml:"step"`

	configured bool
}

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

type ServerTierConfig struct {
	Count       int      `yaml:"count"`
	CPU         IntRange `yaml:"cpu"`
	Memory      IntRange `yaml:"memory"`
	Disk        IntRange `yaml:"disk"`
	WANLatency  IntRange `yaml:"wan_latency"`
	CostPerHour float64  `yaml:"cost_per_hour"` // USD per server-hour
	PowerWatts  IntRange `yaml:"power_watts"`   // power draw in W
}

type ServersConfig struct {
	Seed  int64            `yaml:"seed"`
	Edge  ServerTierConfig `yaml:"edge"`
	Fog   ServerTierConfig `yaml:"fog"`
	Cloud ServerTierConfig `yaml:"cloud"`
}

type TOPSISWeights struct {
	Time   float64 `yaml:"time"`
	Cost   float64 `yaml:"cost"`
	Energy float64 `yaml:"energy"`
}

type SchedulerConfig struct {
	Algorithm     string        `yaml:"algorithm"`
	Seed          int64         `yaml:"seed"`
	TOPSISWeights TOPSISWeights `yaml:"topsis_weights"`
}

type GeneratorConfig struct {
	TaskCount         int    `yaml:"task_count"`
	Seed              int64  `yaml:"seed"`
	DatasetPath       string `yaml:"dataset_path"`
	EdgeMaxDurationMS int    `yaml:"edge_max_duration_ms"`
	FogMaxDurationMS  int    `yaml:"fog_max_duration_ms"`
	WorkflowSeed      int64  `yaml:"workflow_seed"`
	PriorityMin       int    `yaml:"priority_min"`
	PriorityMax       int    `yaml:"priority_max"`
}

type RedisConfig struct {
	Address string `yaml:"address"`
	DB      int    `yaml:"db"`
}

type RabbitMQConfig struct {
	URL string `yaml:"url"`
}

type WebConfig struct {
	Port int `yaml:"port"`
}

type ExperimentConfig struct {
	DurationSeconds    int  `yaml:"duration_seconds"`
	ProgressLogSeconds int  `yaml:"progress_log_seconds"`
	ExitOnComplete     bool `yaml:"exit_on_complete"` // exit process when experiment drains (for batch runs)
}

type Config struct {
	Servers    ServersConfig    `yaml:"servers"`
	Scheduler  SchedulerConfig  `yaml:"scheduler"`
	Generator  GeneratorConfig  `yaml:"generator"`
	Redis      RedisConfig      `yaml:"redis"`
	RabbitMQ   RabbitMQConfig   `yaml:"rabbitmq"`
	Web        WebConfig        `yaml:"web"`
	Experiment ExperimentConfig `yaml:"experiment"`
}

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
	validAlgos := map[string]bool{
		"round_robin": true, "random": true, "best_fit": true,
		"worst_fit": true, "topsis": true,
	}
	if !validAlgos[cfg.Scheduler.Algorithm] {
		return fmt.Errorf("scheduler.algorithm %q is not valid (must be one of: round_robin, random, best_fit, worst_fit, topsis)", cfg.Scheduler.Algorithm)
	}
	if cfg.Generator.TaskCount <= 0 {
		return fmt.Errorf("generator.task_count must be > 0, got %d", cfg.Generator.TaskCount)
	}
	if cfg.Generator.PriorityMin > cfg.Generator.PriorityMax {
		return fmt.Errorf("generator.priority_min (%d) > priority_max (%d)", cfg.Generator.PriorityMin, cfg.Generator.PriorityMax)
	}
	tw := cfg.Scheduler.TOPSISWeights
	if sum := tw.Time + tw.Cost + tw.Energy; math.Abs(sum-1.0) > 0.001 {
		return fmt.Errorf("scheduler.topsis_weights must sum to 1.0, got %.3f", sum)
	}
	return nil
}

func applyDefaults(cfg *Config) {
	if cfg.Scheduler.Algorithm == "" {
		cfg.Scheduler.Algorithm = "round_robin"
	}
	// Default TOPSIS weights (only applied when all three are zero)
	if cfg.Scheduler.TOPSISWeights.Time == 0 &&
		cfg.Scheduler.TOPSISWeights.Cost == 0 &&
		cfg.Scheduler.TOPSISWeights.Energy == 0 {
		cfg.Scheduler.TOPSISWeights = TOPSISWeights{Time: 0.4, Cost: 0.3, Energy: 0.3}
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

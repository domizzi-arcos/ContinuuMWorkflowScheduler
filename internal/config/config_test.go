package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// TestIntRangeUnmarshalSupportsScalarAndMap pins both YAML shapes accepted by
// IntRange: a bare scalar (which collapses to Min=Max=value) and the explicit
// {min, max, step} map. Both shapes are exercised in config.yaml today.
func TestIntRangeUnmarshalSupportsScalarAndMap(t *testing.T) {
	var cfg struct {
		CPU    IntRange `yaml:"cpu"`
		Memory IntRange `yaml:"memory"`
	}

	input := []byte(`
cpu: 4
memory:
  min: 2048
  max: 8192
  step: 512
`)
	if err := yaml.Unmarshal(input, &cfg); err != nil {
		t.Fatalf("yaml unmarshal failed: %v", err)
	}

	cfg.CPU.ApplyDefaults(1, 2, 1)
	cfg.Memory.ApplyDefaults(1024, 4096, 256)

	if cfg.CPU.Min != 4 || cfg.CPU.Max != 4 || cfg.CPU.Step != 1 {
		t.Fatalf("cpu range decoded incorrectly: %+v", cfg.CPU)
	}
	if cfg.Memory.Min != 2048 || cfg.Memory.Max != 8192 || cfg.Memory.Step != 512 {
		t.Fatalf("memory range decoded incorrectly: %+v", cfg.Memory)
	}
}

// TestApplyDefaultsAddsGeneratorDefaults documents the fallback values used
// when config.yaml omits the generator/server blocks. These defaults are what
// any reader of a fresh config gets if a field is left unset.
func TestApplyDefaultsAddsGeneratorDefaults(t *testing.T) {
	cfg := &Config{
		Generator: GeneratorConfig{},
	}

	applyDefaults(cfg)

	if cfg.Generator.TaskCount != 120 {
		t.Fatalf("expected default task count to be 120, got %d", cfg.Generator.TaskCount)
	}
	if cfg.Generator.EdgeMaxDurationMS != 2500 {
		t.Fatalf("expected default edge duration threshold to be 2500, got %d", cfg.Generator.EdgeMaxDurationMS)
	}
	if cfg.Servers.Edge.CPU.Min == 0 || cfg.Servers.Edge.CPU.Max == 0 {
		t.Fatalf("expected edge CPU defaults to be populated, got %+v", cfg.Servers.Edge.CPU)
	}
}

func TestValidateWorkloadOptions(t *testing.T) {
	base := func() *Config {
		c := &Config{}
		c.Redis.Address, c.RabbitMQ.URL, c.Web.Port = "r", "q", 8080
		c.Scheduler.Algorithm = "topsis"
		c.Scheduler.TOPSISWeights = TOPSISWeights{Time: 0.4, Cost: 0.3, Energy: 0.3}
		c.Generator.TaskCount, c.Generator.PriorityMin, c.Generator.PriorityMax = 1, 1, 1
		return c
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("base config invalid: %v", err)
	}
	c := base()
	c.Generator.DemandScale = map[string]DemandScale{"cloud": {CPU: 0.25, Memory: 0.125, Disk: 0.25}}
	c.Generator.MaxDurationMS = 30000
	if err := c.Validate(); err != nil {
		t.Errorf("valid options rejected: %v", err)
	}
	for name, mut := range map[string]func(*Config){
		"unknown tier": func(c *Config) { c.Generator.DemandScale = map[string]DemandScale{"EDGE": {1, 1, 1}} },
		"zero factor":  func(c *Config) { c.Generator.DemandScale = map[string]DemandScale{"fog": {1, 0, 1}} },
		"negative cap": func(c *Config) { c.Generator.MaxDurationMS = -1 },
	} {
		c := base()
		mut(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

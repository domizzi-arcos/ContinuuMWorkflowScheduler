package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

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

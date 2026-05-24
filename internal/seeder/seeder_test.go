package seeder

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
)

// TestBuildTierServersProducesUniqueProfiles guards the uniqueness invariant
// for an edge tier: every seeded server must have a distinct (cpu, mem, disk,
// latency) profile so the scheduler can discriminate between candidates.
func TestBuildTierServersProducesUniqueProfiles(t *testing.T) {
	tier := tierSeedConfig{
		name:     "edge",
		nodeType: "EDGE",
		count:    6,
		cpu:      config.IntRange{Min: 1, Max: 2, Step: 1},
		memory:   config.IntRange{Min: 128, Max: 512, Step: 128},
		disk:     config.IntRange{Min: 0, Max: 3, Step: 1},
		latency:  config.IntRange{Min: 3, Max: 9, Step: 1},
	}

	servers, err := buildTierServers(tier, rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatalf("buildTierServers failed: %v", err)
	}
	if len(servers) != tier.count {
		t.Fatalf("expected %d servers, got %d", tier.count, len(servers))
	}

	seen := make(map[string]bool, len(servers))
	for _, srv := range servers {
		key := fmt.Sprintf("%d/%d/%d/%d", srv.MaxCPU, srv.MaxMemory, srv.MaxDisk, srv.Latency)
		if seen[key] {
			t.Fatalf("duplicate server profile generated: %s", key)
		}
		seen[key] = true
	}
}

// TestBuildTierServersKeepsFogDiskPositive pins the rule that non-edge tiers
// (fog, cloud) must always have at least 1 GB of disk — a fog or cloud node
// with 0 GB is treated as misconfigured by the scheduler and never receives tasks.
func TestBuildTierServersKeepsFogDiskPositive(t *testing.T) {
	tier := tierSeedConfig{
		name:     "fog",
		nodeType: "FOG",
		count:    5,
		cpu:      config.IntRange{Min: 4, Max: 6, Step: 1},
		memory:   config.IntRange{Min: 2048, Max: 4096, Step: 512},
		disk:     config.IntRange{Min: 0, Max: 2, Step: 1},
		latency:  config.IntRange{Min: 20, Max: 40, Step: 5},
	}

	servers, err := buildTierServers(tier, rand.New(rand.NewSource(7)))
	if err != nil {
		t.Fatalf("buildTierServers failed: %v", err)
	}
	for _, srv := range servers {
		if srv.MaxDisk <= 0 {
			t.Fatalf("expected fog disk to stay positive, got %d for %s", srv.MaxDisk, srv.ID)
		}
	}
}

// TestBuildTierServersEnsuresDistinctDiskValues exercises the secondary
// uniqueness rule: when the disk range has at least as many distinct steps
// as the requested server count, every server within the tier should end up
// with a distinct disk capacity (improves topology diversity for benchmarks).
func TestBuildTierServersEnsuresDistinctDiskValues(t *testing.T) {
	tier := tierSeedConfig{
		name:     "edge",
		nodeType: "EDGE",
		count:    3,
		cpu:      config.IntRange{Min: 1, Max: 2, Step: 1},
		memory:   config.IntRange{Min: 128, Max: 768, Step: 128},
		disk:     config.IntRange{Min: 0, Max: 20, Step: 1},
		latency:  config.IntRange{Min: 3, Max: 12, Step: 1},
	}

	servers, err := buildTierServers(tier, rand.New(rand.NewSource(42)))
	if err != nil {
		t.Fatalf("buildTierServers failed: %v", err)
	}
	diskSeen := make(map[int]bool, len(servers))
	for _, srv := range servers {
		if diskSeen[srv.MaxDisk] {
			t.Fatalf("duplicate edge disk capacity found: %dGB", srv.MaxDisk)
		}
		diskSeen[srv.MaxDisk] = true
	}
}

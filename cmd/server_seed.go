package main

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

type tierSeedConfig struct {
	name        string
	nodeType    string
	count       int
	cpu         config.IntRange
	memory      config.IntRange
	disk        config.IntRange
	latency     config.IntRange
	costPerHour float64
	power       config.IntRange
}

type serverProfile struct {
	cpu     int
	memory  int
	disk    int
	latency int
	power   int
}

func seedServers(s *store.RedisStore, cfg *config.Config) error {
	_, seed := newServerSeedRNG(cfg.Servers.Seed)
	slog.Info("randomizing server capacities", "component", "seeder", "seed", seed)

	tiers := []tierSeedConfig{
		{
			name:        "edge",
			nodeType:    "EDGE",
			count:       cfg.Servers.Edge.Count,
			cpu:         cfg.Servers.Edge.CPU,
			memory:      cfg.Servers.Edge.Memory,
			disk:        cfg.Servers.Edge.Disk,
			latency:     cfg.Servers.Edge.WANLatency,
			costPerHour: cfg.Servers.Edge.CostPerHour,
			power:       cfg.Servers.Edge.PowerWatts,
		},
		{
			name:        "fog",
			nodeType:    "FOG",
			count:       cfg.Servers.Fog.Count,
			cpu:         cfg.Servers.Fog.CPU,
			memory:      cfg.Servers.Fog.Memory,
			disk:        cfg.Servers.Fog.Disk,
			latency:     cfg.Servers.Fog.WANLatency,
			costPerHour: cfg.Servers.Fog.CostPerHour,
			power:       cfg.Servers.Fog.PowerWatts,
		},
		{
			name:        "cloud",
			nodeType:    "CLOUD",
			count:       cfg.Servers.Cloud.Count,
			cpu:         cfg.Servers.Cloud.CPU,
			memory:      cfg.Servers.Cloud.Memory,
			disk:        cfg.Servers.Cloud.Disk,
			latency:     cfg.Servers.Cloud.WANLatency,
			costPerHour: cfg.Servers.Cloud.CostPerHour,
			power:       cfg.Servers.Cloud.PowerWatts,
		},
	}

	for i, tier := range tiers {
		rng := rand.New(rand.NewSource(seed + int64(i)))
		servers, err := buildTierServers(tier, rng)
		if err != nil {
			return err
		}
		for _, srv := range servers {
			if err := s.StoreServer(srv); err != nil {
				return fmt.Errorf("storing server %s: %w", srv.ID, err)
			}
			slog.Debug("server seeded", "component", "seeder",
				"id", srv.ID, "type", srv.Type, "cpu", srv.MaxCPU,
				"mem_mb", srv.MaxMemory, "disk_gb", srv.MaxDisk,
				"latency_ms", srv.Latency, "cost_per_hr", srv.CostPerHour, "power_w", srv.PowerWatts,
			)
		}
	}

	return nil
}

func buildTierServers(tier tierSeedConfig, rng *rand.Rand) ([]*models.Server, error) {
	if tier.count <= 0 {
		return nil, nil
	}
	combinations := tierCombinationCount(tier)
	if combinations < int64(tier.count) {
		return nil, fmt.Errorf(
			"tier %s requests %d unique servers but only %d unique capacity combinations are available",
			tier.name, tier.count, combinations,
		)
	}

	diskOptions := rangeOptionCount(tier.disk)
	ensureUniqueDisk := diskOptions >= tier.count
	usedDisks := make(map[int]struct{}, tier.count)
	seenProfiles := make(map[string]struct{}, tier.count)
	servers := make([]*models.Server, 0, tier.count)
	for i := 0; i < tier.count; i++ {
		profile, err := uniqueProfile(tier, seenProfiles, usedDisks, ensureUniqueDisk, rng)
		if err != nil {
			return nil, err
		}
		servers = append(servers, &models.Server{
			ID:          fmt.Sprintf("%s_%d", tier.name, i),
			Type:        tier.nodeType,
			MaxCPU:      profile.cpu,
			MaxMemory:   profile.memory,
			MaxDisk:     profile.disk,
			Latency:     profile.latency,
			CostPerHour: tier.costPerHour,
			PowerWatts:  profile.power,
		})
	}
	return servers, nil
}

func uniqueProfile(
	tier tierSeedConfig,
	seen map[string]struct{},
	usedDisks map[int]struct{},
	ensureUniqueDisk bool,
	rng *rand.Rand,
) (serverProfile, error) {
	maxAttempts := int(math.Max(64, float64(tier.count*16)))
	for attempt := 0; attempt < maxAttempts; attempt++ {
		profile := serverProfile{
			cpu:     sampleRange(tier.cpu, rng),
			memory:  sampleRange(tier.memory, rng),
			disk:    sampleRange(tier.disk, rng),
			latency: sampleRange(tier.latency, rng),
			power:   sampleRange(tier.power, rng),
		}
		// Edge nodes may legitimately have no local disk. Fog and cloud nodes must
		// always keep some disk available, even if the configured range includes 0.
		if tier.nodeType != "EDGE" && profile.disk <= 0 {
			profile.disk = 1
		}
		if ensureUniqueDisk {
			if _, diskUsed := usedDisks[profile.disk]; diskUsed {
				continue
			}
		}

		key := fmt.Sprintf("%d/%d/%d/%d/%d", profile.cpu, profile.memory, profile.disk, profile.latency, profile.power)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		if ensureUniqueDisk {
			usedDisks[profile.disk] = struct{}{}
		}
		return profile, nil
	}
	return serverProfile{}, fmt.Errorf("could not generate a unique server profile for tier %s", tier.name)
}

func newServerSeedRNG(seed int64) (*rand.Rand, int64) {
	if seed == 0 {
		seed = time.Now().UTC().UnixNano()
	}
	return rand.New(rand.NewSource(seed)), seed
}

func sampleRange(r config.IntRange, rng *rand.Rand) int {
	options := rangeOptionCount(r)
	if options <= 1 {
		return r.Min
	}
	return r.Min + rng.Intn(options)*maxInt(1, r.Step)
}

func rangeOptionCount(r config.IntRange) int {
	step := maxInt(1, r.Step)
	if r.Max < r.Min {
		return 0
	}
	return ((r.Max - r.Min) / step) + 1
}

func tierCombinationCount(tier tierSeedConfig) int64 {
	return int64(rangeOptionCount(tier.cpu)) *
		int64(rangeOptionCount(tier.memory)) *
		int64(rangeOptionCount(tier.disk)) *
		int64(rangeOptionCount(tier.latency))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

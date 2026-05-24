// Package seeder populates Redis with synthetic tier-balanced server profiles
// before the simulation starts. Each tier's server pool is sized and randomized
// from `config.ServersConfig`, with deterministic seeds when configured.
package seeder

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

// tierSeedConfig bundles the resolved range specs and per-server cost for one
// tier so buildTierServers can sample profiles without re-reading the global
// config.
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

// serverProfile is one sampled point in the (cpu, memory, disk, latency, power)
// capacity space; used to deduplicate generated servers within a tier.
type serverProfile struct {
	cpu     int
	memory  int
	disk    int
	latency int
	power   int
}

// SeedServers generates Edge, Fog, and Cloud server records by sampling each
// tier's configured capacity ranges and persists them via the supplied
// RedisStore. Each tier uses an independent RNG derived from the base seed so
// runs with the same seed produce identical fleets.
func SeedServers(s *store.RedisStore, cfg *config.Config) error {
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
		// Offset the seed per tier so tiers are independent yet still
		// reproducible from the single configured base seed.
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

// buildTierServers samples `tier.count` unique server profiles from the tier's
// configured ranges. It aborts up-front if the cartesian product of available
// capacity options is smaller than the requested count — there is no way to
// satisfy uniqueness in that case so we fail loudly instead of looping forever.
func buildTierServers(tier tierSeedConfig, rng *rand.Rand) ([]*models.Server, error) {
	if tier.count <= 0 {
		return nil, nil
	}
	combinations := tierCombinationCount(tier)
	// Abort early: requesting more unique servers than the capacity grid
	// allows would cause uniqueProfile to spin until maxAttempts and fail.
	if combinations < int64(tier.count) {
		return nil, fmt.Errorf(
			"tier %s requests %d unique servers but only %d unique capacity combinations are available",
			tier.name, tier.count, combinations,
		)
	}

	diskOptions := rangeOptionCount(tier.disk)
	// When disk has at least `count` distinct values, force unique disks per
	// server in the tier so visualisations stay visually distinguishable.
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

// uniqueProfile rejection-samples a (cpu, memory, disk, latency, power) tuple
// not already present in `seen`, optionally enforcing a fresh disk size too.
// Returns an error if no unique profile is found within a bounded attempt
// budget derived from the requested tier count.
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
		// Fog and Cloud nodes always need at least 1 GB of disk; Edge may
		// legitimately have 0 (diskless sensors).
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

// newServerSeedRNG returns a fresh RNG and the effective seed. A configured
// seed of 0 means "non-deterministic": fall back to the current wall clock so
// successive runs differ.
func newServerSeedRNG(seed int64) (*rand.Rand, int64) {
	if seed == 0 {
		// seed == 0 in config means "use time.Now()" — i.e. caller wants a
		// fresh random fleet on every run rather than a reproducible one.
		seed = time.Now().UTC().UnixNano()
	}
	return rand.New(rand.NewSource(seed)), seed
}

// sampleRange draws one value uniformly from r's discrete grid (Min, Min+Step,
// …, Max). Degenerate ranges (single option) short-circuit to r.Min.
func sampleRange(r config.IntRange, rng *rand.Rand) int {
	options := rangeOptionCount(r)
	if options <= 1 {
		return r.Min
	}
	return r.Min + rng.Intn(options)*maxInt(1, r.Step)
}

// rangeOptionCount returns how many distinct values fit in r's [Min, Max] grid
// at the configured Step. Returns 0 for inverted ranges (Max < Min).
func rangeOptionCount(r config.IntRange) int {
	step := maxInt(1, r.Step)
	if r.Max < r.Min {
		return 0
	}
	return ((r.Max - r.Min) / step) + 1
}

// tierCombinationCount is the size of the cartesian product of the tier's CPU,
// memory, disk, and latency ranges — the upper bound on unique server profiles
// available for sampling. Power is intentionally excluded to keep the uniqueness
// check focused on scheduling-relevant capacity.
func tierCombinationCount(tier tierSeedConfig) int64 {
	return int64(rangeOptionCount(tier.cpu)) *
		int64(rangeOptionCount(tier.memory)) *
		int64(rangeOptionCount(tier.disk)) *
		int64(rangeOptionCount(tier.latency))
}

// maxInt returns the larger of a and b. Local helper to avoid importing a
// generic math package solely for two ints.
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

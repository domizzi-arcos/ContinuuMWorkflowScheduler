package seeder

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/config"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/store"
)

var update = flag.Bool("update", false, "rewrite golden files")

func r(min, max, step int) config.IntRange { return config.IntRange{Min: min, Max: max, Step: step} }

// goldenProfiles mirrors CAPACITY_PROFILES in scripts/grid_benchmark.py.
var goldenProfiles = map[string][3]config.ServerTierConfig{
	"small": {
		{CPU: r(1, 2, 1), Memory: r(128, 512, 128), Disk: r(0, 10, 1), WANLatency: r(3, 12, 1), CostPerHour: 0.02, PowerWatts: r(10, 25, 5)},
		{CPU: r(2, 4, 1), Memory: r(512, 2048, 512), Disk: r(20, 80, 10), WANLatency: r(20, 45, 5), CostPerHour: 0.10, PowerWatts: r(80, 150, 25)},
		{CPU: r(4, 8, 2), Memory: r(2048, 8192, 2048), Disk: r(100, 400, 50), WANLatency: r(80, 160, 10), CostPerHour: 0.60, PowerWatts: r(200, 400, 50)},
	},
	"medium": {
		{CPU: r(1, 2, 1), Memory: r(128, 768, 128), Disk: r(0, 20, 1), WANLatency: r(3, 12, 1), CostPerHour: 0.02, PowerWatts: r(10, 25, 5)},
		{CPU: r(4, 8, 1), Memory: r(2048, 8192, 512), Disk: r(40, 140, 10), WANLatency: r(20, 45, 5), CostPerHour: 0.25, PowerWatts: r(100, 200, 25)},
		{CPU: r(16, 32, 2), Memory: r(16384, 65536, 4096), Disk: r(400, 1600, 100), WANLatency: r(80, 160, 10), CostPerHour: 1.50, PowerWatts: r(300, 600, 50)},
	},
	"large": {
		{CPU: r(2, 4, 1), Memory: r(512, 2048, 512), Disk: r(0, 40, 2), WANLatency: r(3, 12, 1), CostPerHour: 0.04, PowerWatts: r(15, 40, 5)},
		{CPU: r(8, 16, 2), Memory: r(8192, 32768, 2048), Disk: r(100, 400, 50), WANLatency: r(20, 45, 5), CostPerHour: 0.50, PowerWatts: r(150, 300, 25)},
		{CPU: r(32, 64, 4), Memory: r(65536, 262144, 16384), Disk: r(800, 3200, 200), WANLatency: r(80, 160, 10), CostPerHour: 3.00, PowerWatts: r(400, 800, 100)},
	},
}

var goldenTopologies = []struct {
	label            string
	edge, fog, cloud int
}{
	{"uniform_s", 2, 2, 2}, {"uniform_l", 8, 8, 8}, {"edge_heavy", 8, 3, 1}, {"cloud_heavy", 1, 3, 8},
}

var goldenSeeds = []int64{1, 2, 3, 42}

// goldenConfig builds the config the way the campaign does: YAML through
// config.Load, so ApplyDefaults normalisation (e.g. min 0 mirrored to max)
// is part of what is pinned.
func goldenConfig(t *testing.T, profile string, topo int, seed int64) *config.Config {
	t.Helper()
	p := goldenProfiles[profile]
	tp := goldenTopologies[topo]
	var b strings.Builder
	fmt.Fprintf(&b, "servers:\n  seed: %d\n", seed)
	for i, name := range []string{"edge", "fog", "cloud"} {
		tc := p[i]
		count := [3]int{tp.edge, tp.fog, tp.cloud}[i]
		fmt.Fprintf(&b, "  %s:\n    count: %d\n    cost_per_hour: %g\n", name, count, tc.CostPerHour)
		for _, kv := range []struct {
			key string
			r   config.IntRange
		}{{"cpu", tc.CPU}, {"memory", tc.Memory}, {"disk", tc.Disk}, {"wan_latency", tc.WANLatency}, {"power_watts", tc.PowerWatts}} {
			fmt.Fprintf(&b, "    %s: {min: %d, max: %d, step: %d}\n", kv.key, kv.r.Min, kv.r.Max, kv.r.Step)
		}
	}
	b.WriteString("redis: {address: x}\nrabbitmq: {url: x}\n")
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg
}

func formatServers(servers []*models.Server) string {
	sorted := append([]*models.Server(nil), servers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	var b strings.Builder
	for _, s := range sorted {
		fmt.Fprintf(&b, "  %s %s cpu=%d mem=%d disk=%d lat=%d cost=%g power=%d\n",
			s.ID, s.Type, s.MaxCPU, s.MaxMemory, s.MaxDisk, s.Latency, s.CostPerHour, s.PowerWatts)
	}
	return b.String()
}

// seededViaRedis runs SeedServers against miniredis and reads the fleet back.
func seededViaRedis(t *testing.T, cfg *config.Config) []*models.Server {
	t.Helper()
	mr := miniredis.RunT(t)
	s, err := store.NewRedisStore(mr.Addr(), 0)
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	if err := SeedServers(s, cfg); err != nil {
		t.Fatalf("SeedServers: %v", err)
	}
	servers, err := s.GetAllServers()
	if err != nil {
		t.Fatalf("GetAllServers: %v", err)
	}
	return servers
}

// TestSeedServersGolden pins the fleets SeedServers produces for every
// capacity profile across several topologies and seeds. Regenerate only
// with intent: go test ./internal/seeder -run Golden -update
func TestSeedServersGolden(t *testing.T) {
	var b strings.Builder
	for _, profile := range []string{"small", "medium", "large"} {
		for ti, topo := range goldenTopologies {
			for _, seed := range goldenSeeds {
				fmt.Fprintf(&b, "%s %s seed=%d\n", profile, topo.label, seed)
				b.WriteString(formatServers(seededViaRedis(t, goldenConfig(t, profile, ti, seed))))
			}
		}
	}
	compareGolden(t, filepath.Join("testdata", "servers.golden"), b.String())
}

func compareGolden(t *testing.T, path, got string) {
	t.Helper()
	if *update {
		if err := os.WriteFile(path, []byte(got), 0644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if string(want) != got {
		t.Fatalf("output differs from %s; diff it against a run with -update", path)
	}
}

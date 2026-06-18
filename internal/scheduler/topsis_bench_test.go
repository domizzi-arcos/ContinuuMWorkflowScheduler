package scheduler

import (
	"fmt"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// benchServers builds n deterministic candidate servers spread across the
// cost/energy/latency space so TOPSIS has a non-degenerate matrix to score.
func benchServers(n int) []*models.Server {
	servers := make([]*models.Server, n)
	for i := 0; i < n; i++ {
		servers[i] = &models.Server{
			ID:          fmt.Sprintf("srv-%05d", i),
			Type:        "FOG",
			MaxCPU:      8,
			MaxMemory:   16384,
			MaxDisk:     500,
			UsedCPU:     i % 6,
			UsedMemory:  (i % 6) * 1024,
			UsedDisk:    (i % 6) * 20,
			Latency:     5 + i%50,
			CostPerHour: 0.10 + float64(i%20)*0.05,
			PowerWatts:  40 + i%200,
		}
	}
	return servers
}

// BenchmarkTOPSISSelect measures the per-decision cost of the TOPSIS policy as
// the candidate pool grows. The point of sweeping N is to show where the
// numeric inner loops (vecNorm / weightedNorm / distance) actually start to
// matter: at the experiment's real pool sizes (3-9 servers/tier) the cost is
// dominated by sort + allocation, not arithmetic, which is why vectorisation
// (Go 1.26 simd) buys nothing here. It only becomes relevant in the thousands.
func BenchmarkTOPSISSelect(b *testing.B) {
	task := &models.Task{ID: "t", Name: "bench", CPU: 2, Memory: 2048, Disk: 20, Duration: 1500, TargetTier: "FOG"}
	tp := NewTOPSIS(0.4, 0.3, 0.3, 0.25)

	for _, n := range []int{3, 9, 27, 100, 1000} {
		servers := benchServers(n)
		b.Run(fmt.Sprintf("candidates=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if got := tp.Select(task, servers); got == nil {
					b.Fatal("nil selection")
				}
			}
		})
	}
}

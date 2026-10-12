package scheduler

import (
	"fmt"
	"testing"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/algos"
	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

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

// BenchmarkSelect measures one placement decision for every registered
// policy at candidate-set sizes spanning the campaign topologies (6, 15, 24
// nodes) plus 100. This is the decision-overhead table of the paper:
// go test -bench=Select -benchmem -run='^$' ./internal/scheduler/
func BenchmarkSelect(b *testing.B) {
	task := &models.Task{ID: "t", Name: "bench", CPU: 2, Memory: 2048, Disk: 20, Duration: 1500, TargetTier: "FOG"}
	for _, name := range algos.All() {
		for _, n := range []int{6, 15, 24, 100} {
			servers := benchServers(n)
			algo := NewAlgorithm(name, 42, 0.4, 0.3, 0.3, 0.25)
			b.Run(fmt.Sprintf("%s/n=%d", name, n), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if got := algo.Select(task, servers); got == nil {
						b.Fatal("nil selection")
					}
				}
			})
		}
	}
}

// BenchmarkTOPSISSelect is the v1 benchmark, kept for comparison with
// earlier measurements.
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

package scheduler

import (
	"math"

	"github.com/pcturnes/distributed-workflow-scheduler/internal/models"
)

// TOPSIS scores each candidate server on three criteria — estimated execution
// time, monetary cost, and energy consumption — using the Technique for Order
// of Preference by Similarity to Ideal Solution (TOPSIS) method.
// All three criteria are "lower is better", so the positive-ideal solution is
// the minimum on every axis and the negative-ideal is the maximum.
type TOPSIS struct {
	wTime   float64
	wCost   float64
	wEnergy float64
}

func NewTOPSIS(wTime, wCost, wEnergy float64) *TOPSIS {
	total := wTime + wCost + wEnergy
	if total <= 0 {
		wTime, wCost, wEnergy = 1.0/3, 1.0/3, 1.0/3
		total = 1
	}
	return &TOPSIS{
		wTime:   wTime / total,
		wCost:   wCost / total,
		wEnergy: wEnergy / total,
	}
}

func (t *TOPSIS) Name() string { return "topsis" }

func (t *TOPSIS) Select(task *models.Task, candidates []*models.Server) *models.Server {
	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	n := len(candidates)
	durSec := float64(task.Duration) / 1000.0

	// Raw criteria matrix: all "lower is better"
	rawTime := make([]float64, n)
	rawCost := make([]float64, n)
	rawEnergy := make([]float64, n)
	for i, srv := range candidates {
		rawTime[i] = float64(task.Duration + srv.Latency) // effective ms
		rawCost[i] = srv.CostPerHour * durSec / 3600.0    // USD
		rawEnergy[i] = float64(srv.PowerWatts) * durSec / 3600.0 // Wh
	}

	// Vector normalisation then weight application
	wt := weightedNorm(rawTime, t.wTime)
	wc := weightedNorm(rawCost, t.wCost)
	we := weightedNorm(rawEnergy, t.wEnergy)

	// Positive ideal = min per criterion; negative ideal = max
	idealT, antiT := minMaxSlice(wt)
	idealC, antiC := minMaxSlice(wc)
	idealE, antiE := minMaxSlice(we)

	best, bestScore := 0, -1.0
	for i := range candidates {
		dPlus := math.Sqrt(sq(wt[i]-idealT) + sq(wc[i]-idealC) + sq(we[i]-idealE))
		dMinus := math.Sqrt(sq(wt[i]-antiT) + sq(wc[i]-antiC) + sq(we[i]-antiE))
		var score float64
		if dPlus+dMinus > 0 {
			score = dMinus / (dPlus + dMinus)
		}
		if score > bestScore {
			best = i
			bestScore = score
		}
	}
	return candidates[best]
}

func weightedNorm(v []float64, w float64) []float64 {
	norm := vecNorm(v)
	out := make([]float64, len(v))
	for i, x := range v {
		if norm > 0 {
			out[i] = w * x / norm
		}
	}
	return out
}

func vecNorm(v []float64) float64 {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	return math.Sqrt(sum)
}

func minMaxSlice(v []float64) (float64, float64) {
	mn, mx := v[0], v[0]
	for _, x := range v[1:] {
		if x < mn {
			mn = x
		}
		if x > mx {
			mx = x
		}
	}
	return mn, mx
}

func sq(x float64) float64 { return x * x }

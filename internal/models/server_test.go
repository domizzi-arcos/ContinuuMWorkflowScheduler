package models

import "testing"

// TestExecMS pins the effective time model: round(duration/speed) + latency,
// with speed 0 (unset) treated as 1 so v1 and C3 behaviour is unchanged.
func TestExecMS(t *testing.T) {
	cases := []struct {
		speed float64
		want  int
	}{
		{0, 1010},   // unset
		{1, 1010},   // explicit 1
		{2, 510},    // 1000/2 + 10
		{0.5, 2010}, // 1000/0.5 + 10
		{3, 343},    // round(333.3) + 10
	}
	for _, c := range cases {
		s := &Server{Latency: 10, Speed: c.speed}
		if got := s.ExecMS(1000); got != c.want {
			t.Errorf("speed %v: ExecMS = %d, want %d", c.speed, got, c.want)
		}
	}
}

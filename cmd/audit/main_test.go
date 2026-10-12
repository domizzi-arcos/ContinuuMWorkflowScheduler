package main

import "testing"

func TestComboRe(t *testing.T) {
	tests := []struct{ id, topo, cap, seed string }{
		{"topo_uniform_m_cap_medium_seed1", "uniform_m", "medium", "1"},
		{"topo_edge_heavy_cap_small_seed12", "edge_heavy", "small", "12"},
		{"topo_cloud_med_cap_large", "cloud_med", "large", ""},
		{"topo_fog_heavy_cap_large_w_cost_seed3", "fog_heavy", "large", "3"},
	}
	for _, tc := range tests {
		m := comboRe.FindStringSubmatch(tc.id)
		if m == nil || m[1] != tc.topo || m[2] != tc.cap || m[3] != tc.seed {
			t.Errorf("%s: got %v", tc.id, m)
		}
	}
	if comboRe.MatchString("benchmark_20260101") {
		t.Error("matched a non-combo name")
	}
}

func TestRunRequiresConfigs(t *testing.T) {
	if err := run(nil); err == nil {
		t.Fatal("want error without -configs")
	}
}

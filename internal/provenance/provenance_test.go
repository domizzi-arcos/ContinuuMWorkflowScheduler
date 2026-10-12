package provenance

import (
	"runtime"
	"strings"
	"testing"
)

func TestHashString(t *testing.T) {
	// sha256("example-host")[:16]; scripts/grid_benchmark.py uses the same rule.
	if got, want := HashString("example-host"), "8f1f0466fc41bb15"; got != want {
		t.Fatalf("HashString = %q, want %q", got, want)
	}
	if HashString("a") == HashString("b") {
		t.Fatal("different inputs hashed equal")
	}
}

func TestCollect(t *testing.T) {
	args := []string{"-config", "x.yaml"}
	info := Collect(args)
	if info.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q", info.GoVersion)
	}
	if info.GitCommit == "" {
		t.Error("GitCommit empty; want a revision or \"unknown\"")
	}
	if len(info.HostHash) != 16 && info.HostHash != "unknown" {
		t.Errorf("HostHash = %q", info.HostHash)
	}
	args[0] = "mutated"
	if strings.Join(info.Args, " ") != "-config x.yaml" {
		t.Errorf("Args not copied: %v", info.Args)
	}
}

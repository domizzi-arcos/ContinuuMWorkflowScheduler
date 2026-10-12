// Package provenance records where a result came from: the source revision
// the binary was built from, the Go toolchain, a hashed host identifier and
// the command line. The host name itself is never stored.
package provenance

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"runtime"
	"runtime/debug"
)

// Info is embedded in every exported result's meta and in run manifests.
type Info struct {
	GitCommit string   `json:"git_commit"`
	GitDirty  bool     `json:"git_dirty"`
	GoVersion string   `json:"go_version"`
	HostHash  string   `json:"host_hash"`
	Args      []string `json:"args"`
}

// Collect builds an Info for the current process. The commit and dirty flag
// come from the VCS stamp that `go build` embeds when run inside a git
// checkout; they read "unknown"/false when the stamp is absent (for example
// under `go run` or with -buildvcs=false).
func Collect(args []string) *Info {
	info := &Info{
		GitCommit: "unknown",
		GoVersion: runtime.Version(),
		HostHash:  HashHost(),
		Args:      append([]string(nil), args...),
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				info.GitCommit = s.Value
			case "vcs.modified":
				info.GitDirty = s.Value == "true"
			}
		}
	}
	return info
}

// HashHost returns the first 16 hex characters of SHA-256 over the host
// name, so runs from the same machine can be grouped without naming it.
func HashHost() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return HashString(h)
}

// HashString is the hashing rule used by HashHost, exposed so the Python
// orchestrator's equivalent can be checked against it.
func HashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:16]
}

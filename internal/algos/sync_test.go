package algos

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// pyList extracts the quoted names of the first list literal after
// "<name> = " (optionally "<name> = OTHER + ") in a Python source.
func pyList(t *testing.T, src, name string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + name + ` = (?:[A-Z_]+ \+ )?\[([^\]]*)\]`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("list %s not found in scripts/algorithms.py", name)
	}
	return regexp.MustCompile(`"([a-z_0-9]+)"`).FindAllString(m[1], -1)
}

// pyDictKeys extracts the quoted keys of the dict literal "<name> = {...}".
func pyDictKeys(t *testing.T, src, name string) map[string]bool {
	t.Helper()
	re := regexp.MustCompile(`(?ms)^` + name + ` = \{(.*?)^\}`)
	m := re.FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("dict %s not found in scripts/algorithms.py", name)
	}
	keys := map[string]bool{}
	for _, k := range regexp.MustCompile(`(?m)^\s+"([a-z_0-9]+)":`).FindAllStringSubmatch(m[1], -1) {
		keys[k[1]] = true
	}
	return keys
}

func unquote(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = strings.Trim(v, `"`)
	}
	return out
}

// TestPythonListsMatch fails when scripts/algorithms.py and this package
// disagree on the policy lists or their order, or when a registered policy
// lacks a label, colour, marker or abbreviation.
func TestPythonListsMatch(t *testing.T) {
	data, err := os.ReadFile("../../scripts/algorithms.py")
	if err != nil {
		t.Fatalf("read scripts/algorithms.py: %v", err)
	}
	src := string(data)
	legacyPy := unquote(pyList(t, src, "LEGACY"))
	namesPy := append(append([]string(nil), legacyPy...), unquote(pyList(t, src, "NAMES"))...)
	optionalPy := unquote(pyList(t, src, "OPTIONAL"))

	if !reflect.DeepEqual(legacyPy, Legacy()) {
		t.Errorf("LEGACY = %v, Go Legacy() = %v", legacyPy, Legacy())
	}
	if !reflect.DeepEqual(namesPy, Names()) {
		t.Errorf("NAMES = %v, Go Names() = %v", namesPy, Names())
	}
	if !reflect.DeepEqual(optionalPy, Optional()) {
		t.Errorf("OPTIONAL = %v, Go Optional() = %v", optionalPy, Optional())
	}
	for _, dict := range []string{"LABELS", "COLORS", "MARKERS", "ABBREV"} {
		keys := pyDictKeys(t, src, dict)
		for _, n := range All() {
			if !keys[n] {
				t.Errorf("%s has no entry for %q", dict, n)
			}
		}
		if len(keys) != len(All()) {
			t.Errorf("%s has %d entries, want %d", dict, len(keys), len(All()))
		}
	}
}

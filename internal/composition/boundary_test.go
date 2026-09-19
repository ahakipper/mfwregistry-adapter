package composition

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestActiveGraphDoesNotImportLegacyGlobals runs the repository-wide checker
// rather than duplicating a partial package allowlist in this test. The test
// keeps the repository-wide checker honest after the legacy bridge is removed:
// all production and test code must use explicit dependencies.
func TestActiveGraphDoesNotImportLegacyGlobals(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	cmd := exec.Command("bash", filepath.Join(root, "scripts", "check_no_legacy_globals.sh"))
	cmd.Dir = root
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Run(); err != nil {
		t.Fatalf("legacy-global checker failed: %v\n%s", err, output.String())
	}
	if !strings.Contains(output.String(), "legacy-global-boundary: PASS") {
		t.Fatalf("legacy-global checker omitted PASS marker: %s", output.String())
	}
}

// TestProductionCompositionHasNoLegacyGlobalImports guards the C2 boundary:
// the server/composition path must use injected ports and the resolved config.
func TestProductionCompositionHasNoLegacyGlobalImports(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	for _, rel := range []string{"internal/server.go", "internal/composition/root.go", "internal/infra/notice/notice.go", "pkg/providers/k8s/k8s.go", "pkg/providers/k8s/conversion.go", "pkg/providers/consul/consul.go", "pkg/providers/consul/monitor.go", "pkg/worker/elector.go", "pkg/distribute/election/election.go", "pkg/metrics/proserver.go"} {
		path := filepath.Join(root, rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		text := string(data)
		for _, forbidden := range []string{`"spotter/pkg/log"`, `"spotter/pkg/notice"`, `"spotter/config"`, `"spotter/internal/infra/legacycompat"`, `"spotter/pkg/providers/aggregate"`} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s imports legacy production dependency %s", rel, forbidden)
			}
		}
	}
}

func TestLegacyAggregateIsDeleted(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "pkg", "providers", "aggregate", "controller.go"))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("legacy aggregate controller still exists or cannot be checked: %v", err)
	}
}

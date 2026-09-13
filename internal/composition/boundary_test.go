package composition

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestProductionCompositionHasNoLegacyGlobalImports guards the C2 boundary:
// the server/composition path must use injected ports. Legacy globals remain
// available only through explicitly named compatibility wrappers in cmd or
// provider constructors, never through the production composition root.
func TestProductionCompositionHasNoLegacyGlobalImports(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	for _, rel := range []string{"internal/server.go", "internal/composition/root.go", "internal/infra/notice/notice.go", "pkg/providers/k8s/k8s.go", "pkg/providers/k8s/conversion.go", "pkg/providers/consul/consul.go", "pkg/providers/consul/monitor.go", "pkg/worker/elector.go", "pkg/metrics/proserver.go"} {
		path := filepath.Join(root, rel)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		text := string(data)
		for _, forbidden := range []string{`"spotter/pkg/log"`, `"spotter/pkg/notice"`, `"spotter/config"`, `"spotter/pkg/providers/aggregate"`} {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s imports legacy production dependency %s", rel, forbidden)
			}
		}
	}
}

func TestLegacyAggregateIsExcludedFromNormalBuild(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "pkg", "providers", "aggregate", "controller.go"))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read aggregate controller: %v", err)
	}
	if !strings.HasPrefix(string(data), "//go:build legacyaggregate") {
		t.Fatalf("aggregate controller lacks legacyaggregate build gate")
	}
}

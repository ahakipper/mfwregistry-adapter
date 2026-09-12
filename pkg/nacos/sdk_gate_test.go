package nacos

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNacosHTTPUsageIsCentralized verifies the B3 static boundary: business
// code may not grow new net/http call sites. The only allow-listed source is
// client.go, which contains the versioned Admin/Catalog/readiness compatibility
// adapter and its rollback transport; naming operations are routed through
// sdk.go's official SDK facade when TransportSDK is selected.
func TestNacosHTTPUsageIsCentralized(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read pkg/nacos: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(".", entry.Name())
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(contents), "\"net/http\"") && entry.Name() != "client.go" {
			t.Fatalf("%s imports net/http outside the allow-listed compatibility adapter client.go", entry.Name())
		}
	}
}

func TestHTTPCompatibilityExceptionsAreOwnedAndExpiring(t *testing.T) {
	if len(HTTPCompatibilityExceptions) == 0 {
		t.Fatal("HTTP compatibility exception registry is empty")
	}
	for _, item := range HTTPCompatibilityExceptions {
		if item.Operation == "" || item.Owner == "" || item.ExpiresOn == "" || item.RemovalCriteria == "" {
			t.Fatalf("incomplete HTTP compatibility exception: %+v", item)
		}
	}
}

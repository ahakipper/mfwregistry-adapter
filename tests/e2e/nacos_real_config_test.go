//go:build nacos_real || nacos_sdk_eval
// +build nacos_real nacos_sdk_eval

package e2e

import (
	"strings"
	"testing"
)

func clearNacosRealEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"NACOS_SERVER", "NACOS_NAMESPACE", "NACOS_GROUP", "NACOS_USERNAME", "NACOS_PASSWORD", "NACOS_ACCESS_TOKEN",
		"NACOS_CA_FILE", "NACOS_SERVER_NAME", "NACOS_TLS", "NACOS_INSECURE_SKIP_VERIFY", "NACOS_ALLOW_INSECURE_AUTH",
		"NACOS_REAL_SCRATCH", "NACOS_REAL_ALLOW_WRITE", "NACOS_REAL_TIMEOUT",
	} {
		t.Setenv(key, "")
	}
}

func TestParseNacosRealConfigSecurityGuards(t *testing.T) {
	clearNacosRealEnv(t)
	if _, err := parseNacosRealConfig(); err == nil {
		t.Fatal("missing NACOS_SERVER returned nil error")
	}
	t.Setenv("NACOS_SERVER", "127.0.0.1:8848")
	t.Setenv("NACOS_USERNAME", "user")
	if _, err := parseNacosRealConfig(); err == nil || !strings.Contains(err.Error(), "supplied together") {
		t.Fatalf("partial credentials error = %v", err)
	}
	t.Setenv("NACOS_PASSWORD", "pass")
	if _, err := parseNacosRealConfig(); err == nil || !strings.Contains(err.Error(), "plaintext") {
		t.Fatalf("plaintext credentials error = %v", err)
	}
	t.Setenv("NACOS_ALLOW_INSECURE_AUTH", "1")
	t.Setenv("NACOS_REAL_SCRATCH", "1")
	if _, err := parseNacosRealConfig(); err == nil || !strings.Contains(err.Error(), "scratch+write") {
		t.Fatalf("missing write guard error = %v", err)
	}
	t.Setenv("NACOS_REAL_ALLOW_WRITE", "1")
	cfg, err := parseNacosRealConfig()
	if err != nil || !cfg.canWrite {
		t.Fatalf("guarded plaintext config = %+v, err=%v", cfg, err)
	}
}

func TestParseNacosRealConfigTLSAndSDKAuth(t *testing.T) {
	clearNacosRealEnv(t)
	t.Setenv("NACOS_SERVER", "nacos-a:8848,nacos-b:8848")
	t.Setenv("NACOS_CA_FILE", "/tmp/ca.pem")
	cfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("TLS config error = %v", err)
	}
	if !cfg.tls || !strings.HasPrefix(cfg.client.ServerURLs[0], "https://") {
		t.Fatalf("TLS config = %+v, want inferred https", cfg)
	}
	t.Setenv("NACOS_ACCESS_TOKEN", "secret")
	if _, err := parseNacosRealConfig(); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("access-token config error = %v", err)
	}
	t.Setenv("NACOS_ACCESS_TOKEN", "")
	t.Setenv("NACOS_SERVER", "http://nacos-a:8848,https://nacos-b:8848")
	if _, err := parseNacosRealConfig(); err == nil || !strings.Contains(err.Error(), "mixed") {
		t.Fatalf("mixed URL schemes error = %v", err)
	}
}

func TestRedactNacosEndpointsNeverIncludesQueryOrCredentials(t *testing.T) {
	got := redactNacosEndpoints([]string{"https://user:pass@nacos-a:8848/nacos?accessToken=secret"})
	if strings.Contains(got, "pass") || strings.Contains(got, "secret") || strings.Contains(got, "accessToken") {
		t.Fatalf("redacted endpoint leaked secret: %q", got)
	}
	if got != "https://nacos-a:8848" {
		t.Fatalf("redacted endpoint = %q", got)
	}
}

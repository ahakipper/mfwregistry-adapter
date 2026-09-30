//go:build consul_real
// +build consul_real

package e2e

import (
	"strings"
	"testing"
	"time"
)

func clearConsulRealEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"CONSUL_SERVER", "CONSUL_TOKEN", "CONSUL_TOKEN_FILE", "CONSUL_CA_FILE", "CONSUL_CERT_FILE", "CONSUL_KEY_FILE",
		"CONSUL_SERVER_NAME", "CONSUL_TLS", "CONSUL_INSECURE_SKIP_VERIFY", "CONSUL_ALLOW_INSECURE_AUTH",
		"CONSUL_DATACENTER", "CONSUL_NAMESPACE", "CONSUL_SOURCE_ID", "CONSUL_REAL_SOURCE_ID",
		"CONSUL_REAL_SCRATCH", "CONSUL_REAL_ALLOW_WRITE", "CONSUL_REAL_TIMEOUT", "CONSUL_REAL_SAMPLES",
	} {
		t.Setenv(key, "")
	}
}

func TestParseConsulRealConfigGuardsAndRedaction(t *testing.T) {
	clearConsulRealEnv(t)
	if _, err := parseConsulRealConfig(); err == nil {
		t.Fatal("missing CONSUL_SERVER returned nil error")
	}
	t.Setenv("CONSUL_SERVER", "http://user:secret@consul-a:8500/path?token=leak")
	if _, err := parseConsulRealConfig(); err == nil || !strings.Contains(err.Error(), "credentials") {
		t.Fatalf("unsafe endpoint error = %v", err)
	}
	t.Setenv("CONSUL_SERVER", "consul-a:8500")
	t.Setenv("CONSUL_TOKEN", "secret")
	if _, err := parseConsulRealConfig(); err == nil || !strings.Contains(err.Error(), "scratch+write") {
		t.Fatalf("plaintext token error = %v", err)
	}
	t.Setenv("CONSUL_ALLOW_INSECURE_AUTH", "1")
	t.Setenv("CONSUL_REAL_SCRATCH", "1")
	if _, err := parseConsulRealConfig(); err == nil || !strings.Contains(err.Error(), "scratch+write") {
		t.Fatalf("missing write guard error = %v", err)
	}
	t.Setenv("CONSUL_REAL_ALLOW_WRITE", "1")
	t.Setenv("CONSUL_REAL_SAMPLES", "3")
	t.Setenv("CONSUL_SOURCE_ID", "scratch-source")
	cfg, err := parseConsulRealConfig()
	if err != nil {
		t.Fatalf("guarded config error = %v", err)
	}
	if !cfg.canWrite || cfg.samples != 3 || cfg.source.ID != "scratch-source" {
		t.Fatalf("guarded config = %+v", cfg)
	}
	if strings.Contains(cfg.endpoint, "secret") || cfg.endpoint != "http://consul-a:8500" {
		t.Fatalf("endpoint redaction = %q", cfg.endpoint)
	}
}

func TestParseConsulRealConfigTLSAndAuthFields(t *testing.T) {
	clearConsulRealEnv(t)
	t.Setenv("CONSUL_SERVER", "consul-a:8501,https://consul-b:8501")
	if _, err := parseConsulRealConfig(); err == nil || !strings.Contains(err.Error(), "mixed") {
		t.Fatalf("mixed scheme error = %v", err)
	}
	clearConsulRealEnv(t)
	t.Setenv("CONSUL_SERVER", "consul-a:8501")
	t.Setenv("CONSUL_CA_FILE", "/tmp/ca.pem")
	t.Setenv("CONSUL_TOKEN_FILE", "/tmp/token")
	t.Setenv("CONSUL_DATACENTER", "dc1")
	t.Setenv("CONSUL_NAMESPACE", "ns1")
	cfg, err := parseConsulRealConfig()
	if err != nil {
		t.Fatalf("TLS config error = %v", err)
	}
	if !cfg.tls || cfg.source.Addresses[0] != "https://consul-a:8501" || cfg.source.TokenFile != "/tmp/token" || cfg.source.Datacenter != "dc1" || cfg.source.Namespace != "ns1" {
		t.Fatalf("TLS/source config = %+v", cfg)
	}
}

func TestPercentileNearestRankAndNoMutation(t *testing.T) {
	samples := []time.Duration{50 * time.Millisecond, 10 * time.Millisecond, 90 * time.Millisecond, 30 * time.Millisecond}
	if got := percentile(samples, 0.50); got != 30*time.Millisecond {
		t.Fatalf("p50 = %s, want 30ms", got)
	}
	if got := percentile(samples, 0.90); got != 90*time.Millisecond {
		t.Fatalf("p90 = %s, want 90ms", got)
	}
	if got := percentile(samples, 0.99); got != 90*time.Millisecond {
		t.Fatalf("p99 = %s, want 90ms", got)
	}
	if samples[0] != 50*time.Millisecond {
		t.Fatalf("percentile mutated operation order: %v", samples)
	}
	if got := percentile(nil, 0.5); got != 0 {
		t.Fatalf("empty percentile = %s, want 0", got)
	}
}

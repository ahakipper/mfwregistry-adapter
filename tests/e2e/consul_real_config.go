//go:build consul_real
// +build consul_real

package e2e

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"spotter/pkg/providers/consul"
)

// consulRealConfig is the explicitly opt-in contract for the real Consul to
// Nacos qualification. Credentials are retained only in the source options;
// endpoint is the only value intended for logs and evidence.
type consulRealConfig struct {
	source    consul.ConsulSource
	addresses []string
	endpoint  string
	tls       bool
	canWrite  bool
	timeout   time.Duration
	samples   int
}

func parseConsulRealConfig() (consulRealConfig, error) {
	var cfg consulRealConfig
	raw := strings.TrimSpace(os.Getenv("CONSUL_SERVER"))
	if raw == "" {
		return cfg, errors.New("CONSUL_SERVER is required")
	}

	ca := strings.TrimSpace(os.Getenv("CONSUL_CA_FILE"))
	cert := strings.TrimSpace(os.Getenv("CONSUL_CERT_FILE"))
	key := strings.TrimSpace(os.Getenv("CONSUL_KEY_FILE"))
	serverName := strings.TrimSpace(os.Getenv("CONSUL_SERVER_NAME"))
	insecure := os.Getenv("CONSUL_INSECURE_SKIP_VERIFY") == "1"
	cfg.tls = os.Getenv("CONSUL_TLS") == "1" || ca != "" || cert != "" || key != "" || serverName != "" || insecure

	var scheme string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if !strings.Contains(item, "://") {
			if cfg.tls {
				item = "https://" + item
			} else {
				item = "http://" + item
			}
		}
		u, err := url.Parse(item)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return cfg, errors.New("invalid CONSUL_SERVER address: malformed URL or unsupported scheme")
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return cfg, errors.New("CONSUL_SERVER address must not contain credentials, path, or query parameters")
		}
		if u.Scheme == "https" {
			cfg.tls = true
		}
		if scheme == "" {
			scheme = u.Scheme
		} else if scheme != u.Scheme {
			return cfg, fmt.Errorf("mixed CONSUL_SERVER URL schemes %q and %q are not supported", scheme, u.Scheme)
		}
		cfg.addresses = append(cfg.addresses, u.Scheme+"://"+u.Host)
	}
	if len(cfg.addresses) == 0 {
		return cfg, errors.New("CONSUL_SERVER contains no addresses")
	}
	if insecure && os.Getenv("CONSUL_REAL_SCRATCH") != "1" {
		return cfg, errors.New("CONSUL_INSECURE_SKIP_VERIFY requires CONSUL_REAL_SCRATCH=1")
	}

	token := strings.TrimSpace(os.Getenv("CONSUL_TOKEN"))
	tokenFile := strings.TrimSpace(os.Getenv("CONSUL_TOKEN_FILE"))
	if token != "" && tokenFile != "" {
		return cfg, errors.New("CONSUL_TOKEN and CONSUL_TOKEN_FILE must not both be supplied")
	}
	if token != "" || tokenFile != "" {
		if !cfg.tls && !consulInsecureAuthScratchGuards() {
			return cfg, errors.New("CONSUL_TOKEN/TOKEN_FILE over plaintext requires scratch+write+explicit insecure-auth guards")
		}
	}

	timeout := 20 * time.Second
	if rawTimeout := strings.TrimSpace(os.Getenv("CONSUL_REAL_TIMEOUT")); rawTimeout != "" {
		parsed, err := time.ParseDuration(rawTimeout)
		if err != nil || parsed <= 0 {
			return cfg, fmt.Errorf("invalid CONSUL_REAL_TIMEOUT %q", rawTimeout)
		}
		timeout = parsed
	}
	samples := 2
	if rawSamples := strings.TrimSpace(os.Getenv("CONSUL_REAL_SAMPLES")); rawSamples != "" {
		parsed, err := strconv.Atoi(rawSamples)
		if err != nil || parsed <= 0 || parsed > 100 {
			return cfg, fmt.Errorf("invalid CONSUL_REAL_SAMPLES %q (want 1..100)", rawSamples)
		}
		samples = parsed
	}

	sourceID := strings.TrimSpace(os.Getenv("CONSUL_SOURCE_ID"))
	if sourceID == "" {
		sourceID = strings.TrimSpace(os.Getenv("CONSUL_REAL_SOURCE_ID"))
	}
	cfg.source = consul.ConsulSource{
		ID:                    sourceID,
		Addresses:             append([]string(nil), cfg.addresses...),
		Token:                 token,
		TokenFile:             tokenFile,
		TLSCAFile:             ca,
		TLSCertFile:           cert,
		TLSKeyFile:            key,
		TLSServerName:         serverName,
		TLSInsecureSkipVerify: insecure,
		Datacenter:            strings.TrimSpace(os.Getenv("CONSUL_DATACENTER")),
		Namespace:             strings.TrimSpace(os.Getenv("CONSUL_NAMESPACE")),
	}
	cfg.endpoint = redactConsulEndpoints(cfg.addresses)
	cfg.canWrite = os.Getenv("CONSUL_REAL_SCRATCH") == "1" && os.Getenv("CONSUL_REAL_ALLOW_WRITE") == "1"
	cfg.timeout = timeout
	cfg.samples = samples
	return cfg, nil
}

func consulInsecureAuthScratchGuards() bool {
	return os.Getenv("CONSUL_ALLOW_INSECURE_AUTH") == "1" &&
		os.Getenv("CONSUL_REAL_SCRATCH") == "1" &&
		os.Getenv("CONSUL_REAL_ALLOW_WRITE") == "1"
}

// redactConsulEndpoints removes credentials, paths, and query material while
// retaining the scheme and host needed to identify the configured target.
func redactConsulEndpoints(addresses []string) string {
	redacted := make([]string, 0, len(addresses))
	for _, address := range addresses {
		u, err := url.Parse(address)
		if err != nil || u.Host == "" {
			redacted = append(redacted, "<invalid>")
			continue
		}
		redacted = append(redacted, u.Scheme+"://"+u.Host)
	}
	return strings.Join(redacted, ",")
}

// percentile returns a nearest-rank percentile over duration samples. A
// copy is sorted so callers can retain their operation-order samples.
func percentile(samples []time.Duration, q float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if math.IsNaN(q) || q <= 0 {
		q = 0
	} else if q >= 1 {
		q = 1
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

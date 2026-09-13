//go:build nacos_real || nacos_sdk_eval
// +build nacos_real nacos_sdk_eval

package e2e

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"spotter/pkg/nacos"
)

// nacosRealConfig is the explicitly opt-in real scratch contract. Secrets are
// retained only in the SDK config and are never included in evidence logs.
type nacosRealConfig struct {
	client    nacos.ClientConfig
	addresses []string
	endpoint  string
	tls       bool
	canWrite  bool
	timeout   time.Duration
}

func parseNacosRealConfig() (nacosRealConfig, error) {
	var cfg nacosRealConfig
	raw := strings.TrimSpace(os.Getenv("NACOS_SERVER"))
	if raw == "" {
		return cfg, errors.New("NACOS_SERVER is required")
	}
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			cfg.addresses = append(cfg.addresses, item)
		}
	}
	if len(cfg.addresses) == 0 {
		return cfg, errors.New("NACOS_SERVER contains no addresses")
	}

	ca := strings.TrimSpace(os.Getenv("NACOS_CA_FILE"))
	serverName := strings.TrimSpace(os.Getenv("NACOS_SERVER_NAME"))
	insecure := os.Getenv("NACOS_INSECURE_SKIP_VERIFY") == "1"
	cfg.tls = os.Getenv("NACOS_TLS") == "1" || ca != "" || serverName != "" || insecure
	var scheme string
	for i, address := range cfg.addresses {
		if !strings.Contains(address, "://") {
			if cfg.tls {
				address = "https://" + address
			} else {
				address = "http://" + address
			}
			cfg.addresses[i] = address
		}
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return cfg, fmt.Errorf("invalid NACOS_SERVER address %q", address)
		}
		if u.User != nil || len(u.RawQuery) != 0 {
			return cfg, fmt.Errorf("NACOS_SERVER address %q must not contain credentials or query parameters", address)
		}
		if u.Scheme == "https" {
			cfg.tls = true
		}
		if scheme == "" {
			scheme = u.Scheme
		} else if scheme != u.Scheme {
			return cfg, fmt.Errorf("mixed NACOS_SERVER URL schemes %q and %q are not supported", scheme, u.Scheme)
		}
	}
	if insecure && os.Getenv("NACOS_REAL_SCRATCH") != "1" {
		return cfg, errors.New("NACOS_INSECURE_SKIP_VERIFY requires NACOS_REAL_SCRATCH=1")
	}
	username := strings.TrimSpace(os.Getenv("NACOS_USERNAME"))
	password := os.Getenv("NACOS_PASSWORD")
	accessToken := strings.TrimSpace(os.Getenv("NACOS_ACCESS_TOKEN"))
	if accessToken != "" {
		return cfg, errors.New("NACOS_ACCESS_TOKEN is unsupported by the official SDK; use username/password")
	}
	if (username == "") != (password == "") {
		return cfg, errors.New("NACOS_USERNAME and NACOS_PASSWORD must be supplied together")
	}
	if username != "" && !cfg.tls && !insecureAuthScratchGuards() {
		return cfg, errors.New("NACOS_USERNAME/PASSWORD over plaintext requires scratch+write+explicit insecure-auth guards")
	}
	timeout := 10 * time.Second
	if rawTimeout := strings.TrimSpace(os.Getenv("NACOS_REAL_TIMEOUT")); rawTimeout != "" {
		parsed, err := time.ParseDuration(rawTimeout)
		if err != nil || parsed <= 0 {
			return cfg, fmt.Errorf("invalid NACOS_REAL_TIMEOUT %q", rawTimeout)
		}
		timeout = parsed
	}
	cfg.timeout = timeout
	cfg.endpoint = redactNacosEndpoints(cfg.addresses)
	cfg.canWrite = os.Getenv("NACOS_REAL_SCRATCH") == "1" && os.Getenv("NACOS_REAL_ALLOW_WRITE") == "1"
	cfg.client = nacos.ClientConfig{
		TransportMode:      nacos.TransportSDK,
		ServerURLs:         append([]string(nil), cfg.addresses...),
		NamespaceID:        strings.TrimSpace(os.Getenv("NACOS_NAMESPACE")),
		GroupName:          strings.TrimSpace(os.Getenv("NACOS_GROUP")),
		Username:           username,
		Password:           password,
		CAFile:             ca,
		ServerName:         serverName,
		InsecureSkipVerify: insecure,
		Timeout:            timeout,
	}
	return cfg, nil
}

func insecureAuthScratchGuards() bool {
	return os.Getenv("NACOS_ALLOW_INSECURE_AUTH") == "1" &&
		os.Getenv("NACOS_REAL_SCRATCH") == "1" &&
		os.Getenv("NACOS_REAL_ALLOW_WRITE") == "1"
}

// redactNacosEndpoints returns a stable endpoint summary with credentials and
// query material removed. It is safe for test evidence logs.
func redactNacosEndpoints(addresses []string) string {
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

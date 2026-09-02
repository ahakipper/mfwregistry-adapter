// Package config assembles the runtime configuration of the adapter.
//
// It combines the environment endpoint presets (exact copies of the values
// assigned by InitTest/InitDev/InitProd in config/config.go) with
// command-line flag values and default fallbacks into a single resolved
// Config value.
package config

import (
	"errors"
	"fmt"
	"strings"
)

// Endpoints holds an environment-specific endpoint preset.
//
// The values are exact copies of the globals assigned by InitTest,
// InitDev and InitProd in config/config.go. Each call to PresetFor returns
// a fresh value with freshly allocated slices, so callers may not corrupt
// the presets by mutating the result.
type Endpoints struct {
	// EtcdEndpoints lists the etcd cluster members.
	EtcdEndpoints []string
	// CertFile is the etcd certificate file path.
	CertFile string
	// KeyFile is the etcd key file path.
	KeyFile string
	// CAFile is the etcd CA file path.
	CAFile string
	// KubeConfigPath lists the K8s kubeconfig file paths to watch.
	KubeConfigPath []string
	// ConsulAddress lists the consul server addresses.
	ConsulAddress []string
	// LockCampaignKey is the etcd prefix key used for the leader campaign.
	LockCampaignKey string
	// Providers lists the providers enabled by the preset (may be empty).
	Providers []string
}

// testPreset mirrors InitTest in config/config.go.
func testPreset() Endpoints {
	return Endpoints{
		EtcdEndpoints: []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"},
		CertFile:      "./config/certs/etcdtest/etcd.pem",
		KeyFile:       "./config/certs/etcdtest/etcd-key.pem",
		CAFile:        "./config/certs/etcdtest/ca.pem",
		KubeConfigPath: []string{
			"./config/kubeconfigs/k8s-sailor",
		},
		ConsulAddress:   []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"},
		Providers:       []string{},
		LockCampaignKey: "/paas/spotter-test",
	}
}

// devPreset mirrors InitDev in config/config.go.
func devPreset() Endpoints {
	return Endpoints{
		EtcdEndpoints: []string{"172.18.12.181:2379", "172.18.12.182:2379", "172.18.12.183:2379"},
		CertFile:      "./config/certs/etcdtest/etcd.pem",
		KeyFile:       "./config/certs/etcdtest/etcd-key.pem",
		CAFile:        "./config/certs/etcdtest/ca.pem",
		// sailor: microservice dev environment; vipper: big-monolith dev environment
		KubeConfigPath: []string{
			"./config/kubeconfigs/k8s-sailor",
			"./config/kubeconfigs/k8s-vipper",
		},
		ConsulAddress:   []string{"10.72.73.172:8520", "10.72.73.173:8520", "10.72.73.174:8520"},
		Providers:       []string{},
		LockCampaignKey: "/paas/spotter",
	}
}

// prodPreset mirrors InitProd in config/config.go.
func prodPreset() Endpoints {
	return Endpoints{
		EtcdEndpoints: []string{"192.168.11.100:2479", "192.168.11.101:2479", "192.168.11.102:2479"},
		// this dir depend on Dockerfile
		CertFile: "./config/certs/etcdprod/etcd.pem",
		KeyFile:  "./config/certs/etcdprod/etcd-key.pem",
		CAFile:   "./config/certs/etcdprod/ca.pem",
		// deck: microservice legacy pre-release; eel: microservice Tengren datacenter; otter: microservice TKE; slug: microservice new pre-release
		KubeConfigPath: []string{
			"./config/kubeconfigs/k8s-eel",
			"./config/kubeconfigs/k8s-otter",
			"./config/kubeconfigs/k8s-slug",
			"./config/kubeconfigs/k8s-bernuda",
		},
		ConsulAddress:   []string{"10.132.2.40:8520", "10.132.2.42:8520", "10.132.2.43:8520"},
		Providers:       []string{},
		LockCampaignKey: "/paas/spotter",
	}
}

// PresetFor returns the endpoint preset for the given environment.
//
// Environment names are matched case-insensitively; accepted values are
// "test", "dev" and "product" (the same names accepted by cmd/adapter.go,
// where "product" selects InitProd). Any other value yields an
// "unsupported environment" error.
func PresetFor(env string) (Endpoints, error) {
	switch strings.ToLower(env) {
	case "test":
		return testPreset(), nil
	case "dev":
		return devPreset(), nil
	case "product":
		return prodPreset(), nil
	default:
		return Endpoints{}, fmt.Errorf("unsupported environment: %q", env)
	}
}

// Flags carries the raw command-line flag values.
//
// The zero value means "flag not set"; Load fills unset fields with
// defaults. The field set mirrors the flags registered by cmd/root.go and
// cmd/adapter.go.
type Flags struct {
	// LogFilePath is the log file path flag.
	LogFilePath string
	// LogSize is the max log size in MB.
	LogSize int
	// LogLevel is the log level flag (-1 debug, 0 info, 1 warning).
	LogLevel int
	// LogBackups is the number of log backups to keep.
	LogBackups int
	// LogAge is the max log age in days.
	LogAge int
	// LogEncoding is the log output format: "log" or "json".
	LogEncoding string
	// LogToStd reports whether logs are also written to standard output.
	LogToStd bool
	// PushAllInterval is the full push interval in seconds.
	PushAllInterval int
	// GrpcAddr is the Atlas gRPC address.
	GrpcAddr string
	// DisablePushWorker stops the real push action of the worker and only
	// prints push info. This configuration is for test use only.
	DisablePushWorker bool
	// Providers lists the providers to enable; it overrides the preset.
	Providers []string
	// PushAppCodes restricts pushes to these appcodes; empty means all.
	PushAppCodes []string
	// LeaderElection is the --leader-elect flag, kept as a tri-state
	// pointer rather than a plain bool so that Load can distinguish
	// "flag not set" from an explicit --leader-elect=false:
	//   - nil   -> flag not set, Config.EnableLeaderElection defaults to true
	//   - true  -> leader election explicitly enabled
	//   - false -> leader election explicitly disabled
	// The resolved value is exposed as Config.EnableLeaderElection.
	LeaderElection *bool
	// MetricsAddr is the Prometheus metrics address.
	MetricsAddr string
}

// Config is the fully resolved runtime configuration.
//
// It is immutable by convention: callers must treat the returned value and
// its slices as read-only and must not mutate them after Load returns.
type Config struct {
	Endpoints

	// Log settings.
	LogFilePath string
	LogSize     int
	LogLevel    int
	LogBackups  int
	LogAge      int
	LogEncoding string
	LogToStd    bool

	// Push settings.
	PushAllInterval int

	// GrpcAddr is the Atlas gRPC address.
	GrpcAddr string

	// DisablePushWorker stops the real push action (test use only).
	DisablePushWorker bool

	// Providers lists the enabled providers.
	Providers []string

	// PushAppCodes restricts pushes to these appcodes; empty means all.
	PushAppCodes []string

	// EnableLeaderElection reports whether leader election is enabled.
	EnableLeaderElection bool

	// MetricsAddr is the Prometheus metrics address.
	MetricsAddr string
}

// Default flag values applied by Load when a flag is not set (zero). They
// match the cobra flag defaults registered in cmd/root.go and
// cmd/adapter.go.
const (
	defaultLogFilePath          = "./logfiles/"
	defaultLogSize              = 100
	defaultLogLevel             = -1
	defaultLogBackups           = 10
	defaultLogAge               = 7
	defaultLogEncoding          = "json"
	defaultLogToStd             = true
	defaultPushAllInterval      = 21600
	defaultGrpcAddr             = "172.16.130.71:50051"
	defaultMetricsAddr          = ":8090"
	defaultEnableLeaderElection = true
)

// Load resolves the runtime configuration for env from the flag values.
//
// env is matched case-insensitively against "test", "dev" and "product".
// Flag fields left at their zero value fall back to the defaults above.
// The providers flag list overrides the preset provider list; both lists
// are trimmed of surrounding whitespace and blank entries are dropped.
// When both lists end up empty, Load fails with "no providers configured".
func Load(env string, flags Flags) (Config, error) {
	endpoints, err := PresetFor(env)
	if err != nil {
		return Config{}, err
	}

	// Providers: the flag list overrides the preset list.
	providers := cleanList(flags.Providers)
	if len(providers) == 0 {
		providers = cleanList(endpoints.Providers)
	}
	if len(providers) == 0 {
		return Config{}, errors.New("no providers configured")
	}

	cfg := Config{Endpoints: endpoints}

	// Log settings.
	cfg.LogFilePath = strOrDefault(flags.LogFilePath, defaultLogFilePath)
	cfg.LogSize = intOrDefault(flags.LogSize, defaultLogSize)
	cfg.LogLevel = intOrDefault(flags.LogLevel, defaultLogLevel)
	cfg.LogBackups = intOrDefault(flags.LogBackups, defaultLogBackups)
	cfg.LogAge = intOrDefault(flags.LogAge, defaultLogAge)
	cfg.LogEncoding = strOrDefault(flags.LogEncoding, defaultLogEncoding)
	// LogToStd is a plain bool, so the zero value (false) is treated as
	// "not set" and defaults to true, matching the legacy flag default.
	// An explicit false cannot be expressed through this field.
	if flags.LogToStd {
		cfg.LogToStd = true
	} else {
		cfg.LogToStd = defaultLogToStd
	}

	// Push settings.
	cfg.PushAllInterval = intOrDefault(flags.PushAllInterval, defaultPushAllInterval)

	// gRPC and worker settings.
	cfg.GrpcAddr = strOrDefault(flags.GrpcAddr, defaultGrpcAddr)
	cfg.DisablePushWorker = flags.DisablePushWorker

	// Providers and appcodes.
	cfg.Providers = providers
	cfg.PushAppCodes = cleanList(flags.PushAppCodes)

	// Leader election: the flag is tri-state, so an unset pointer falls
	// back to the legacy default of enabled; an explicit pointer value is
	// used verbatim.
	cfg.EnableLeaderElection = defaultEnableLeaderElection
	if flags.LeaderElection != nil {
		cfg.EnableLeaderElection = *flags.LeaderElection
	}

	// Metrics settings.
	cfg.MetricsAddr = strOrDefault(flags.MetricsAddr, defaultMetricsAddr)

	return cfg, nil
}

// cleanList trims surrounding whitespace from every entry and drops the
// empty ones. It never returns a nil slice.
func cleanList(items []string) []string {
	cleaned := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			cleaned = append(cleaned, item)
		}
	}
	return cleaned
}

// strOrDefault returns value when it is not empty, otherwise fallback.
func strOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// intOrDefault returns value when it is not zero, otherwise fallback.
func intOrDefault(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

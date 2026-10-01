package consul

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/internal/ports"
)

type ConsulClientFactory interface {
	ConsulClientFactory() (*api.Client, error)
}

// ConsulClientOptions contains the authentication, transport and tenancy
// settings applied to every client created by a ClientFactorySimple. The
// nested TLSConfig mirrors the Consul API type; the flattened TLS fields are
// provided for descriptor callers that do not otherwise depend on the Consul
// API package. When both forms set a value, the flattened field wins.
//
// Token and TokenFile are intentionally retained only in memory. They are not
// included in probe errors, logs, metrics, or other diagnostic strings.
type ConsulClientOptions struct {
	Token     string
	TokenFile string

	TLSConfig             api.TLSConfig
	TLSCAFile             string
	TLSCertFile           string
	TLSKeyFile            string
	TLSServerName         string
	TLSInsecureSkipVerify bool

	Datacenter string
	Namespace  string
}

// ClientFactoryOptions is retained as an alias for callers that used the
// generic factory terminology before ConsulClientOptions was named.
type ClientFactoryOptions = ConsulClientOptions

type ClientFactorySimple struct {
	addrs     []string
	clients   map[string]*api.Client
	logger    ports.Logger
	options   ConsulClientOptions
	mu        sync.RWMutex
	metricsMu sync.RWMutex
	metrics   ports.ConsulRequestMetricsRecorder
	source    string
}

func NewClientFactory(addrs []string, logger ports.Logger) (*ClientFactorySimple, error) {
	return NewClientFactoryWithOptions(addrs, ConsulClientOptions{}, logger)
}

// NewClientFactoryWithOptions constructs a client factory with the supplied
// Consul API options. A fresh api.Config is derived for each address so that
// each client has the same authentication, TLS and tenancy settings and no
// client can accidentally inherit a mutable config from another address.
func NewClientFactoryWithOptions(addrs []string, options ConsulClientOptions, logger ports.Logger) (*ClientFactorySimple, error) {
	usableAddrs := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		usableAddrs = append(usableAddrs, addr)
	}
	if len(usableAddrs) == 0 {
		return nil, errors.New("Consul client factory has no usable addresses")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}

	return &ClientFactorySimple{
		addrs:   usableAddrs,
		clients: make(map[string]*api.Client),
		logger:  logger,
		options: normalizeClientOptions(options),
	}, nil
}

// NewClientFactoryWithConfig is a descriptive compatibility spelling for
// callers that prefer to call the option bundle a config.
func NewClientFactoryWithConfig(addrs []string, options ConsulClientOptions, logger ports.Logger) (*ClientFactorySimple, error) {
	return NewClientFactoryWithOptions(addrs, options, logger)
}

// NeweClientFacotorySimple is deprecated. Use NewClientFactory instead.
func NeweClientFacotorySimple(addrs []string) (*ClientFactorySimple, error) {
	return NewClientFactory(addrs, nil)
}

// ConsulClientFactory returns the first healthy cached or configured Consul client.
func (cfs *ClientFactorySimple) ConsulClientFactory() (*api.Client, error) {
	attempted := make(map[string]struct{}, len(cfs.addrs))
	failures := make([]error, 0, len(cfs.addrs))

	for _, cached := range cfs.cachedClients() {
		attempted[cached.addr] = struct{}{}
		probeStarted := time.Now()
		leader, err := cached.client.Status().Leader()
		cfs.observeLeaderProbe(probeStarted, leader, err)
		if err == nil && leader != "" {
			return cached.client, nil
		}
		cfs.removeCachedClient(cached.addr, cached.client)
		if err != nil {
			err = fmt.Errorf("check cached Consul leader at %q: %w", cached.addr, err)
		} else {
			err = fmt.Errorf("cached Consul address %q returned an empty leader", cached.addr)
		}
		failures = append(failures, err)
		cfs.warnf("%v", err)
	}

	for _, addr := range cfs.addrs {
		if _, ok := attempted[addr]; ok {
			continue
		}
		attempted[addr] = struct{}{}

		config := cfs.apiConfig(addr)

		createStarted := time.Now()
		client, err := api.NewClient(config)
		if err != nil {
			cfs.observeRequest(createStarted, "client_create", "error")
			err = fmt.Errorf("create Consul client for %q: %w", addr, err)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}

		probeStarted := time.Now()
		leader, err := client.Status().Leader()
		cfs.observeLeaderProbe(probeStarted, leader, err)
		if err != nil {
			err = fmt.Errorf("check Consul leader at %q: %w", addr, err)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}
		if leader == "" {
			err = fmt.Errorf("Consul address %q returned an empty leader", addr)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}

		cfs.cacheClient(addr, client)
		return client, nil
	}

	if len(failures) == 0 {
		return nil, errors.New("no valid Consul client found")
	}
	return nil, &clientProbeAggregateError{failures: failures}
}

// SetConsulRequestMetricsRecorder installs the optional request-level
// recorder. It is intentionally additive to the existing ConsulMetricsRecorder
// seam so external catalog metric implementations remain source-compatible.
func (cfs *ClientFactorySimple) SetConsulRequestMetricsRecorder(recorder ports.ConsulRequestMetricsRecorder) {
	if cfs == nil {
		return
	}
	cfs.metricsMu.Lock()
	cfs.metrics = recorder
	cfs.metricsMu.Unlock()
}

// SetMetricSource installs the logical source scope used by request metrics.
// Only wire-safe source IDs are accepted; an endpoint or token-like value is
// collapsed to "unknown" and therefore cannot leak through a metric label.
func (cfs *ClientFactorySimple) SetMetricSource(source string) {
	if cfs == nil {
		return
	}
	cfs.metricsMu.Lock()
	cfs.source = normalizeConsulMetricSource(source)
	cfs.metricsMu.Unlock()
}

func (cfs *ClientFactorySimple) requestMetrics() (ports.ConsulRequestMetricsRecorder, string) {
	if cfs == nil {
		return nil, "unknown"
	}
	cfs.metricsMu.RLock()
	defer cfs.metricsMu.RUnlock()
	source := cfs.source
	if source == "" {
		source = "unknown"
	}
	return cfs.metrics, source
}

func (cfs *ClientFactorySimple) observeLeaderProbe(started time.Time, leader string, err error) {
	recorder, source := cfs.requestMetrics()
	if recorder == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = classifyConsulRequestError(err, nil)
	} else if strings.TrimSpace(leader) == "" {
		outcome = "empty_leader"
	}
	recorder.ObserveConsulLeaderProbeDuration(source, outcome, elapsedSince(started))
}

func (cfs *ClientFactorySimple) observeRequest(started time.Time, operation, outcome string) {
	recorder, source := cfs.requestMetrics()
	if recorder == nil {
		return
	}
	recorder.ObserveConsulRequestDuration(source, operation, outcome, elapsedSince(started))
}

func elapsedSince(started time.Time) time.Duration {
	duration := time.Since(started)
	if duration < 0 {
		return 0
	}
	return duration
}

func normalizeClientOptions(options ConsulClientOptions) ConsulClientOptions {
	// Keep the option value immutable from the caller's perspective. TLSConfig
	// currently contains slices only for PEM values, so copy those slices when
	// retaining the factory option.
	if options.TLSConfig.CAPem != nil {
		options.TLSConfig.CAPem = append([]byte(nil), options.TLSConfig.CAPem...)
	}
	if options.TLSConfig.CertPEM != nil {
		options.TLSConfig.CertPEM = append([]byte(nil), options.TLSConfig.CertPEM...)
	}
	if options.TLSConfig.KeyPEM != nil {
		options.TLSConfig.KeyPEM = append([]byte(nil), options.TLSConfig.KeyPEM...)
	}
	return options
}

// apiConfig builds a fresh Consul API config for one endpoint. It deliberately
// does not log or otherwise expose any credential fields.
func (cfs *ClientFactorySimple) apiConfig(addr string) *api.Config {
	config := api.DefaultConfig()
	config.Address = addr
	config.Token = cfs.options.Token
	config.TokenFile = cfs.options.TokenFile
	config.Datacenter = cfs.options.Datacenter
	config.Namespace = cfs.options.Namespace
	config.TLSConfig = cfs.options.TLSConfig
	if cfs.options.TLSCAFile != "" {
		config.TLSConfig.CAFile = cfs.options.TLSCAFile
	}
	if cfs.options.TLSCertFile != "" {
		config.TLSConfig.CertFile = cfs.options.TLSCertFile
	}
	if cfs.options.TLSKeyFile != "" {
		config.TLSConfig.KeyFile = cfs.options.TLSKeyFile
	}
	if cfs.options.TLSServerName != "" {
		config.TLSConfig.Address = cfs.options.TLSServerName
	}
	if cfs.options.TLSInsecureSkipVerify {
		config.TLSConfig.InsecureSkipVerify = true
	}
	return config
}

type clientProbeAggregateError struct {
	failures []error
}

func (err *clientProbeAggregateError) Error() string {
	messages := make([]string, len(err.failures))
	for i, failure := range err.failures {
		messages[i] = failure.Error()
	}
	return fmt.Sprintf("no valid Consul client found: %s", strings.Join(messages, "; "))
}

func (err *clientProbeAggregateError) Unwrap() error {
	if len(err.failures) == 0 {
		return nil
	}
	return err.failures[0]
}

type cachedClient struct {
	addr   string
	client *api.Client
}

func (cfs *ClientFactorySimple) cachedClients() []cachedClient {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()

	clients := make([]cachedClient, 0, len(cfs.clients))
	for _, addr := range cfs.addrs {
		if client := cfs.clients[addr]; client != nil {
			clients = append(clients, cachedClient{addr: addr, client: client})
		}
	}
	return clients
}

func (cfs *ClientFactorySimple) removeCachedClient(addr string, client *api.Client) {
	cfs.mu.Lock()
	if cfs.clients[addr] == client {
		delete(cfs.clients, addr)
	}
	cfs.mu.Unlock()
}

func (cfs *ClientFactorySimple) cacheClient(addr string, client *api.Client) {
	cfs.mu.Lock()
	cfs.clients[addr] = client
	cfs.mu.Unlock()
}

func (cfs *ClientFactorySimple) warnf(format string, args ...interface{}) {
	if cfs.logger != nil {
		cfs.logger.Warnf(format, args...)
	}
}

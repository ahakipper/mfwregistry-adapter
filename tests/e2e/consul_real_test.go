//go:build consul_real
// +build consul_real

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/pkg/nacos"
	"spotter/pkg/providers/consul"
	"spotter/pkg/worker"
)

type consulRealLatencySummary struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P90MS   float64 `json:"p90_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
}

type consulRealQualificationReport struct {
	Endpoint         string                   `json:"endpoint"`
	SourceID         string                   `json:"source_id"`
	ObservedNacosIPs []string                 `json:"observed_nacos_ips,omitempty"`
	LatencyBoundary  string                   `json:"latency_boundary"`
	PollIntervalMS   int                      `json:"oracle_poll_interval_ms"`
	ConfiguredRuns   int                      `json:"configured_runs"`
	Create           consulRealLatencySummary `json:"create"`
	Update           consulRealLatencySummary `json:"update"`
	Delete           consulRealLatencySummary `json:"delete"`
	Recovery         consulRealLatencySummary `json:"recovery"`
	HealthDown       consulRealLatencySummary `json:"health_down"`
	HealthRecovery   consulRealLatencySummary `json:"health_recovery"`
	CleanupStatus    string                   `json:"cleanup_status"`
	ResidualUnknown  bool                     `json:"residual_unknown"`
}

// TestConsulRealToNacos3Qualification is a guarded, opt-in qualification of
// the real Agent -> provider monitor -> DefaultWorker -> official Nacos SDK
// path. It deliberately never fabricates worker events: Consul mutations are
// the only source of provider changes. Missing configuration or write guards
// is a NOT VERIFIED skip, never a passing result.
func TestConsulRealToNacos3Qualification(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CONSUL_SERVER")) == "" {
		t.Skip("NOT VERIFIED: CONSUL_SERVER is not configured")
	}
	consulCfg, err := parseConsulRealConfig()
	if err != nil {
		t.Fatalf("invalid Consul real config: %v", err)
	}
	if !consulCfg.canWrite {
		t.Skip("NOT VERIFIED: set CONSUL_REAL_SCRATCH=1 and CONSUL_REAL_ALLOW_WRITE=1 for an explicit scratch target")
	}
	if strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: NACOS_SERVER is not configured")
	}
	nacosCfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("invalid Nacos real config: %v", err)
	}
	if !nacosCfg.canWrite {
		t.Skip("NOT VERIFIED: set NACOS_REAL_SCRATCH=1 and NACOS_REAL_ALLOW_WRITE=1 for an explicit scratch target")
	}
	if os.Getenv("NACOS_REAL_ALLOW_ADMIN") != "1" {
		t.Skip("NOT VERIFIED: set NACOS_REAL_ALLOW_ADMIN=1 to change the Nacos naming health switch on the scratch target")
	}
	// The business path remains official SDK-only, but the local scratch gate
	// must prove Nacos persistent health probing is disabled before any fake
	// 127.0.0.1 service is registered. Use the same explicit Admin compatibility
	// preflight as the Observe harness and fail closed on readback mismatch.
	adminConfig := nacosCfg.client
	adminConfig.TransportMode = nacos.TransportHTTPCompat
	admin, err := nacos.NewClientWithConfig(adminConfig, nil)
	if err != nil {
		t.Fatalf("create Nacos health-policy preflight client: %v", err)
	}
	initialHealthEnabled, err := admin.GetNamingHealthCheckEnabledV3AdminCompat()
	if err != nil {
		_ = admin.Close()
		t.Fatalf("read original Nacos naming health checks: %v", err)
	}
	defer func() {
		if err := admin.SetNamingHealthCheckEnabledV3AdminCompat(initialHealthEnabled); err != nil {
			t.Errorf("restore original Nacos naming health switch: %v", err)
		} else if restored, err := admin.GetNamingHealthCheckEnabledV3AdminCompat(); err != nil || restored != initialHealthEnabled {
			t.Errorf("restore Nacos naming health switch readback enabled=%t want=%t err=%v", restored, initialHealthEnabled, err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close Nacos health-policy preflight client: %v", err)
		}
	}()
	if err := admin.SetNamingHealthCheckEnabledV3AdminCompat(false); err != nil {
		t.Fatalf("disable Nacos naming health checks: %v", err)
	}
	enabled, err := admin.GetNamingHealthCheckEnabledV3AdminCompat()
	if err != nil {
		t.Fatalf("read back Nacos naming health checks: %v", err)
	}
	if enabled {
		t.Fatal("Nacos naming healthCheckEnabled remained true; refusing to start real gate")
	}

	normalizedSource, err := consulCfg.source.Normalized()
	if err != nil {
		t.Fatalf("normalize Consul source: %v", err)
	}
	consulClient, err := newConsulRealClient(consulCfg)
	if err != nil {
		t.Fatalf("create Consul client: %v", err)
	}
	if err := nacos.CheckReadinessWithConfig(nacosCfg.client, nil); err != nil {
		t.Fatalf("Nacos SDK readiness: %v", err)
	}
	sink, err := nacos.NewSinkWithConfig(nacosCfg.client, nil)
	if err != nil {
		t.Fatalf("create Nacos SDK sink: %v", err)
	}
	verifyClient, err := nacos.NewClientWithConfig(nacosCfg.client, nil)
	if err != nil {
		_ = sink.Close()
		t.Fatalf("create Nacos verifier: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	workerInstance, err := worker.NewResourceWorker(ctx, sink, nil, nil)
	if err != nil {
		cancel()
		_ = verifyClient.Close()
		_ = sink.Close()
		t.Fatalf("create DefaultWorker: %v", err)
	}
	provider, err := consul.NewConsulProviderWithSource(ctx, workerInstance, 1, normalizedSource, nil, nil)
	if err != nil {
		cancel()
		_ = verifyClient.Close()
		_ = sink.Close()
		t.Fatalf("create Consul provider: %v", err)
	}
	providerDone := make(chan error, 1)
	go func() { providerDone <- provider.Run() }()

	report := consulRealQualificationReport{
		Endpoint:        redactConsulEndpoints(consulCfg.addresses) + " -> " + nacosCfg.endpoint,
		SourceID:        normalizedSource.ID,
		LatencyBoundary: "consul_agent_mutation_start_to_nacos_catalog_observed",
		PollIntervalMS:  100,
		ConfiguredRuns:  consulCfg.samples,
		CleanupStatus:   "not_needed",
	}
	observedIPs := make(map[string]struct{})
	createSamples := make([]time.Duration, 0, consulCfg.samples)
	deleteSamples := make([]time.Duration, 0, consulCfg.samples)
	recoverySamples := make([]time.Duration, 0, consulCfg.samples)
	updateSamples := make([]time.Duration, 0, consulCfg.samples)
	healthDownSamples := make([]time.Duration, 0, consulCfg.samples)
	healthRecoverySamples := make([]time.Duration, 0, consulCfg.samples)
	services := make(map[string][]string)
	active := make(map[string]map[string]bool)
	cleanupAttempted := false
	defer func() {
		cleanupAttempted = true
		cleanupErrs := make([]string, 0)
		for service, ids := range active {
			for id, registered := range ids {
				if !registered {
					continue
				}
				if err := consulClient.Agent().ServiceDeregister(id); err != nil {
					cleanupErrs = append(cleanupErrs, fmt.Sprintf("service_deregister:%s", service))
				}
			}
		}
		for service := range services {
			if _, err := waitForNacosCatalogEmpty(verifyClient, service, normalizedSource.ID, nacosCfg.timeout); err != nil {
				cleanupErrs = append(cleanupErrs, "nacos_catalog_residual")
			}
		}
		if consulServices, err := consulClient.Agent().Services(); err != nil {
			cleanupErrs = append(cleanupErrs, "consul_residual_readback")
		} else {
			for _, ids := range services {
				for _, id := range ids {
					if _, exists := consulServices[id]; exists {
						cleanupErrs = append(cleanupErrs, "consul_residual:"+id)
					}
				}
			}
		}
		report.CleanupStatus = "passed"
		if len(cleanupErrs) > 0 {
			report.CleanupStatus = "failed"
			report.ResidualUnknown = true
		}
		cancel()
		select {
		case <-providerDone:
		case <-time.After(10 * time.Second):
			report.CleanupStatus = "failed"
			report.ResidualUnknown = true
		}
		if err := verifyClient.Close(); err != nil {
			report.CleanupStatus = "failed"
			report.ResidualUnknown = true
		}
		if err := sink.Close(); err != nil {
			report.CleanupStatus = "failed"
			report.ResidualUnknown = true
		}
		if report.CleanupStatus != "passed" || report.ResidualUnknown {
			t.Errorf("CONSUL_REAL_QUALIFICATION cleanup failed: status=%s residual_unknown=%t", report.CleanupStatus, report.ResidualUnknown)
		}
		report.Create = summarizeConsulRealLatencies(createSamples)
		report.Update = summarizeConsulRealLatencies(updateSamples)
		report.Delete = summarizeConsulRealLatencies(deleteSamples)
		report.Recovery = summarizeConsulRealLatencies(recoverySamples)
		report.HealthDown = summarizeConsulRealLatencies(healthDownSamples)
		report.HealthRecovery = summarizeConsulRealLatencies(healthRecoverySamples)
		report.ObservedNacosIPs = make([]string, 0, len(observedIPs))
		for ip := range observedIPs {
			report.ObservedNacosIPs = append(report.ObservedNacosIPs, ip)
		}
		sort.Strings(report.ObservedNacosIPs)
		encoded, _ := json.Marshal(report)
		t.Logf("CONSUL_REAL_QUALIFICATION report=%s cleanup_attempted=%t cleanup_errors=%d", encoded, cleanupAttempted, len(cleanupErrs))
	}()

	for i := 0; i < consulCfg.samples; i++ {
		stamp := time.Now().UTC().Format("20060102T150405.000000000")
		serviceName := fmt.Sprintf("__spotter_consul_real_%s_%02d", stamp, i)
		firstID := serviceName + "-create"
		recoveryID := serviceName + "-recovery"
		services[serviceName] = []string{firstID, recoveryID}
		active[serviceName] = map[string]bool{firstID: false, recoveryID: false}

		registration := realConsulRegistration(serviceName, firstID, 19000+i)
		registerAck, err := registerConsulService(consulClient.Agent(), registration)
		if err != nil {
			t.Fatalf("Consul ServiceRegister create %s: %v", serviceName, err)
		}
		active[serviceName][firstID] = true
		_, hosts, err := waitForNacosCatalog(verifyClient, serviceName, normalizedSource.ID, 1, registration.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("create catalog convergence %s: %v", serviceName, err)
		}
		for _, host := range hosts {
			if host.IP != "" {
				observedIPs[host.IP] = struct{}{}
			}
		}
		createSamples = append(createSamples, time.Since(registerAck))

		// Update the same registered service through the Consul Agent API and
		// require the Nacos catalog to expose the changed port and metadata.
		updated := realConsulRegistration(serviceName, firstID, 19500+i)
		updated.Meta["version"] = "consul-real-updated"
		updateStarted := time.Now()
		if err := consulClient.Agent().ServiceRegister(updated); err != nil {
			t.Fatalf("Consul ServiceRegister update %s: %v", serviceName, err)
		}
		if err := consulClient.Agent().PassTTL(updated.Check.CheckID, "spotter consul qualification update passing"); err != nil {
			t.Fatalf("Consul PassTTL update %s: %v", serviceName, err)
		}
		if _, err := waitForNacosHost(verifyClient, serviceName, normalizedSource.ID, firstID, func(host nacos.Host) bool {
			return host.Port == 19500+i && host.Metadata["version"] == "consul-real-updated"
		}, nacosCfg.timeout); err != nil {
			t.Fatalf("update catalog convergence %s: %v", serviceName, err)
		}
		if err := waitForNacosHostPortAbsent(verifyClient, serviceName, normalizedSource.ID, firstID, 19000+i, nacosCfg.timeout); err != nil {
			t.Fatalf("stale update catalog entry %s: %v", serviceName, err)
		}
		updateSamples = append(updateSamples, time.Since(updateStarted))

		healthDownStarted := time.Now()
		if err := consulClient.Agent().FailTTL(updated.Check.CheckID, "spotter consul qualification health down"); err != nil {
			t.Fatalf("Consul FailTTL %s: %v", serviceName, err)
		}
		if _, err := waitForNacosCatalogEmpty(verifyClient, serviceName, normalizedSource.ID, nacosCfg.timeout); err != nil {
			t.Fatalf("health-down catalog convergence %s: %v", serviceName, err)
		}
		healthDownSamples = append(healthDownSamples, time.Since(healthDownStarted))

		healthRecoveryStarted := time.Now()
		if err := consulClient.Agent().PassTTL(updated.Check.CheckID, "spotter consul qualification health recovery"); err != nil {
			t.Fatalf("Consul PassTTL recovery %s: %v", serviceName, err)
		}
		if _, err := waitForNacosHost(verifyClient, serviceName, normalizedSource.ID, firstID, func(host nacos.Host) bool {
			return host.Port == 19500+i && host.Metadata["version"] == "consul-real-updated"
		}, nacosCfg.timeout); err != nil {
			t.Fatalf("health-recovery catalog convergence %s: %v", serviceName, err)
		}
		healthRecoverySamples = append(healthRecoverySamples, time.Since(healthRecoveryStarted))

		deleteStarted := time.Now()
		if err := consulClient.Agent().ServiceDeregister(firstID); err != nil {
			t.Fatalf("Consul ServiceDeregister create %s: %v", serviceName, err)
		}
		active[serviceName][firstID] = false
		_, err = waitForNacosCatalogEmpty(verifyClient, serviceName, normalizedSource.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("delete catalog convergence %s: %v", serviceName, err)
		}
		deleteSamples = append(deleteSamples, time.Since(deleteStarted))

		recoveryRegistration := realConsulRegistration(serviceName, recoveryID, 19100+i)
		recoveryAck, err := registerConsulService(consulClient.Agent(), recoveryRegistration)
		if err != nil {
			t.Fatalf("Consul ServiceRegister recovery %s: %v", serviceName, err)
		}
		active[serviceName][recoveryID] = true
		_, hosts, err = waitForNacosCatalog(verifyClient, serviceName, normalizedSource.ID, 1, recoveryRegistration.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("recovery catalog convergence %s: %v", serviceName, err)
		}
		recoverySamples = append(recoverySamples, time.Since(recoveryAck))
		for _, host := range hosts {
			if host.IP != "" {
				observedIPs[host.IP] = struct{}{}
			}
		}

		if err := consulClient.Agent().ServiceDeregister(recoveryID); err != nil {
			t.Fatalf("Consul ServiceDeregister recovery %s: %v", serviceName, err)
		}
		active[serviceName][recoveryID] = false
		if _, err := waitForNacosCatalogEmpty(verifyClient, serviceName, normalizedSource.ID, nacosCfg.timeout); err != nil {
			t.Fatalf("final catalog cleanup %s: %v", serviceName, err)
		}
	}

}

func newConsulRealClient(cfg consulRealConfig) (*api.Client, error) {
	options := consul.ConsulClientOptions{
		Token:                 cfg.source.Token,
		TokenFile:             cfg.source.TokenFile,
		TLSCAFile:             cfg.source.TLSCAFile,
		TLSCertFile:           cfg.source.TLSCertFile,
		TLSKeyFile:            cfg.source.TLSKeyFile,
		TLSServerName:         cfg.source.TLSServerName,
		TLSInsecureSkipVerify: cfg.source.TLSInsecureSkipVerify,
		Datacenter:            cfg.source.Datacenter,
		Namespace:             cfg.source.Namespace,
	}
	factory, err := consul.NewClientFactoryWithOptions(cfg.source.Addresses, options, nil)
	if err != nil {
		return nil, err
	}
	return factory.ConsulClientFactory()
}

func realConsulRegistration(service, id string, port int) *api.AgentServiceRegistration {
	return &api.AgentServiceRegistration{
		ID:      id,
		Name:    service,
		Tags:    []string{"microservice", "spotter-consul-real"},
		Address: "127.0.0.1",
		Port:    port,
		Meta: map[string]string{
			"ports":      fmt.Sprintf(`[{"name":"http","protocol":"http","port":%d}]`, port),
			"envType":    "test",
			"envGroup":   "0",
			"appCode":    service,
			"version":    "consul-real",
			"instanceId": id,
		},
		Check: &api.AgentServiceCheck{
			CheckID: "service:" + service,
			TTL:     "2m",
		},
	}
}

func registerConsulService(agent *api.Agent, registration *api.AgentServiceRegistration) (time.Time, error) {
	started := time.Now()
	if err := agent.ServiceRegister(registration); err != nil {
		return time.Time{}, err
	}
	if err := agent.PassTTL(registration.Check.CheckID, "spotter consul qualification passing"); err != nil {
		return time.Time{}, err
	}
	return started, nil
}

func waitForNacosCatalog(client *nacos.Client, service, cluster string, want int, instanceID string, timeout time.Duration) (time.Duration, []nacos.Host, error) {
	started := time.Now()
	deadline := started.Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		hosts, err := client.ListCatalogInstances(service, cluster)
		if err == nil {
			if len(hosts) == want {
				if want == 0 || (len(hosts) == 1 && hosts[0].Metadata["instanceId"] == instanceID && hosts[0].Enabled && hosts[0].Healthy) {
					return time.Since(started), hosts, nil
				}
			} else {
				lastErr = fmt.Errorf("catalog entries=%d want=%d", len(hosts), want)
			}
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, nil, fmt.Errorf("catalog convergence timeout for %s/%s: %v", service, cluster, lastErr)
}

func waitForNacosCatalogEmpty(client *nacos.Client, service, cluster string, timeout time.Duration) (time.Duration, error) {
	latency, _, err := waitForNacosCatalog(client, service, cluster, 0, "", timeout)
	return latency, err
}

func waitForNacosHost(client *nacos.Client, service, cluster, instanceID string, predicate func(nacos.Host) bool, timeout time.Duration) (time.Duration, error) {
	started := time.Now()
	deadline := started.Add(timeout)
	for time.Now().Before(deadline) {
		hosts, err := client.ListCatalogInstances(service, cluster)
		if err == nil {
			for _, host := range hosts {
				if host.Metadata["instanceId"] == instanceID && host.Enabled && host.Healthy && predicate(host) {
					return time.Since(started), nil
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return 0, fmt.Errorf("catalog host %s did not satisfy predicate", instanceID)
}

func waitForNacosHostPortAbsent(client *nacos.Client, service, cluster, instanceID string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		hosts, err := client.ListCatalogInstances(service, cluster)
		if err == nil {
			stale := false
			for _, host := range hosts {
				if host.Metadata["instanceId"] == instanceID && host.Port == port {
					stale = true
					break
				}
			}
			if !stale {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("catalog host %s retained stale port %d", instanceID, port)
}

func summarizeConsulRealLatencies(samples []time.Duration) consulRealLatencySummary {
	toMS := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return consulRealLatencySummary{
		Samples: len(samples),
		P50MS:   toMS(percentile(samples, 0.50)),
		P90MS:   toMS(percentile(samples, 0.90)),
		P95MS:   toMS(percentile(samples, 0.95)),
		P99MS:   toMS(percentile(samples, 0.99)),
	}
}

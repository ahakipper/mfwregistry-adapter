//go:build consul_real
// +build consul_real

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	Endpoint        string                   `json:"endpoint"`
	SourceID        string                   `json:"source_id"`
	ConfiguredRuns  int                      `json:"configured_runs"`
	Create          consulRealLatencySummary `json:"create"`
	Delete          consulRealLatencySummary `json:"delete"`
	Recovery        consulRealLatencySummary `json:"recovery"`
	CleanupStatus   string                   `json:"cleanup_status"`
	ResidualUnknown bool                     `json:"residual_unknown"`
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
		Endpoint:       redactConsulEndpoints(consulCfg.addresses) + " -> " + nacosCfg.endpoint,
		SourceID:       normalizedSource.ID,
		ConfiguredRuns: consulCfg.samples,
		CleanupStatus:  "not_needed",
	}
	createSamples := make([]time.Duration, 0, consulCfg.samples)
	deleteSamples := make([]time.Duration, 0, consulCfg.samples)
	recoverySamples := make([]time.Duration, 0, consulCfg.samples)
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
		report.Delete = summarizeConsulRealLatencies(deleteSamples)
		report.Recovery = summarizeConsulRealLatencies(recoverySamples)
		encoded, _ := json.Marshal(report)
		t.Logf("CONSUL_REAL_QUALIFICATION report=%s cleanup_attempted=%t cleanup_errors=%d", encoded, cleanupAttempted, len(cleanupErrs))
	}()

	for i := 0; i < consulCfg.samples; i++ {
		stamp := time.Now().UTC().Format("20060102T150405.000000000")
		serviceName := fmt.Sprintf("__spotter_consul_real_%s_%02d", stamp, i)
		firstID := serviceName + "-create"
		recoveryID := serviceName + "-recovery"
		services[serviceName] = []string{firstID, recoveryID}
		active[serviceName] = map[string]bool{firstID: true, recoveryID: false}

		registration := realConsulRegistration(serviceName, firstID, 19000+i)
		registerAck, err := registerConsulService(consulClient.Agent(), registration)
		if err != nil {
			t.Fatalf("Consul ServiceRegister create %s: %v", serviceName, err)
		}
		_, _, err = waitForNacosCatalog(verifyClient, serviceName, normalizedSource.ID, 1, registration.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("create catalog convergence %s: %v", serviceName, err)
		}
		createSamples = append(createSamples, time.Since(registerAck))

		deleteAck := time.Now()
		if err := consulClient.Agent().ServiceDeregister(firstID); err != nil {
			t.Fatalf("Consul ServiceDeregister create %s: %v", serviceName, err)
		}
		active[serviceName][firstID] = false
		deleteAck = time.Now()
		_, err = waitForNacosCatalogEmpty(verifyClient, serviceName, normalizedSource.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("delete catalog convergence %s: %v", serviceName, err)
		}
		deleteSamples = append(deleteSamples, time.Since(deleteAck))

		recoveryRegistration := realConsulRegistration(serviceName, recoveryID, 19100+i)
		active[serviceName][recoveryID] = true
		recoveryAck, err := registerConsulService(consulClient.Agent(), recoveryRegistration)
		if err != nil {
			t.Fatalf("Consul ServiceRegister recovery %s: %v", serviceName, err)
		}
		_, _, err = waitForNacosCatalog(verifyClient, serviceName, normalizedSource.ID, 1, recoveryRegistration.ID, nacosCfg.timeout)
		if err != nil {
			t.Fatalf("recovery catalog convergence %s: %v", serviceName, err)
		}
		recoverySamples = append(recoverySamples, time.Since(recoveryAck))

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
	if err := agent.ServiceRegister(registration); err != nil {
		return time.Time{}, err
	}
	ack := time.Now()
	if err := agent.PassTTL(registration.Check.CheckID, "spotter consul qualification passing"); err != nil {
		return time.Time{}, err
	}
	return ack, nil
}

func waitForNacosCatalog(client *nacos.Client, service, cluster string, want int, instanceID string, timeout time.Duration) (time.Duration, []nacos.Host, error) {
	started := time.Now()
	deadline := started.Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		hosts, err := client.ListCatalogInstances(service, cluster)
		if err == nil {
			if len(hosts) == want {
				if want == 0 || (len(hosts) == 1 && hosts[0].Metadata["instanceId"] == instanceID) {
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

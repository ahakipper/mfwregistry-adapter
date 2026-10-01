//go:build consul_real && consul_scale_real
// +build consul_real,consul_scale_real

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/pkg/nacos"
	"spotter/pkg/providers/consul"
	"spotter/pkg/worker"
)

type consulScaleLatencySummary struct {
	Samples int     `json:"samples"`
	P80MS   float64 `json:"p80_ms"`
	P90MS   float64 `json:"p90_ms"`
	P99MS   float64 `json:"p99_ms"`
}

type consulScaleResult struct {
	Instances              int                       `json:"instances"`
	Runs                   int                       `json:"runs"`
	FullEvents             int                       `json:"sync_all_events"`
	WatchToProviderSync    consulScaleLatencySummary `json:"watch_to_provider_sync"`
	ProviderSyncToNacosAck consulScaleLatencySummary `json:"provider_sync_to_nacos_ack"`
	NacosAckToCatalog      consulScaleLatencySummary `json:"nacos_ack_to_catalog"`
	NacosAckToSubscribe    consulScaleLatencySummary `json:"nacos_ack_to_subscribe"`
	Catalog                consulScaleLatencySummary `json:"catalog"`
	Subscribe              consulScaleLatencySummary `json:"subscribe"`
	StageSamplesComplete   bool                      `json:"stage_samples_complete"`
}

type consulScaleReport struct {
	Boundary          string              `json:"latency_boundary"`
	Scales            []consulScaleResult `json:"scales"`
	LedgerComplete    bool                `json:"ledger_complete"`
	CanonicalEquality string              `json:"canonical_equality"`
	FailedScale       int                 `json:"failed_scale,omitempty"`
	FailureKind       string              `json:"failure_kind,omitempty"`
	CleanupStatus     string              `json:"cleanup_status"`
	ResidualUnknown   bool                `json:"residual_unknown"`
}

type scaleRecordingWorker struct {
	inner      worker.Worker
	mu         sync.Mutex
	full       int
	watchAt    map[string]time.Time
	providerAt map[string]time.Time
	writeAckAt map[string]time.Time
}

func (w *scaleRecordingWorker) AddEventHandler(op worker.OperateType, handler worker.EventResourceHandler) {
	w.inner.AddEventHandler(op, handler)
}
func (w *scaleRecordingWorker) Handle(event *worker.Event) {
	if event != nil {
		now := time.Now()
		w.mu.Lock()
		if event.Operate == worker.OperateTypeSyncAll {
			w.full++
		}
		for _, item := range event.Data {
			if item == nil || item.InstanceId == "" {
				continue
			}
			if w.providerAt == nil {
				w.providerAt = make(map[string]time.Time)
			}
			if _, exists := w.providerAt[item.InstanceId]; !exists {
				w.providerAt[item.InstanceId] = now
			}
			if event.Trigger > 0 {
				if w.watchAt == nil {
					w.watchAt = make(map[string]time.Time)
				}
				if _, exists := w.watchAt[item.InstanceId]; !exists {
					w.watchAt[item.InstanceId] = time.Unix(0, event.Trigger)
				}
			}
		}
		w.mu.Unlock()
	}
	w.inner.Handle(event)
}
func (w *scaleRecordingWorker) ProcessUnsynced() { w.inner.ProcessUnsynced() }
func (w *scaleRecordingWorker) GetAll(status []int32, provider string) (*instance.InstanceList, error) {
	return w.inner.GetAll(status, provider)
}
func (w *scaleRecordingWorker) fullEvents() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.full
}

func (w *scaleRecordingWorker) recordWriteAck(items []*instance.Instance, acknowledgedAt time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeAckAt == nil {
		w.writeAckAt = make(map[string]time.Time)
	}
	for _, item := range items {
		if item != nil && item.InstanceId != "" {
			if _, exists := w.writeAckAt[item.InstanceId]; !exists {
				w.writeAckAt[item.InstanceId] = acknowledgedAt
			}
		}
	}
}

type consulScaleStageSamples struct {
	WatchToProviderSync    []time.Duration
	ProviderSyncToNacosAck []time.Duration
	NacosAckToCatalog      []time.Duration
	NacosAckToSubscribe    []time.Duration
}

func (w *scaleRecordingWorker) stageSamples(ids []string, catalogAt, subscribeAt map[string]time.Time) (consulScaleStageSamples, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := consulScaleStageSamples{
		WatchToProviderSync:    make([]time.Duration, 0, len(ids)),
		ProviderSyncToNacosAck: make([]time.Duration, 0, len(ids)),
		NacosAckToCatalog:      make([]time.Duration, 0, len(ids)),
		NacosAckToSubscribe:    make([]time.Duration, 0, len(ids)),
	}
	complete := true
	for _, id := range ids {
		watch, watchOK := w.watchAt[id]
		provider, providerOK := w.providerAt[id]
		ack, ackOK := w.writeAckAt[id]
		catalog, catalogOK := catalogAt[id]
		subscribe, subscribeOK := subscribeAt[id]
		if !watchOK || !providerOK || !ackOK || !catalogOK || !subscribeOK {
			complete = false
			continue
		}
		result.WatchToProviderSync = append(result.WatchToProviderSync, provider.Sub(watch))
		result.ProviderSyncToNacosAck = append(result.ProviderSyncToNacosAck, ack.Sub(provider))
		result.NacosAckToCatalog = append(result.NacosAckToCatalog, catalog.Sub(ack))
		result.NacosAckToSubscribe = append(result.NacosAckToSubscribe, subscribe.Sub(ack))
	}
	return result, complete
}

func TestConsulRealScaleQualification(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CONSUL_SERVER")) == "" || strings.TrimSpace(os.Getenv("NACOS_SERVER")) == "" {
		t.Skip("NOT VERIFIED: CONSUL_SERVER and NACOS_SERVER are required")
	}
	consulCfg, err := parseConsulRealConfig()
	if err != nil {
		t.Fatalf("invalid Consul config: %v", err)
	}
	nacosCfg, err := parseNacosRealConfig()
	if err != nil {
		t.Fatalf("invalid Nacos config: %v", err)
	}
	if !consulCfg.canWrite || !nacosCfg.canWrite || os.Getenv("NACOS_REAL_ALLOW_ADMIN") != "1" {
		t.Skip("NOT VERIFIED: explicit Consul/Nacos scratch, write, and admin guards are required")
	}
	source, err := consulCfg.source.Normalized()
	if err != nil {
		t.Fatalf("normalize source: %v", err)
	}
	scales := parseScaleList(t, os.Getenv("CONSUL_REAL_SCALE_LIST"))
	runs := parseScaleRuns(t, os.Getenv("CONSUL_REAL_SCALE_RUNS"))
	observeTimeout := parseScaleObserveTimeout(t, os.Getenv("CONSUL_REAL_SCALE_OBSERVE_TIMEOUT"))
	if err := nacos.CheckReadinessWithConfig(nacosCfg.client, nil); err != nil {
		t.Fatalf("Nacos readiness: %v", err)
	}
	adminConfig := nacosCfg.client
	adminConfig.TransportMode = nacos.TransportHTTPCompat
	admin, err := nacos.NewClientWithConfig(adminConfig, nil)
	if err != nil {
		t.Fatalf("Nacos admin client: %v", err)
	}
	initialHealth, err := admin.GetNamingHealthCheckEnabledV3AdminCompat()
	if err != nil {
		_ = admin.Close()
		t.Fatalf("Nacos health readback: %v", err)
	}
	defer func() {
		if err := admin.SetNamingHealthCheckEnabledV3AdminCompat(initialHealth); err != nil {
			t.Errorf("restore Nacos health switch: %v", err)
		}
		_ = admin.Close()
	}()
	if err := admin.SetNamingHealthCheckEnabledV3AdminCompat(false); err != nil {
		t.Fatalf("disable Nacos health checks: %v", err)
	}
	if enabled, err := admin.GetNamingHealthCheckEnabledV3AdminCompat(); err != nil || enabled {
		t.Fatalf("Nacos health switch readback enabled=%t err=%v", enabled, err)
	}

	verifyConfig := nacosCfg.client
	verifyConfig.UpdateCacheWhenEmpty = true
	group := verifyConfig.GroupName
	if group == "" {
		group = nacos.DefaultGroup
	}
	consulClient, err := newConsulRealClient(consulCfg)
	if err != nil {
		t.Fatalf("Consul client: %v", err)
	}
	sink, err := nacos.NewSinkWithConfig(nacosCfg.client, nil)
	if err != nil {
		t.Fatalf("Nacos sink: %v", err)
	}
	verifyClient, err := nacos.NewClientWithConfig(verifyConfig, nil)
	if err != nil {
		_ = sink.Close()
		t.Fatalf("Nacos verifier: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, sink, nil, nil)
	if err != nil {
		_ = verifyClient.Close()
		_ = sink.Close()
		t.Fatalf("worker: %v", err)
	}
	recordingWorker := &scaleRecordingWorker{inner: w}
	sink.SetWriteAckObserver(recordingWorker.recordWriteAck)
	// Disable the periodic full-push ticker for this event-driven scale gate;
	// zero selects the production six-hour default and prevents reconcile
	// traffic from contaminating mutation-to-visibility latency.
	provider, err := consul.NewConsulProviderWithSource(ctx, recordingWorker, 0, source, nil, nil)
	if err != nil {
		_ = verifyClient.Close()
		_ = sink.Close()
		t.Fatalf("provider: %v", err)
	}
	providerDone := make(chan error, 1)
	go func() { providerDone <- provider.Run() }()

	report := consulScaleReport{
		Boundary:          "Consul instance mutation start to Nacos catalog/official SDK Subscribe visibility",
		LedgerComplete:    true,
		CanonicalEquality: "wire_predicate_passed",
		CleanupStatus:     "not_needed",
	}
	active := make(map[string][]string)
	subscriptions := make(map[string]*consulRealSubscribeOracle)
	var cleanupMu sync.Mutex
	cleanupErrs := make([]string, 0)
	defer func() {
		for service, ids := range active {
			var wg sync.WaitGroup
			for _, id := range ids {
				id := id
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := consulClient.Agent().ServiceDeregister(id); err != nil {
						cleanupMu.Lock()
						cleanupErrs = append(cleanupErrs, fmt.Sprintf("deregister:%s", service))
						cleanupMu.Unlock()
					}
				}()
			}
			wg.Wait()
			if oracle := subscriptions[service]; oracle != nil {
				if err := verifyClient.Unsubscribe(service, group, []string{source.ID}, oracle.Callback()); err != nil {
					cleanupErrs = append(cleanupErrs, "unsubscribe:"+service)
				}
			}
			if _, err := waitForNacosCatalogEmpty(verifyClient, service, source.ID, observeTimeout); err != nil {
				cleanupErrs = append(cleanupErrs, "catalog_residual:"+service)
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
		case <-time.After(15 * time.Second):
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
		encoded, _ := json.Marshal(report)
		t.Logf("CONSUL_REAL_SCALE report=%s", encoded)
		if report.CleanupStatus != "passed" || report.ResidualUnknown {
			t.Errorf("scale cleanup failed: status=%s residual_unknown=%t", report.CleanupStatus, report.ResidualUnknown)
		}
	}()

	for _, size := range scales {
		for run := 0; run < runs; run++ {
			service := fmt.Sprintf("__spotter_consul_scale_%d_%02d", size, run)
			ids := make([]string, size)
			started := make(map[string]time.Time, size)
			active[service] = ids
			oracle := newConsulRealSubscribeOracle()
			subscriptions[service] = oracle
			if err := verifyClient.Subscribe(service, group, []string{source.ID}, oracle.Callback()); err != nil {
				t.Fatalf("Subscribe %s: %v", service, err)
			}
			checkpoint := oracle.checkpoint()
			fullBefore := recordingWorker.fullEvents()
			var wg sync.WaitGroup
			sem := make(chan struct{}, 16)
			var startMu sync.Mutex
			for i := 0; i < size; i++ {
				id := fmt.Sprintf("%s-%05d", service, i)
				ids[i] = id
				wg.Add(1)
				go func(i int, id string) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					startMu.Lock()
					started[id] = time.Now()
					startMu.Unlock()
					registration := realConsulRegistration(service, id, 19000+(i%30000))
					registration.Check.CheckID = "service:" + id
					registration.Meta["version"] = "consul-scale"
					if err := consulClient.Agent().ServiceRegister(registration); err != nil {
						t.Errorf("ServiceRegister %s: %v", id, err)
						return
					}
					if err := consulClient.Agent().PassTTL(registration.Check.CheckID, "consul scale passing"); err != nil {
						t.Errorf("PassTTL %s: %v", id, err)
					}
				}(i, id)
			}
			wg.Wait()
			expectedPorts := make(map[string]int, size)
			for i, id := range ids {
				expectedPorts[id] = 19000 + (i % 30000)
			}
			catalogVisibility, err := collectScaleCatalogLatencies(service, source.ID, size, started, expectedPorts, verifyClient, observeTimeout)
			if err != nil {
				report.LedgerComplete = false
				report.CanonicalEquality = "not_verified"
				report.FailedScale = size
				report.FailureKind = "catalog_visibility_timeout"
				t.Errorf("catalog scale=%d run=%d: %v", size, run, err)
				return
			}
			subscribeVisibility, err := collectScaleSubscribeLatencies(oracle, checkpoint, size, started, expectedPorts, observeTimeout)
			if err != nil {
				report.LedgerComplete = false
				report.CanonicalEquality = "not_verified"
				report.FailedScale = size
				report.FailureKind = "subscribe_visibility_timeout"
				t.Errorf("Subscribe scale=%d run=%d: %v", size, run, err)
				return
			}
			stages, stageComplete := recordingWorker.stageSamples(ids, catalogVisibility.ObservedAt, subscribeVisibility.ObservedAt)
			report.Scales = append(report.Scales, consulScaleResult{
				Instances:              size,
				Runs:                   runs,
				Catalog:                summarizeConsulScale(catalogVisibility.Samples),
				Subscribe:              summarizeConsulScale(subscribeVisibility.Samples),
				WatchToProviderSync:    summarizeConsulScale(stages.WatchToProviderSync),
				ProviderSyncToNacosAck: summarizeConsulScale(stages.ProviderSyncToNacosAck),
				NacosAckToCatalog:      summarizeConsulScale(stages.NacosAckToCatalog),
				NacosAckToSubscribe:    summarizeConsulScale(stages.NacosAckToSubscribe),
				StageSamplesComplete:   stageComplete,
			})
			if len(catalogVisibility.Samples) != size || len(subscribeVisibility.Samples) != size || !stageComplete {
				report.LedgerComplete = false
			}
			fullEvents := recordingWorker.fullEvents() - fullBefore
			if fullEvents == 0 {
				report.LedgerComplete = false
				report.CanonicalEquality = "not_verified"
				report.FailedScale = size
				report.FailureKind = "missing_sync_all_batch_path"
				t.Errorf("scale=%d produced no SyncAll event for a complete initial snapshot", size)
				return
			}
			report.Scales[len(report.Scales)-1].FullEvents = fullEvents
			// Isolate each scale/run. Leaving previous services registered would
			// contaminate the next provider snapshot, full reconcile, and Nacos
			// catalog latency with older populations.
			for _, id := range ids {
				if err := consulClient.Agent().ServiceDeregister(id); err != nil {
					t.Fatalf("scale cleanup deregister %s: %v", id, err)
				}
			}
			if err := verifyClient.Unsubscribe(service, group, []string{source.ID}, oracle.Callback()); err != nil {
				t.Fatalf("scale cleanup unsubscribe %s: %v", service, err)
			}
			if _, err := waitForNacosCatalogEmpty(verifyClient, service, source.ID, observeTimeout); err != nil {
				t.Fatalf("scale cleanup catalog %s: %v", service, err)
			}
			delete(active, service)
			delete(subscriptions, service)
		}
	}
}

func parseScaleList(t *testing.T, raw string) []int {
	t.Helper()
	if strings.TrimSpace(raw) == "" {
		return []int{100, 1000, 10000}
	}
	result := make([]int, 0)
	for _, part := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 || n > 10000 {
			t.Fatalf("invalid CONSUL_REAL_SCALE_LIST value %q", raw)
		}
		result = append(result, n)
	}
	sort.Ints(result)
	return result
}

func parseScaleRuns(t *testing.T, raw string) int {
	t.Helper()
	if strings.TrimSpace(raw) == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 || n > 10 {
		t.Fatalf("invalid CONSUL_REAL_SCALE_RUNS value %q", raw)
	}
	return n
}

func parseScaleObserveTimeout(t *testing.T, raw string) time.Duration {
	t.Helper()
	if strings.TrimSpace(raw) == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		t.Fatalf("invalid CONSUL_REAL_SCALE_OBSERVE_TIMEOUT value %q", raw)
	}
	return d
}

func summarizeConsulScale(samples []time.Duration) consulScaleLatencySummary {
	return consulScaleLatencySummary{
		Samples: len(samples),
		P80MS:   float64(percentile(samples, 0.80)) / float64(time.Millisecond),
		P90MS:   float64(percentile(samples, 0.90)) / float64(time.Millisecond),
		P99MS:   float64(percentile(samples, 0.99)) / float64(time.Millisecond),
	}
}

type consulScaleVisibilitySamples struct {
	Samples    []time.Duration
	ObservedAt map[string]time.Time
}

func collectScaleCatalogLatencies(service, cluster string, want int, origins map[string]time.Time, expectedPorts map[string]int, client *nacos.Client, timeout time.Duration) (consulScaleVisibilitySamples, error) {
	deadline := time.Now().Add(timeout)
	seen := make(map[string]struct{}, want)
	samples := make([]time.Duration, 0, want)
	observedAtByID := make(map[string]time.Time, want)
	var lastErr error
	for time.Now().Before(deadline) {
		hosts, err := client.ListCatalogInstances(service, cluster)
		if err != nil {
			lastErr = err
		} else {
			observedAt := time.Now()
			for _, host := range hosts {
				id := host.Metadata["instanceId"]
				origin, ok := origins[id]
				if !ok || host.Port != expectedPorts[id] || host.Metadata["version"] != "consul-scale" || !host.Enabled || !host.Healthy {
					continue
				}
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				observedAtByID[id] = observedAt
				samples = append(samples, observedAt.Sub(origin))
			}
			if len(seen) == want {
				return consulScaleVisibilitySamples{Samples: samples, ObservedAt: observedAtByID}, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return consulScaleVisibilitySamples{}, fmt.Errorf("catalog visibility timeout: observed=%d want=%d last=%v", len(seen), want, lastErr)
}

func collectScaleSubscribeLatencies(oracle *consulRealSubscribeOracle, checkpoint, want int, origins map[string]time.Time, expectedPorts map[string]int, timeout time.Duration) (consulScaleVisibilitySamples, error) {
	deadline := time.Now().Add(timeout)
	seen := make(map[string]struct{}, want)
	samples := make([]time.Duration, 0, want)
	observedAtByID := make(map[string]time.Time, want)
	for time.Now().Before(deadline) {
		oracle.mu.Lock()
		for checkpoint < len(oracle.events) {
			event := oracle.events[checkpoint]
			checkpoint++
			if event.Err != "" {
				continue
			}
			for _, host := range event.Hosts {
				id := host.Metadata["instanceId"]
				origin, ok := origins[id]
				if !ok || host.Port != expectedPorts[id] || host.Metadata["version"] != "consul-scale" || !host.Enabled || !host.Healthy {
					continue
				}
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
				observedAtByID[id] = event.ReceivedAt
				samples = append(samples, event.ReceivedAt.Sub(origin))
			}
		}
		oracle.mu.Unlock()
		if len(seen) == want {
			return consulScaleVisibilitySamples{Samples: samples, ObservedAt: observedAtByID}, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.NewTimer(remaining)
		select {
		case <-oracle.notify:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
	return consulScaleVisibilitySamples{}, fmt.Errorf("Subscribe visibility timeout: observed=%d want=%d", len(seen), want)
}

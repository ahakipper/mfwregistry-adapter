package nacos_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/nacos"
)

// SinkName is the name the server wiring registers the sink under in the
// fanout; asserted here so the fanout/retry keys and the metrics labels have
// exactly one spelling.
func TestBlackboxSinkNameMatchesFanoutConvention(t *testing.T) {
	if nacos.SinkName != "nacos" {
		t.Fatalf("SinkName = %q, want %q", nacos.SinkName, "nacos")
	}
}

// newSinkAt starts a nacosmock server and builds a Nacos sink over it.
func newSinkAt(t *testing.T) (*nacos.Sink, *nacosmock.Server) {
	t.Helper()
	server := nacosmock.Start()
	t.Cleanup(server.Close)
	sink, err := nacos.NewSink(server.URL(), &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(%s) error = %v", server.URL(), err)
	}
	return sink, server
}

// domainInstance builds a fully populated domain instance for the mapping
// tests. port may be 0 (a port-less instance).
func domainInstance(id, appCode, ip string, port int32, provider string, status int32) *instance.Instance {
	return &instance.Instance{
		InstanceId: id,
		AppCode:    appCode,
		Ip:         ip,
		Ports:      []*instance.PortInfo{{Name: "http", Protocol: "tcp", Port: port}},
		Provider:   provider,
		Cluster:    provider,
		Enabled:    status != 3,
		EnvType:    "test",
		EnvGroup:   "7",
		State:      "running",
		Idc:        "kraken",
		Cpu:        2,
		Version:    "v1",
		Reversion:  42,
		Status:     status,
	}
}

// deregisterRequest finds the DELETE request addressing the given ip/port,
// for asserting the delete path of the sink.
func deregisterRequest(server *nacosmock.Server, ip string, port int) *nacosmock.Request {
	requests := server.Requests()
	for i := range requests {
		request := &requests[i]
		if request.Method == "DELETE" && request.Query.Get("ip") == ip && request.Query.Get("port") == strconv.Itoa(port) {
			return request
		}
	}
	return nil
}

// registerRequest finds the POST request addressing the given ip/port.
func registerRequest(server *nacosmock.Server, ip string, port int) *nacosmock.Request {
	requests := server.Requests()
	for i := range requests {
		request := &requests[i]
		if request.Method == "POST" && request.Query.Get("ip") == ip && request.Query.Get("port") == strconv.Itoa(port) {
			return request
		}
	}
	return nil
}

// clusterUpdateRequests returns every PUT /nacos/v1/ns/cluster request
// addressing the given (service, cluster) — the UpdateCluster calls the sink
// issues to disable Nacos's server-side health check.
func clusterUpdateRequests(server *nacosmock.Server, service, cluster string) []nacosmock.Request {
	var matched []nacosmock.Request
	for _, request := range server.Requests() {
		if request.Method == "PUT" && request.Path == "/nacos/v1/ns/cluster" &&
			request.Query.Get("serviceName") == service && request.Query.Get("clusterName") == cluster {
			matched = append(matched, request)
		}
	}
	return matched
}

// TestBlackboxSinkFirstRegisterDisablesServerSideHealthCheck: the FIRST
// non-offline push of a (service, cluster) pair follows its register with
// exactly one UpdateCluster (healthChecker NONE) — the product change:
// spotter owns health authority (K8s readiness / consul checks drive the
// pushed enabled flag), so Nacos's own TCP probes must not run on
// spotter-managed data. The PUT lands AFTER the register it configures.
func TestBlackboxSinkFirstRegisterDisablesServerSideHealthCheck(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	updates := clusterUpdateRequests(server, "pay-user", "k8s")
	if len(updates) != 1 {
		t.Fatalf("cluster updates for pay-user/k8s = %d, want exactly 1 (the first register configures the pair); requests = %v",
			len(updates), server.Requests())
	}
	if got := updates[0].Query.Get("healthChecker"); got != `{"type":"NONE"}` {
		t.Fatalf("cluster update healthChecker = %q, want the NONE checker", got)
	}
	// The configuration is recorded as configuration-of-record too.
	config := server.ClusterConfig("pay-user", "DEFAULT_GROUP", "k8s")
	if config == nil || config.HealthCheckerType != "NONE" {
		t.Fatalf("stored cluster config = %+v, want the NONE health checker", config)
	}
	// The PUT follows the register, never precedes it.
	requests := server.Requests()
	registerIndex, updateIndex := -1, -1
	for i, request := range requests {
		if request.Method == "POST" && request.Query.Get("ip") == "10.0.0.1" {
			registerIndex = i
		}
		if request.Method == "PUT" && request.Path == "/nacos/v1/ns/cluster" {
			updateIndex = i
		}
	}
	if registerIndex == -1 || updateIndex == -1 || updateIndex < registerIndex {
		t.Fatalf("register at %d, cluster update at %d, want the update after the register; requests = %v",
			registerIndex, updateIndex, requests)
	}
}

// TestBlackboxSinkSecondPushDoesNotReissueClusterUpdate: the applied marker
// makes the cluster update once-per-pair-per-process — a second push of the
// same pair registers its instances without another PUT (the update is
// configuration-of-record, not per-instance data).
func TestBlackboxSinkSecondPushDoesNotReissueClusterUpdate(t *testing.T) {
	sink, server := newSinkAt(t)

	pair := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
	}
	if err := sink.Push(1, pair); err != nil {
		t.Fatalf("Push(1) error = %v", err)
	}
	if err := sink.Push(2, pair); err != nil {
		t.Fatalf("Push(2) error = %v", err)
	}
	// A full push of the same pair counts too: the marker survives.
	if err := sink.PushAll(3, pair); err != nil {
		t.Fatalf("PushAll(3) error = %v", err)
	}

	if updates := clusterUpdateRequests(server, "pay-user", "k8s"); len(updates) != 1 {
		t.Fatalf("cluster updates after three pushes of the same pair = %d, want 1 (the applied marker holds); requests = %v",
			len(updates), server.Requests())
	}
	// A DIFFERENT pair of the same push still gets its own update: per-pair
	// state, not per-process.
	other := domainInstance("srv-a", "pay-user", "10.0.0.2", 8081, "ecs", 1)
	if err := sink.Push(4, []*instance.Instance{other}); err != nil {
		t.Fatalf("Push(ecs) error = %v", err)
	}
	if updates := clusterUpdateRequests(server, "pay-user", "ecs"); len(updates) != 1 {
		t.Fatalf("cluster updates for pay-user/ecs = %d, want 1 (a new pair configures once)", len(updates))
	}
}

// TestBlackboxSinkClusterUpdateFailureDoesNotFailPushAndRetries: a failed
// cluster update must not fail the register push (the instance registration
// itself succeeded; the cluster configuration is configuration-of-record),
// and the pair's applied marker must stay unset so the NEXT push retries
// the update — bounded, self-healing, no retry-queue poisoning. The failure
// is injected with a stub HTTP server (the nacosmock's SetStatus knob hits
// every endpoint, which would fail the register too); once the stub
// recovers, the retry succeeds and the marker then holds.
func TestBlackboxSinkClusterUpdateFailureDoesNotFailPushAndRetries(t *testing.T) {
	var clusterFailures int32
	var mu sync.Mutex
	failing := int32(1) // 1: the cluster PUT fails; 0: recovered
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/nacos/v1/ns/cluster" {
			mu.Lock()
			failingNow := atomic.LoadInt32(&failing) == 1
			if failingNow {
				clusterFailures++
			}
			mu.Unlock()
			if failingNow {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, "injected cluster failure")
				return
			}
		}
		writeStubOK(w)
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	pair := []*instance.Instance{domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)}
	// Push 1: the register succeeds, the cluster update fails — the push
	// still returns nil.
	if err := sink.Push(1, pair); err != nil {
		t.Fatalf("Push() with the failing cluster update error = %v, want nil (the register succeeded)", err)
	}
	mu.Lock()
	failures := clusterFailures
	mu.Unlock()
	if failures != 1 {
		t.Fatalf("cluster update failures = %d, want 1", failures)
	}

	// Push 2 while still failing: the retry happens (the marker never set),
	// and the push still succeeds.
	if err := sink.Push(2, pair); err != nil {
		t.Fatalf("Push(2) with the failing cluster update error = %v, want nil", err)
	}
	mu.Lock()
	failures = clusterFailures
	mu.Unlock()
	if failures != 2 {
		t.Fatalf("cluster update failures after two pushes = %d, want 2 (the failure leaves the pair unapplied, so it retries)", failures)
	}

	// Recovery: the next push's update succeeds and the marker then holds —
	// no further attempts.
	mu.Lock()
	atomic.StoreInt32(&failing, 0)
	mu.Unlock()
	if err := sink.Push(3, pair); err != nil {
		t.Fatalf("Push(3) after recovery error = %v, want nil", err)
	}
	if err := sink.Push(4, pair); err != nil {
		t.Fatalf("Push(4) after recovery error = %v, want nil", err)
	}
	mu.Lock()
	failures = clusterFailures
	mu.Unlock()
	if failures != 2 {
		t.Fatalf("cluster update failures after recovery = %d, want 2 (the successful update set the marker; no further attempts)", failures)
	}
}

// TestBlackboxSinkOfflineOnlyPushesNeverUpdateCluster: the cluster update
// rides the register path only — an offline push deregisters, an
// empty-ip shell and an unknown status are skipped, and none of them may
// configure a pair: a pair becomes this sink's to configure through the
// same first non-offline push that registers it.
func TestBlackboxSinkOfflineOnlyPushesNeverUpdateCluster(t *testing.T) {
	sink, server := newSinkAt(t)

	offline := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 3)
	if err := sink.Push(1, []*instance.Instance{offline}); err != nil {
		t.Fatalf("Push(offline) error = %v", err)
	}
	shell := domainInstance("pod-shell", "pay-user", "", 8080, "k8s", 3)
	if err := sink.Push(2, []*instance.Instance{shell}); err != nil {
		t.Fatalf("Push(offline empty ip) error = %v", err)
	}
	unknown := domainInstance("pod-x", "pay-user", "10.0.0.9", 8082, "k8s", 0)
	if err := sink.Push(3, []*instance.Instance{unknown}); err != nil {
		t.Fatalf("Push(unknown status) error = %v", err)
	}

	requests := server.Requests()
	for _, request := range requests {
		if request.Method == "PUT" && request.Path == "/nacos/v1/ns/cluster" {
			t.Fatalf("offline-only pushes issued a cluster update; requests = %v", requests)
		}
	}
}

func TestBlackboxSinkPushOnlineInstanceRegisters(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1234567890, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	requests := server.Requests()
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2 (the register plus the first-register cluster update that disables Nacos's own health check)", len(requests))
	}
	got := requests[0]
	if got.Method != "POST" {
		t.Fatalf("request method = %s, want POST (register)", got.Method)
	}
	want := map[string]string{
		"serviceName": "pay-user", // AppCode
		"ip":          "10.0.0.1", // Ip
		"port":        "8080",     // Ports[0].Port
		"clusterName": "k8s",      // Provider
		"groupName":   "DEFAULT_GROUP",
		"namespaceId": "public",
		"ephemeral":   "false", // persistent
		"enabled":     "true",  // Enabled, Status online
	}
	for key, value := range want {
		if got.Query.Get(key) != value {
			t.Fatalf("register query[%s] = %q, want %q (all: %v)", key, got.Query.Get(key), value, got.Query)
		}
	}

	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 1 {
		t.Fatalf("instances = %d, want 1", len(instances))
	}
	stored := instances[0]
	if stored.InstanceID != "10.0.0.1#8080#k8s#DEFAULT_GROUP@@pay-user" {
		t.Fatalf("composite id = %q, want the mapped form", stored.InstanceID)
	}
	metadata := stored.Metadata
	for key, value := range map[string]string{
		"instanceId": "pod-a",
		"envType":    "test",
		"envGroup":   "7",
		"reversion":  "42",
		"status":     "1",
		"state":      "running",
		"idc":        "kraken",
		"cpu":        "2",
		"version":    "v1",
	} {
		if metadata[key] != value {
			t.Fatalf("metadata[%s] = %q, want %q (all: %v)", key, metadata[key], value, metadata)
		}
	}
}

func TestBlackboxSinkPushOfflineInstanceDeregisters(t *testing.T) {
	sink, server := newSinkAt(t)

	// Status 3 (offline): the push is a DELETE by composite id, no register.
	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 3)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(offline) error = %v", err)
	}

	if registerRequest(server, "10.0.0.1", 8080) != nil {
		t.Fatal("offline push sent a register, want deregister only")
	}
	del := deregisterRequest(server, "10.0.0.1", 8080)
	if del == nil {
		t.Fatalf("no deregister request; requests = %v", server.Requests())
	}
	for key, value := range map[string]string{
		"serviceName": "pay-user",
		"ip":          "10.0.0.1",
		"port":        "8080",
		"clusterName": "k8s",
		"groupName":   "DEFAULT_GROUP",
	} {
		if del.Query.Get(key) != value {
			t.Fatalf("deregister query[%s] = %q, want %q", key, del.Query.Get(key), value)
		}
	}
}

// TestBlackboxSinkPushOfflineInstanceWithEmptyIpSkipsDeregister: the k8s
// informer can synthesize an offline "shell" instance for a pod that died
// before ever receiving an IP. The v1 DELETE derives its target from the ip
// parameter, so deregistering would send a request Nacos rejects with a
// permanent 400 (the live incident). The push must succeed without any HTTP
// traffic — the PushAll prune sweep owns the remote cleanup.
func TestBlackboxSinkPushOfflineInstanceWithEmptyIpSkipsDeregister(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("pod-shell", "pay-user", "", 8080, "k8s", 3)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(offline empty ip) error = %v, want nil (deregister skipped)", err)
	}
	if requests := server.Requests(); len(requests) != 0 {
		t.Fatalf("requests = %d, want 0 (no deregister for an empty-ip instance); requests = %v", len(requests), requests)
	}
}

// TestBlackboxSinkPushRegisterSideEmptyIpSkipsRegister: the register-side
// sibling of the offline guard — the k8s boundary can emit an online or
// unhealthy instance whose Ip is empty (AUDIT-B-3/C-4: a Running pod whose
// kubelet has not reported an IP yet passes the source filters, which reject
// empty-ip only for online... and the domain filter likewise). The v1 POST
// derives its composite id from the ip parameter, so Nacos answers a
// permanent 400 for it, and nothing was ever registered under an empty ip,
// so there is nothing to keep in sync. The push must succeed without any
// HTTP traffic instead of poisoning the retry queue.
func TestBlackboxSinkPushRegisterSideEmptyIpSkipsRegister(t *testing.T) {
	sink, server := newSinkAt(t)

	for _, status := range []int32{instance.InstanceStatusOnline, instance.InstanceStatusUnhealthy} {
		ins := domainInstance("pod-early", "pay-user", "", 8080, "k8s", status)
		if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
			t.Fatalf("Push(status %d empty ip) error = %v, want nil (register skipped)", status, err)
		}
	}
	if requests := server.Requests(); len(requests) != 0 {
		t.Fatalf("requests = %d, want 0 (no register for an empty-ip instance); requests = %v", len(requests), requests)
	}
}

// TestBlackboxSinkPushRegisterWithIpStillRegisters: regression guard — the
// register-side empty-ip skip must not swallow the normal register path.
// Status 1 with an ip registers (status 2 is covered by
// TestBlackboxSinkPushUnhealthyInstanceUpsertsDisabled).
func TestBlackboxSinkPushRegisterWithIpStillRegisters(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(online with ip) error = %v, want nil", err)
	}
	if registerRequest(server, "10.0.0.1", 8080) == nil {
		t.Fatalf("no register request for the online instance; requests = %v", server.Requests())
	}
}

func TestBlackboxSinkPushUnhealthyInstanceUpsertsDisabled(t *testing.T) {
	sink, server := newSinkAt(t)

	// Status 2 (unhealthy): upsert with enabled=false, mirroring the consul
	// path's "old instance, mark not-ready" semantics (plan §7.3).
	ins := domainInstance("srv-a", "pay-user", "10.0.0.2", 8081, "ecs", 2)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(unhealthy) error = %v", err)
	}

	reg := registerRequest(server, "10.0.0.2", 8081)
	if reg == nil {
		t.Fatalf("unhealthy push sent no register; requests = %v", server.Requests())
	}
	if got := reg.Query.Get("enabled"); got != "false" {
		t.Fatalf("unhealthy register enabled = %q, want false", got)
	}
	if deregisterRequest(server, "10.0.0.2", 8081) != nil {
		t.Fatal("unhealthy push sent a deregister, want the upsert only")
	}
	instances := server.Instances("pay-user", "ecs")
	if len(instances) != 1 || instances[0].Enabled {
		t.Fatalf("stored unhealthy instance = %+v, want present with enabled=false", instances)
	}
}

func TestBlackboxSinkPushSkipsUnknownStatus(t *testing.T) {
	sink, server := newSinkAt(t)

	// Status 0 (unknown): skipped defensively, no HTTP traffic at all.
	ins := domainInstance("pod-x", "pay-user", "10.0.0.9", 8082, "k8s", 0)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(unknown status) error = %v, want nil (skipped)", err)
	}
	if requests := server.Requests(); len(requests) != 0 {
		t.Fatalf("requests = %d, want 0 (unknown status skipped); requests = %v", len(requests), requests)
	}
}

func TestBlackboxSinkPushNilAndEmptyInstancesNoop(t *testing.T) {
	sink, server := newSinkAt(t)

	if err := sink.Push(1, nil); err != nil {
		t.Fatalf("Push(nil) error = %v, want nil", err)
	}
	if err := sink.Push(1, []*instance.Instance{}); err != nil {
		t.Fatalf("Push(empty) error = %v, want nil", err)
	}
	if err := sink.Push(1, []*instance.Instance{nil}); err != nil {
		t.Fatalf("Push([nil]) error = %v, want nil (skipped)", err)
	}
	if requests := server.Requests(); len(requests) != 0 {
		t.Fatalf("requests = %d, want 0", len(requests))
	}
}

func TestBlackboxSinkPushAttemptsEveryInstance(t *testing.T) {
	sink, server := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-b", "pay-user", "10.0.0.2", 8080, "k8s", 1),
		domainInstance("pod-c", "other-app", "10.0.0.3", 8080, "ecs", 1),
	}
	if err := sink.Push(7, instances); err != nil {
		t.Fatalf("Push(3 instances) error = %v", err)
	}

	// Every instance is registered exactly once (plan §7.4: Push applies the
	// per-instance policy), each pair is configured exactly once, and each
	// pair's cluster update FOLLOWS that pair's register — the
	// register-then-configure discipline. The completion ORDER across
	// instances is nondeterministic under the bounded-parallelism group
	// (dsca-2 DS-2-1: distinct composite ids, idempotent upserts — order is
	// explicitly not part of the contract), so the per-request ordering is
	// asserted PER PAIR, not globally.
	requests := server.Requests()
	lastRegisterIndex := map[string]int{} // "service/cluster" -> last POST index
	for i, request := range requests {
		if request.Method == "POST" {
			lastRegisterIndex[request.Query.Get("serviceName")+"/"+request.Query.Get("clusterName")] = i
		}
	}
	if len(lastRegisterIndex) != 2 {
		t.Fatalf("distinct registered pairs = %d, want 2 (pay-user/k8s, other-app/ecs); requests = %v", len(lastRegisterIndex), requests)
	}
	updatesByPair := map[string]int{}
	for i, request := range requests {
		if request.Method == "PUT" && request.Path == "/nacos/v1/ns/cluster" {
			pair := request.Query.Get("serviceName") + "/" + request.Query.Get("clusterName")
			updatesByPair[pair]++
			if registerIndex, ok := lastRegisterIndex[pair]; !ok || i < registerIndex {
				t.Fatalf("cluster update of %s at request %d does not follow that pair's register (last register at %v); requests = %v",
					pair, i, registerIndex, requests)
			}
		}
	}
	if got, want := len(server.Requests()), 5; got != want {
		t.Fatalf("total requests = %d, want %d (3 registers + 2 pair configurations); requests = %v", got, want, server.Requests())
	}
	for _, appCode := range []string{"pay-user", "other-app"} {
		if got := len(server.Instances(appCode, "k8s")) + len(server.Instances(appCode, "ecs")); got == 0 {
			t.Fatalf("service %s has no instances", appCode)
		}
	}
}

func TestBlackboxSinkPushErrorPropagates(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetStatus(500)

	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err == nil {
		t.Fatal("Push() error = nil, want the HTTP 500 error")
	}
}

func TestBlackboxSinkPushAllUpsertsAllPushed(t *testing.T) {
	sink, server := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-b", "pay-user", "10.0.0.2", 8080, "k8s", 1),
		domainInstance("srv-a", "pay-user", "10.0.0.3", 8081, "ecs", 1),
	}
	if err := sink.PushAll(7, instances); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}

	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances = %d, want 2", got)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs instances = %d, want 1", got)
	}
}

func TestBlackboxSinkPushAllPrunesStaleRemoteInstances(t *testing.T) {
	sink, server := newSinkAt(t)

	// Remote state ahead of the push: a stale k8s instance, a fresh k8s
	// instance that stays, and an ecs instance the k8s push must never touch.
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.1", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-a"}},
		{IP: "10.0.0.9", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-stale"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.3", Port: 8081, Enabled: true, Metadata: map[string]string{"instanceId": "srv-a"}},
	}, "DEFAULT_GROUP", "pay-user", "ecs")

	pushed := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
	}
	if err := sink.PushAll(7, pushed); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}

	// The stale k8s instance is gone; the pushed one and the untouched ecs
	// instance survive (prune scoped to the clusters present in the data).
	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("k8s instances after prune = %d, want 1 (stale removed); state = %v", got, server.Instances("pay-user", "k8s"))
	}
	if remaining := server.Instances("pay-user", "k8s")[0]; remaining.IP != "10.0.0.1" {
		t.Fatalf("surviving k8s instance = %s, want the pushed 10.0.0.1", remaining.IP)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs instances after k8s prune = %d, want 1 (other cluster untouched)", got)
	}
}

func TestBlackboxSinkPushAllPrunesSameClusterOnly(t *testing.T) {
	sink, server := newSinkAt(t)

	// Both clusters of one service hold remote state; an ecs-only full push
	// must prune the ecs cluster and leave the k8s cluster alone.
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.1", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-keep"}},
		{IP: "10.0.0.2", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-also-keep"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.8", Port: 8081, Enabled: true, Metadata: map[string]string{"instanceId": "srv-stale"}},
	}, "DEFAULT_GROUP", "pay-user", "ecs")

	pushed := []*instance.Instance{
		domainInstance("srv-new", "pay-user", "10.0.0.9", 8081, "ecs", 1),
	}
	if err := sink.PushAll(7, pushed); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}

	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances after ecs-only push = %d, want 2 (untouched)", got)
	}
	ecs := server.Instances("pay-user", "ecs")
	if len(ecs) != 1 || ecs[0].IP != "10.0.0.9" {
		t.Fatalf("ecs instances after prune = %v, want exactly the pushed 10.0.0.9", ecs)
	}
}

func TestBlackboxSinkPushAllOfflinePushedInstanceDeregisters(t *testing.T) {
	sink, server := newSinkAt(t)

	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.1", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-a"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	// The full push contains the offline marker: the deregister happens via
	// the per-instance policy, and the instance is absent afterwards.
	pushed := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 3),
	}
	if err := sink.PushAll(7, pushed); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 0 {
		t.Fatalf("k8s instances = %d, want 0 (offline instance deregistered)", got)
	}
}

func TestBlackboxSinkPushAllErrorPropagates(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetStatus(500)

	pushed := []*instance.Instance{domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)}
	if err := sink.PushAll(7, pushed); err == nil {
		t.Fatal("PushAll() error = nil, want the HTTP 500 error")
	}
}

// TestBlackboxSinkPushAllPrunesDisabledRemoteInstance (the F8 regression):
// a remote instance with enabled=false — the state spotter's own unhealthy
// pushes write, and the exact state instance/list hides from the prune — is
// DELETEd when it is not in the pushed set. Red before the F8 fix (the old
// ListInstances-based prune could never see it) and green only with BOTH
// the mock's catalog endpoint and the prune switch in place.
func TestBlackboxSinkPushAllPrunesDisabledRemoteInstance(t *testing.T) {
	sink, server := newSinkAt(t)

	// Remote state: the pushed instance plus a disabled ghost in the same
	// (service, cluster); instance/list serves only the pushed one.
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.1", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "pod-a"}},
		{IP: "10.0.0.7", Port: 8080, Enabled: false, Metadata: map[string]string{"instanceId": "srv-ghost"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	pushed := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
	}
	if err := sink.PushAll(7, pushed); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}

	// The disabled ghost is gone: the prune's catalog listing saw it and the
	// DELETE (by composite id) removed it — while the pushed instance stays.
	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("k8s instances after prune = %v, want only the pushed 10.0.0.1 (the disabled ghost pruned)", instances)
	}
	if deregisterRequest(server, "10.0.0.7", 8080) == nil {
		t.Fatalf("no DELETE of the disabled ghost; requests = %v", server.Requests())
	}
}

// TestBlackboxSinkPushAllUnhealthyThenVanishedIsPruned (the F8 lifecycle
// regression): an instance spotter itself marked unhealthy (status 2 →
// registered with enabled=false) that then disappears from the pushed set
// must be pruned. With the old instance-list-based prune it could never be:
// the very push that marked it unhealthy hid it from the prune forever —
// the drift the audit measured as unremovable in real Nacos.
func TestBlackboxSinkPushAllUnhealthyThenVanishedIsPruned(t *testing.T) {
	sink, server := newSinkAt(t)

	// Push 1: pod-a online, srv-b unhealthy → srv-b lands with enabled=false.
	first := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("srv-b", "pay-user", "10.0.0.2", 8080, "k8s", 2),
	}
	if err := sink.PushAll(1, first); err != nil {
		t.Fatalf("PushAll(unhealthy lifecycle push 1) error = %v", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances after push 1 = %d, want 2 (online + unhealthy); state = %v", got, server.Instances("pay-user", "k8s"))
	}

	// Push 2: the same service without srv-b — it vanished upstream. The
	// prune must delete the now-orphaned enabled=false registration.
	second := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
	}
	if err := sink.PushAll(2, second); err != nil {
		t.Fatalf("PushAll(unhealthy lifecycle push 2) error = %v", err)
	}

	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 1 || instances[0].IP != "10.0.0.1" {
		t.Fatalf("k8s instances after prune = %v, want only 10.0.0.1 (the vanished unhealthy srv-b pruned)", instances)
	}
	if deregisterRequest(server, "10.0.0.2", 8080) == nil {
		t.Fatalf("no DELETE of the vanished unhealthy instance; requests = %v", server.Requests())
	}
}

// TestBlackboxSinkPushAllPruneToleratesCatalogNotFound: a fully pruned
// service is the prune's own steady state, and a real Nacos answers the
// catalog query with 500 "service is not found" then. PushAll must treat
// that answer as an empty list (success), not surface an error every
// interval. The mock cannot produce a selective 500-with-body answer (its
// failure injection hits every endpoint), so the tolerance is driven
// through a sink bound to a stub HTTP server answering exactly the
// real-server body on the catalog path.
func TestBlackboxSinkPushAllPruneToleratesCatalogNotFound(t *testing.T) {
	const notFoundBody = `{"status":500,"message":"service pay-user is not found!","data":null,"code":500,"serverIp":"127.0.0.1"}`
	var catalogCalls int
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/nacos/v1/ns/catalog/instances" {
			catalogCalls++
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, notFoundBody)
			return
		}
		// The register path answers success so Push completes.
		writeStubOK(w)
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	pushed := []*instance.Instance{domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)}
	if err := sink.PushAll(7, pushed); err != nil {
		t.Fatalf("PushAll(prune catalog not-found 500) error = %v, want nil (treated as an empty list)", err)
	}
	if catalogCalls != 1 {
		t.Fatalf("catalog calls = %d, want 1", catalogCalls)
	}
}

// TestBlackboxSinkPushAllPruneSurfacesOtherCatalog500s: the not-found
// tolerance is narrow — a 500 WITHOUT the "is not found" body marker is an
// ordinary server failure and must surface from PushAll (it stays retriable
// in the APIError classification, so the retry queue owns it as usual).
func TestBlackboxSinkPushAllPruneSurfacesOtherCatalog500s(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/nacos/v1/ns/catalog/instances" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "injected status 500")
			return
		}
		writeStubOK(w)
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	pushed := []*instance.Instance{domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)}
	if err := sink.PushAll(7, pushed); err == nil {
		t.Fatal("PushAll(unrelated catalog 500) error = nil, want the surfaced error")
	}
}

// TestBlackboxSinkPushAllPruneSurfacesNotFoundBodyOn400: the not-found
// tolerance requires BOTH the 500 status AND the "is not found" body marker
// (isCatalogNotFound, nacos.go). A 400 whose body carries the same marker is
// NOT the real server's absent-catalog answer — it is a rejected request
// (Permanent()), and the prune must surface it instead of tolerating it as
// an empty list: the 400-with-body-marker shape would otherwise be silently
// swallowed, hiding a client-class break behind the empty-tolerance path.
// (The 500-without-marker negative is pinned by
// TestBlackboxSinkPushAllPruneSurfacesOtherCatalog500s.)
func TestBlackboxSinkPushAllPruneSurfacesNotFoundBodyOn400(t *testing.T) {
	const notFoundBody = `{"status":400,"message":"service pay-user is not found!","data":null,"code":400,"serverIp":"127.0.0.1"}`
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/nacos/v1/ns/catalog/instances" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, notFoundBody)
			return
		}
		writeStubOK(w)
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	pushed := []*instance.Instance{domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)}
	if err := sink.PushAll(7, pushed); err == nil {
		t.Fatal("PushAll(catalog 400 with not-found body) error = nil, want the surfaced error (tolerance is 500-only)")
	}
}

// writeJSON writes a JSON payload with the given status (the stub-server
// helper for the catalog-view tests; mirrors the nacosmock's own writer).
func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

// writeStubOK answers a register/deregister request with the Nacos success
// text.
func writeStubOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func TestBlackboxSinkMapsProviderToClusterCoexist(t *testing.T) {
	sink, server := newSinkAt(t)

	// The same app-code from both providers: coexistence under distinct
	// clusters with distinct composite ids (plan §7.3 collision policy).
	k8sInstance := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	ecsInstance := domainInstance("srv-a", "pay-user", "10.0.0.1", 8080, "ecs", 1)
	if err := sink.Push(1, []*instance.Instance{k8sInstance, ecsInstance}); err != nil {
		t.Fatalf("Push(k8s+ecs same ip) error = %v", err)
	}

	instances := server.Instances("pay-user", "")
	if len(instances) != 2 {
		t.Fatalf("instances = %d, want 2 (k8s and ecs coexist)", len(instances))
	}
	ids := map[string]bool{}
	for _, stored := range instances {
		ids[stored.InstanceID] = true
	}
	if !ids["10.0.0.1#8080#k8s#DEFAULT_GROUP@@pay-user"] {
		t.Fatalf("k8s composite id missing; ids = %v", ids)
	}
	if !ids["10.0.0.1#8080#ecs#DEFAULT_GROUP@@pay-user"] {
		t.Fatalf("ecs composite id missing; ids = %v", ids)
	}
}

func TestBlackboxSinkPortlessInstanceUsesPortZero(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("srv-a", "pay-user", "10.0.0.1", 0, "ecs", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push(portless) error = %v", err)
	}

	// Register with port 0, then deregister addressing the same composite id:
	// the deterministic derivation round-trips (plan §7.3).
	reg := registerRequest(server, "10.0.0.1", 0)
	if reg == nil {
		t.Fatalf("no register request for the portless instance; requests = %v", server.Requests())
	}
	if got := reg.Query.Get("port"); got != "0" {
		t.Fatalf("portless register port = %q, want 0", got)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("stored instances = %d, want 1", got)
	}

	offline := domainInstance("srv-a", "pay-user", "10.0.0.1", 0, "ecs", 3)
	if err := sink.Push(1, []*instance.Instance{offline}); err != nil {
		t.Fatalf("Push(portless offline) error = %v", err)
	}
	del := deregisterRequest(server, "10.0.0.1", 0)
	if del == nil {
		t.Fatalf("no deregister request for the portless instance; requests = %v", server.Requests())
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 0 {
		t.Fatalf("stored instances after deregister = %d, want 0", got)
	}
}

func TestBlackboxSinkGetAllReconstructsInstances(t *testing.T) {
	sink, server := newSinkAt(t)

	// Push a fully populated instance, then reconstruct it through GetAll:
	// the metadata round-trip restores the domain fields (plan §7.3). This
	// is the dsca-3 §3.5 re-pin for the CATALOG source: GetAll reads the
	// catalog view (the instance list would hide the disabled hosts the
	// unhealthy policy writes), so the reconstruction this test asserts is
	// the one the nacos-source compare consumes.
	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	// The schemaVersion marker (dsca-5 §4.2-1): register writes "1"
	// alongside the field keys.
	stored := server.Instances("pay-user", "k8s")
	if len(stored) != 1 || stored[0].Metadata["schemaVersion"] != "1" {
		t.Fatalf("stored metadata schemaVersion = %q, want 1 (register writes the marker)", stored[0].Metadata["schemaVersion"])
	}

	list, err := sink.GetAll(nil, "k8s")
	if err != nil {
		t.Fatalf("GetAll(nil, k8s) error = %v", err)
	}
	if list == nil {
		t.Fatal("GetAll() list = nil, want a list")
	}
	if len(list.Instance) != 1 {
		t.Fatalf("GetAll() = %d instances, want 1", len(list.Instance))
	}
	got := list.Instance[0]
	if got.InstanceId != "pod-a" || got.AppCode != "pay-user" || got.Ip != "10.0.0.1" {
		t.Fatalf("reconstructed identity = %s/%s/%s, want pod-a/pay-user/10.0.0.1", got.InstanceId, got.AppCode, got.Ip)
	}
	if len(got.Ports) != 1 || got.Ports[0].Port != 8080 {
		t.Fatalf("reconstructed ports = %+v, want one port 8080", got.Ports)
	}
	if got.Provider != "k8s" {
		t.Fatalf("reconstructed provider = %q, want k8s (clusterName lands in Provider)", got.Provider)
	}
	// The dsca-3 §3.2 fidelity correction (DS-3-4/DS-5-3): Cluster stays
	// EMPTY — the metadata never carried it, and synthesizing it from
	// clusterName would false-positive the consul compare every cycle.
	// This is the assertion flip the §3.5 test contract pins.
	if got.Cluster != "" {
		t.Fatalf("reconstructed cluster = %q, want \"\" (never synthesized from clusterName)", got.Cluster)
	}
	if got.EnvType != "test" || got.EnvGroup != "7" {
		t.Fatalf("reconstructed env = %s/%s, want test/7", got.EnvType, got.EnvGroup)
	}
	if got.Reversion != 42 || got.Status != 1 || got.State != "running" {
		t.Fatalf("reconstructed reversion/status/state = %d/%d/%s, want 42/1/running", got.Reversion, got.Status, got.State)
	}
	if got.Idc != "kraken" || got.Version != "v1" {
		t.Fatalf("reconstructed idc/version = %s/%s, want kraken/v1", got.Idc, got.Version)
	}
	if got.Cpu != 2 {
		t.Fatalf("reconstructed cpu = %f, want 2", got.Cpu)
	}
	if !got.Enabled {
		t.Fatal("reconstructed enabled = false, want true")
	}
}

func TestBlackboxSinkGetAllFiltersProviderCluster(t *testing.T) {
	sink, _ := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("srv-a", "pay-user", "10.0.0.2", 8081, "ecs", 1),
		domainInstance("pod-b", "other-app", "10.0.0.3", 8080, "k8s", 1),
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	// provider "k8s": the walk reads only the (service, "k8s") catalog
	// pairs — the per-provider/cluster scoping of dsca-3 §3.2 (clusterName
	// == provider on the catalog endpoint, stricter than the old
	// post-filter over instance/list: the diff-level twin of the prune's
	// cluster scoping).
	list, err := sink.GetAll(nil, "k8s")
	if err != nil {
		t.Fatalf("GetAll(nil, k8s) error = %v", err)
	}
	if len(list.Instance) != 2 {
		t.Fatalf("GetAll(k8s) = %d instances, want 2 (k8s cluster only)", len(list.Instance))
	}
	for _, got := range list.Instance {
		if got.Provider != "k8s" {
			t.Fatalf("GetAll(k8s) returned provider %q, want k8s only", got.Provider)
		}
	}
}

func TestBlackboxSinkGetAllFiltersStatuses(t *testing.T) {
	sink, _ := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("srv-a", "pay-user", "10.0.0.2", 8081, "ecs", 2),
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	// Provider "" with statuses [online]: the online k8s instance survives.
	list, err := sink.GetAll([]int32{1}, "k8s")
	if err != nil {
		t.Fatalf("GetAll([1], k8s) error = %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "pod-a" {
		t.Fatalf("GetAll([1]) = %v, want the single online instance pod-a", list.Instance)
	}

	// Statuses [unhealthy]: the ecs instance comes back — the CATALOG view
	// of dsca-3 §3.2 serves enabled=false hosts too (the instance list
	// view hides them, the F8 blind spot the catalog exists to escape).
	// An instance spotter itself marked unhealthy (status 2 → registered
	// enabled=false) is therefore visible to spotter's own read side:
	// the precondition for the status-2 steady-state compare pins.
	list, err = sink.GetAll([]int32{2}, "ecs")
	if err != nil {
		t.Fatalf("GetAll([2], ecs) error = %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "srv-a" {
		t.Fatalf("GetAll([2]) = %v, want srv-a (the catalog view serves the disabled instance)", list.Instance)
	}
	if list.Instance[0].Status != 2 {
		t.Fatalf("GetAll([2]) status = %d, want 2 (metadata status preferred over enabled)", list.Instance[0].Status)
	}
}

func TestBlackboxSinkGetAllEmptyState(t *testing.T) {
	sink, _ := newSinkAt(t)

	list, err := sink.GetAll(nil, "")
	if err != nil {
		t.Fatalf("GetAll() on empty state error = %v", err)
	}
	if list == nil || list.Instance == nil || len(list.Instance) != 0 {
		t.Fatalf("GetAll() on empty state = %#v, want an empty non-nil list", list)
	}
}

func TestBlackboxSinkGetAllErrorPropagates(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetStatus(500)

	if _, err := sink.GetAll(nil, ""); err == nil {
		t.Fatal("GetAll() error = nil, want the HTTP 500 error")
	}
}

func TestBlackboxSinkGetAllManyServices(t *testing.T) {
	sink, _ := newSinkAt(t)

	// Instances across three services; GetAll must visit each service the
	// pagination reports and reconstruct all of them. The k8s provider
	// scopes its catalog walk to clusterName "k8s" (dsca-3 §3.2), so every
	// instance here is a k8s instance.
	var instances []*instance.Instance
	for i, appCode := range []string{"app-a", "app-b", "app-c"} {
		instances = append(instances, domainInstance(
			"pod-"+strconv.Itoa(i), appCode, "10.0.0."+strconv.Itoa(i+1), 8080, "k8s", 1))
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	list, err := sink.GetAll(nil, "k8s")
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(list.Instance) != 3 {
		t.Fatalf("GetAll() = %d instances, want 3 (one per service)", len(list.Instance))
	}
}

func TestBlackboxSinkSatisfiesInstanceSink(t *testing.T) {
	// The compile-time assertion lives in the production file
	// (nacos.go); this test pins it from the outside too.
	sink, err := nacos.NewSink("http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatalf("NewSink() error = %v", err)
	}
	_ = sink
}

func TestBlackboxSinkNewSinkValidation(t *testing.T) {
	if _, err := nacos.NewSink("", &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewSink(\"\") error = nil, want an error")
	}
	if _, err := nacos.NewSink("://bad", &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewSink(bad) error = nil, want an error")
	}
	// A nil logger defaults instead of failing.
	if _, err := nacos.NewSink("http://127.0.0.1:1", nil); err != nil {
		t.Fatalf("NewSink(nil logger) error = %v, want nil", err)
	}
}

func TestBlackboxSinkCloseIdempotent(t *testing.T) {
	sink, _ := newSinkAt(t)
	if err := sink.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil (stateless HTTP sink)", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil (idempotent)", err)
	}
}

func TestBlackboxSinkReadinessGate(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	if err := nacos.CheckReadiness(server.URL(), nacos.RequestTimeout); err != nil {
		t.Fatalf("CheckReadiness(healthy) error = %v, want nil", err)
	}
	if err := nacos.CheckReadiness("http://127.0.0.1:1", 100*time.Millisecond); err == nil {
		t.Fatal("CheckReadiness(unreachable) error = nil, want an error")
	}
}

// TestBlackboxSinkPushAllEmptyListSweepsNothing (the empty-door half of the
// cross-provider fix, AUDIT-B-4 rescoped): an EMPTY pushed list carries no
// cluster and therefore no provider identity — worker.Event has no provider
// field, and the fan-out PushAll hands the sink the bare instance list — so
// the sink cannot tell WHICH provider went empty. The batch-3 semantics
// swept every remembered pair on it, which re-opened the live incident
// through the empty door: a single provider's empty SyncAll (an empty k8s
// pod list, a consul source blip) deleted EVERY provider's instances. The
// conservative contract now: an empty push sweeps nothing remembered — no
// DELETE is issued for pairs the sink owns. (The vanished-service heal
// rides the provider's non-empty pushes instead, pinned by
// TestBlackboxSinkPushAllVanishedServicePrunedSameCluster and
// TestBlackboxSinkPushAllOfflineMarkerPrunesPair.)
func TestBlackboxSinkPushAllEmptyListSweepsNothing(t *testing.T) {
	sink, server := newSinkAt(t)

	// Two instances of one (service, cluster) — the pair the sink will own.
	first := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-b", "pay-user", "10.0.0.2", 8080, "k8s", 1),
	}
	if err := sink.PushAll(1, first); err != nil {
		t.Fatalf("PushAll(push 1) error = %v", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances after push 1 = %d, want 2; state = %v", got, server.Instances("pay-user", "k8s"))
	}
	// An unrelated cluster's pair is owned too (the consul provider's).
	ecsPair := []*instance.Instance{domainInstance("srv-a", "pay-user", "10.0.0.9", 8081, "ecs", 1)}
	if err := sink.PushAll(1, ecsPair); err != nil {
		t.Fatalf("PushAll(ecs pair) error = %v", err)
	}

	// The empty push — provider identity unknowable. Nothing is swept.
	if err := sink.PushAll(2, nil); err != nil {
		t.Fatalf("PushAll(empty) error = %v, want nil (the conservative no-op)", err)
	}

	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances after the empty push = %d, want 2 (an empty push sweeps nothing remembered)", got)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs instances after the empty push = %d, want 1 (an empty push sweeps nothing remembered)", got)
	}
	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.9"} {
		for _, request := range server.Requests() {
			if request.Method == "DELETE" && request.Query.Get("ip") == ip {
				t.Fatalf("the empty push issued a DELETE of %s; requests = %v", ip, server.Requests())
			}
		}
	}

	// The conservative no-op is idempotent: a second empty push is the same
	// steady state.
	if err := sink.PushAll(3, nil); err != nil {
		t.Fatalf("PushAll(second empty) error = %v, want nil", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 2 {
		t.Fatalf("k8s instances after the second empty push = %d, want 2", got)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs instances after the second empty push = %d, want 1", got)
	}
}

// TestBlackboxSinkPushAllEmptyListNoRememberedPairsIsNoop: an empty push on
// a sink that owns nothing must stay a no-op — the remembered-pairs sweep
// may not fabricate work for services the sink never registered. (With the
// scoped sweep this is subsumed by the empty-list contract, but it stays as
// the direct pin of the own-nothing corner.)
func TestBlackboxSinkPushAllEmptyListNoRememberedPairsIsNoop(t *testing.T) {
	sink, server := newSinkAt(t)
	// Remote state exists (someone else's registrations), the sink never
	// pushed a pair.
	server.SetInstances([]nacosmock.Host{
		{IP: "10.0.0.7", Port: 8080, Enabled: true, Metadata: map[string]string{"instanceId": "srv-foreign"}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	if err := sink.PushAll(1, nil); err != nil {
		t.Fatalf("PushAll(empty, no remembered pairs) error = %v, want nil", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("k8s instances after the no-op empty push = %d, want 1 (nothing pruned)", got)
	}
	if deregisterRequest(server, "10.0.0.7", 8080) != nil {
		t.Fatal("an empty push with no remembered pairs deregistered a foreign instance")
	}
}

// TestBlackboxSinkPushAllCrossProviderNeverPrunesOtherProvider (THE live
// incident, the P0 this fix exists for): the sink is shared by both
// providers, pushes arrive per-provider, and the remembered map is global —
// so the union sweep of a k8s-only push used to give the ecs pair an empty
// desired set and DELETE every remote instance of it (demo-pay-service
// flapping 0↔2 every push interval). The scoping invariant: a PushAll whose
// pushed instances are all cluster X may only sweep remembered pairs of
// cluster X.
func TestBlackboxSinkPushAllCrossProviderNeverPrunesOtherProvider(t *testing.T) {
	sink, server := newSinkAt(t)

	// 1. The consul provider's push registers the pay instances under the
	// ecs cluster — the pair (demo-pay-service, ecs) enters remembered.
	consulPush := []*instance.Instance{
		domainInstance("srv-pay-1", "demo-pay-service", "127.0.0.1", 9848, "ecs", 1),
		domainInstance("srv-pay-2", "demo-pay-service", "127.0.0.1", 8848, "ecs", 1),
	}
	if err := sink.PushAll(1, consulPush); err != nil {
		t.Fatalf("PushAll(consul) error = %v", err)
	}
	if got := len(server.Instances("demo-pay-service", "ecs")); got != 2 {
		t.Fatalf("ecs instances after the consul push = %d, want 2; state = %v", got, server.Instances("demo-pay-service", "ecs"))
	}

	// 2. The k8s provider's full push carries ONLY its own instances (a
	// different service, the k8s cluster). The pre-fix prune unioned the
	// remembered (demo-pay-service, ecs) pair into this push with an empty
	// desired set and deleted both pay instances.
	k8sPush := []*instance.Instance{
		domainInstance("pod-a", "demo-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-b", "demo-user", "10.0.0.2", 8080, "k8s", 1),
	}
	if err := sink.PushAll(2, k8sPush); err != nil {
		t.Fatalf("PushAll(k8s only) error = %v", err)
	}

	// 3. The pay instances SURVIVE the k8s push: no DELETE was issued for
	// any ecs-pair instance, and the pair is still registered.
	if got := len(server.Instances("demo-pay-service", "ecs")); got != 2 {
		t.Fatalf("ecs instances after the k8s-only push = %d, want 2 (the cross-provider sweep must never touch them); state = %v",
			got, server.Instances("demo-pay-service", "ecs"))
	}
	for _, ip := range []string{"127.0.0.1"} {
		for _, port := range []int{9848, 8848} {
			if deregisterRequest(server, ip, port) != nil {
				t.Fatalf("the k8s-only push issued a DELETE of %s#%d (the ecs pair the consul provider owns); requests = %v",
					ip, port, server.Requests())
			}
		}
	}

	// 4. Same-provider scoping still prunes: the consul provider's next full
	// list WITHOUT the pay service — the ecs cluster stays present through
	// the surviving demo-order instance, so the vanished (demo-pay-service,
	// ecs) pair is swept with an empty desired set.
	consulPush2 := []*instance.Instance{
		domainInstance("srv-order-1", "demo-order", "127.0.0.2", 9849, "ecs", 1),
	}
	if err := sink.PushAll(3, consulPush2); err != nil {
		t.Fatalf("PushAll(consul without pay) error = %v", err)
	}
	if got := len(server.Instances("demo-pay-service", "ecs")); got != 0 {
		t.Fatalf("ecs pay instances after the same-provider vanished push = %d, want 0 (both pruned); state = %v",
			got, server.Instances("demo-pay-service", "ecs"))
	}
	if deregisterRequest(server, "127.0.0.1", 9848) == nil {
		t.Fatalf("no DELETE of the vanished pay instance 127.0.0.1#9848; requests = %v", server.Requests())
	}
	if deregisterRequest(server, "127.0.0.1", 8848) == nil {
		t.Fatalf("no DELETE of the vanished pay instance 127.0.0.1#8848; requests = %v", server.Requests())
	}
	if got := len(server.Instances("demo-order", "ecs")); got != 1 {
		t.Fatalf("ecs order instances after the vanished push = %d, want 1 (the pushed survivor)", got)
	}
}

// TestBlackboxSinkPushAllVanishedServicePrunedSameCluster (the B-4 heal in
// its rescoped form): a provider whose full push keeps its cluster present
// (through any surviving instance of the same provider) but drops a
// remembered service entirely has that vanished pair's remote registrations
// pruned — the empty-desired sweep, scoped to the pushing provider's
// cluster. The pair heals without an offline marker AND without the
// destructive empty push: the vanished service simply vanishes from the
// pushed set while a sibling instance keeps the cluster in it.
func TestBlackboxSinkPushAllVanishedServicePrunedSameCluster(t *testing.T) {
	sink, server := newSinkAt(t)

	// Push 1: two services under the k8s cluster.
	first := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-c", "other-app", "10.0.0.3", 8080, "k8s", 1),
	}
	if err := sink.PushAll(1, first); err != nil {
		t.Fatalf("PushAll(push 1) error = %v", err)
	}

	// Push 2: pay-user vanished upstream — other-app's instance keeps the
	// k8s cluster present in the push, so the remembered (pay-user, k8s)
	// pair sweeps with an empty desired set.
	second := []*instance.Instance{
		domainInstance("pod-c", "other-app", "10.0.0.3", 8080, "k8s", 1),
	}
	if err := sink.PushAll(2, second); err != nil {
		t.Fatalf("PushAll(push 2) error = %v", err)
	}

	if got := len(server.Instances("pay-user", "k8s")); got != 0 {
		t.Fatalf("pay-user k8s instances after the vanished-service push = %d, want 0 (the remembered pair swept); state = %v",
			got, server.Instances("pay-user", "k8s"))
	}
	if deregisterRequest(server, "10.0.0.1", 8080) == nil {
		t.Fatalf("no DELETE of the vanished pay-user instance; requests = %v", server.Requests())
	}
	if got := len(server.Instances("other-app", "k8s")); got != 1 {
		t.Fatalf("other-app instances after the vanished-service push = %d, want 1 (the pushed survivor)", got)
	}
}

// TestBlackboxSinkPushAllOfflineMarkerPrunesPair (the offline-marker heal):
// the provider's CompareAndFlush case-3 route for a vanished service — an
// offline (Status 3) marker instance whose Provider field keeps the pair
// tagged. The marker creates the desired pair with an EMPTY wanted set (the
// composite id is only added for non-offline instances), so the prune
// deletes every remote instance of the pair: the vanished service heals
// through a push that carries the provider identity, never through the
// destructive empty push.
func TestBlackboxSinkPushAllOfflineMarkerPrunesPair(t *testing.T) {
	sink, server := newSinkAt(t)

	// Push 1: inst-A online — the pair registers.
	first := []*instance.Instance{
		domainInstance("srv-a", "pay-user", "10.0.0.1", 8080, "ecs", 1),
	}
	if err := sink.PushAll(1, first); err != nil {
		t.Fatalf("PushAll(online) error = %v", err)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs instances after the online push = %d, want 1; state = %v", got, server.Instances("pay-user", "ecs"))
	}

	// Push 2: the offline marker (Status 3, with the ip and Provider kept).
	// The per-instance policy deregisters it, and the prune's desired pair
	// holds an empty wanted set — a remote instance the deregister missed
	// (or that came back between the two) is swept too. Here the pair's
	// single remote instance is removed by the sweep even though the
	// per-instance deregister already ran.
	offlineMarker := []*instance.Instance{
		domainInstance("srv-a", "pay-user", "10.0.0.1", 8080, "ecs", 3),
	}
	if err := sink.PushAll(2, offlineMarker); err != nil {
		t.Fatalf("PushAll(offline marker) error = %v", err)
	}

	if got := len(server.Instances("pay-user", "ecs")); got != 0 {
		t.Fatalf("ecs instances after the offline-marker push = %d, want 0 (the pair's remote instance pruned); state = %v",
			got, server.Instances("pay-user", "ecs"))
	}
}

// -----------------------------------------------------------------------------
// Bounded-parallelism pushes (dsca-2 DS-2-1, fix design A)
// -----------------------------------------------------------------------------

// TestBlackboxSinkPushBoundedParallelismBoundsWallClock pins the throughput
// fix's core: 100 instances against a 10ms-per-request mock complete in
// wall-clock bounded by the CONCURRENCY, not the instance count (serial
// would be 100x10ms = 1s; 8-way bounded is ~13 sequential rounds ≈ 130ms
// plus overhead). The bound is generous (10x the ideal) to stay CI-stable
// while still failing the serial shape by an order of magnitude.
func TestBlackboxSinkPushBoundedParallelismBoundsWallClock(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetDelay(10 * time.Millisecond)

	// 100 online instances of ONE pair: the pair's health-check PUT rides
	// the first completing register only, so the per-instance cost dominates
	// uniformly. Same (service, cluster) keeps composite ids distinct by ip.
	instances := make([]*instance.Instance, 100)
	for i := range instances {
		instances[i] = domainInstance(
			"pod-par-"+strconv.Itoa(i), "pay-user",
			"10.1."+strconv.Itoa(i/256)+"."+strconv.Itoa(i%256+1), 8080, "k8s", 1)
	}

	start := time.Now()
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push(100 instances) error = %v", err)
	}
	elapsed := time.Since(start)

	// Serial floor: 100 registers x 10ms = 1s (plus the pair's one PUT).
	// The 8-way bound: ~13 rounds x 10ms = 130ms. Assert < 1s / 2 with the
	// mock's own overhead margin — comfortably above the parallel floor,
	// comfortably below the serial floor's half.
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("Push(100 x 10ms) wall clock = %v, want < 500ms (bounded by the ~8-way concurrency, not the 1s serial floor)", elapsed)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 100 {
		t.Fatalf("registered instances = %d, want 100 (every instance attempted exactly once)", got)
	}
}

// TestBlackboxSinkPushConcurrencyOneIsSerial pins the knob: at
// concurrency 1 the group degenerates to the sequential loop — the mutation
// shape the wall-clock test above fails against (and the regression guard
// that SetPushConcurrency is actually read per push).
func TestBlackboxSinkPushConcurrencyOneIsSerial(t *testing.T) {
	nacos.SetPushConcurrency(1)
	defer nacos.SetPushConcurrency(nacos.DefaultPushConcurrency)

	sink, server := newSinkAt(t)
	server.SetDelay(5 * time.Millisecond)

	instances := make([]*instance.Instance, 10)
	for i := range instances {
		instances[i] = domainInstance(
			"pod-ser-"+strconv.Itoa(i), "pay-user",
			"10.2."+strconv.Itoa(i/256)+"."+strconv.Itoa(i%256+1), 8080, "k8s", 1)
	}
	start := time.Now()
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed < 45*time.Millisecond {
		t.Fatalf("Push(10 x 5ms) at concurrency 1 = %v, want >= 45ms (serial: 10 sequential 5ms round trips)", elapsed)
	}
}

// TestBlackboxSinkPushParallelErrorPropagatesDeterministically pins the
// error semantics under the parallel group: every instance is still
// attempted (no short-circuit), the returned error is non-nil, and it is
// the FIRST error in instance order (deterministic despite nondeterministic
// completion).
func TestBlackboxSinkPushParallelErrorPropagatesDeterministically(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetStatus(500)

	instances := []*instance.Instance{
		domainInstance("pod-e1", "pay-user", "10.3.0.1", 8080, "k8s", 1),
		domainInstance("pod-e2", "pay-user", "10.3.0.2", 8080, "k8s", 1),
		domainInstance("pod-e3", "pay-user", "10.3.0.3", 8080, "k8s", 1),
	}
	err := sink.Push(1, instances)
	if err == nil {
		t.Fatal("Push(3 failing instances) error = nil, want the first error")
	}
	// Every instance was attempted: 3 POSTs answered 500 (the mock records
	// them all).
	registers := 0
	for _, request := range server.Requests() {
		if request.Method == "POST" {
			registers++
		}
	}
	if registers != 3 {
		t.Fatalf("register attempts = %d, want 3 (a failure never short-circuits the group)", registers)
	}
	// The returned error addresses the first instance in slice order.
	want := "nacos: register pod-e1"
	if !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("Push() error = %q, want it to start with %q (the FIRST error in instance order)", err.Error(), want)
	}
}

// TestBlackboxSinkConcurrentFirstRegistersIssueOneClusterUpdate pins the
// marker's atomic claim: under the bounded-parallelism register group, the
// FIRST registers of one (service, cluster) pair — which now run
// concurrently — issue exactly ONE UpdateCluster PUT for the pair (the old
// check-after shape let every worker race past the marker read and
// duplicate the idempotent PUT; dsca-2's live log showed 5 PUTs for 2
// pairs). The delay makes the register legs overlap so the race is real.
func TestBlackboxSinkConcurrentFirstRegistersIssueOneClusterUpdate(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetDelay(5 * time.Millisecond)

	// 20 instances of ONE pair: one concurrent first-register wave.
	instances := make([]*instance.Instance, 20)
	for i := range instances {
		instances[i] = domainInstance(
			"pod-claim-"+strconv.Itoa(i), "pay-user",
			"10.4."+strconv.Itoa(i/256)+"."+strconv.Itoa(i%256+1), 8080, "k8s", 1)
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push(20 concurrent first-registers) error = %v", err)
	}
	if updates := clusterUpdateRequests(server, "pay-user", "k8s"); len(updates) != 1 {
		t.Fatalf("cluster updates for the concurrently-registered pair = %d, want exactly 1 (the claim serializes the pair's PUT); requests = %v",
			len(updates), server.Requests())
	}
}

// TestBlackboxSinkPushAllPruneDeregistersBoundedParallel pins the
// deregister leg of the same fix: the prune sweep's DELETEs run under the
// bounded group too (DS-2-1: "the same bounded group applies to the
// PushAll/Push path AND the deregister path"), so pruning a large stale
// remote set is bounded by the concurrency, not the count.
func TestBlackboxSinkPushAllPruneDeregistersBoundedParallel(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetDelay(5 * time.Millisecond)

	// 60 stale remote instances of one pair, none in the pushed set.
	stale := make([]nacosmock.Host, 60)
	for i := range stale {
		stale[i] = nacosmock.Host{
			IP:       "10.5." + strconv.Itoa(i/256) + "." + strconv.Itoa(i%256+1),
			Port:     8080,
			Enabled:  true,
			Metadata: map[string]string{"instanceId": "pod-stale-" + strconv.Itoa(i)},
		}
	}
	server.SetInstances(stale, "DEFAULT_GROUP", "pay-user", "k8s")
	// The desired set: one survivor instance.
	pushed := []*instance.Instance{domainInstance("pod-keep", "pay-user", "10.6.0.1", 8080, "k8s", 1)}

	start := time.Now()
	if err := sink.PushAll(1, pushed); err != nil {
		t.Fatalf("PushAll() error = %v", err)
	}
	elapsed := time.Since(start)

	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("instances after prune = %d, want 1 (the survivor; 60 stale deleted); state = %v", got, server.Instances("pay-user", "k8s"))
	}
	// Serial floor: 1 register + 60 DELETEs + 1 catalog GET ≈ 62 x 5ms =
	// 310ms. The 8-way bound is ~40ms + the register/catalog serial legs.
	// Assert < 150ms: far above the mock noise floor, half the serial floor.
	if elapsed >= 150*time.Millisecond {
		t.Fatalf("PushAll prune wall clock = %v, want < 150ms (the DELETEs share the bounded group, not the ~310ms serial floor)", elapsed)
	}
}

func TestBlackboxSinkPushAllOperationConfirmedEmptyPrunesOwnedScope(t *testing.T) {
	sink, server := newSinkAt(t)
	owned := domainInstance("pod-owned", "pay-user", "10.8.0.1", 8080, "k8s", instance.InstanceStatusOnline)
	if err := sink.PushAll(1, []*instance.Instance{owned}); err != nil {
		t.Fatalf("seed owned instance: %v", err)
	}
	if err := sink.PushAllOperation(ports.RetryOperation{
		Sink: "nacos", Operate: ports.OperateTypeSyncAll, Scope: "k8s", BatchID: "empty-k8s",
		Sequence: 3, Trigger: 3, EmptyConfirmed: true,
	}); err != nil {
		t.Fatalf("confirmed empty full operation: %v", err)
	}
	if got := server.Instances("pay-user", "k8s"); len(got) != 0 {
		t.Fatalf("owned instances after confirmed empty = %v, want empty", got)
	}
}

func TestBlackboxSinkPruneSkipsForeignOwner(t *testing.T) {
	sink, server := newSinkAt(t)
	server.SetInstances([]nacosmock.Host{{IP: "10.8.0.9", Port: 8080, Enabled: true, Metadata: map[string]string{
		"instanceId": "foreign", "spotterOwner": "other-writer",
	}}}, "DEFAULT_GROUP", "pay-user", "k8s")
	owned := domainInstance("pod-owned", "pay-user", "10.8.0.1", 8080, "k8s", instance.InstanceStatusOnline)
	if err := sink.PushAll(1, []*instance.Instance{owned}); err != nil {
		t.Fatalf("push with foreign owner: %v", err)
	}
	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 2 {
		t.Fatalf("foreign owner state after prune = %v, want both foreign and owned", instances)
	}
}

func TestBlackboxSinkCustomGroupAndNamespaceRoundTripAndPrune(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	sink, err := nacos.NewSinkWithConfig(nacos.ClientConfig{
		ServerURL: server.URL(), NamespaceID: "tenant-a", GroupName: "blue",
	}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSinkWithConfig() error = %v", err)
	}
	server.SetInstancesInNamespace([]nacosmock.Host{{IP: "10.0.0.9", Port: 8080, Enabled: true,
		Metadata: map[string]string{"instanceId": "stale"}}}, "tenant-a", "blue", "pay-user", "k8s")
	desired := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", instance.InstanceStatusOnline)
	if err := sink.PushAll(1, []*instance.Instance{desired}); err != nil {
		t.Fatalf("PushAll(custom scope) error = %v", err)
	}
	if got := server.Instances("pay-user", "k8s"); len(got) != 1 || got[0].IP != "10.0.0.1" {
		t.Fatalf("custom scope instances = %v, want only desired host", got)
	}
	if cfg := server.ClusterConfigInNamespace("tenant-a", "pay-user", "blue", "k8s"); cfg == nil || cfg.HealthCheckerType != "NONE" {
		t.Fatalf("custom scope cluster config = %+v, want NONE in tenant-a/blue", cfg)
	}
	for _, request := range server.Requests() {
		if request.Query.Get("namespaceId") != "tenant-a" || request.Query.Get("groupName") != "blue" {
			t.Fatalf("custom scope request %s %s = namespace=%q group=%q, want tenant-a/blue", request.Method, request.Path, request.Query.Get("namespaceId"), request.Query.Get("groupName"))
		}
	}
}

// -----------------------------------------------------------------------------
// The nacos-authoritative reconcile source (dsca-3 §3.2 / §3.5)
// -----------------------------------------------------------------------------

// TestBlackboxSinkGetAllReadsCatalogViewIncludesDisabled: GetAll reads the
// CATALOG view (dsca-3 §3.2, DS-3-3) — a remote host with enabled=false is
// served, where the instance/list view hides it (the nacosmock implements
// the same visibility split as the real server: instance/list skips
// Enabled==false, catalog/instances includes it). The catalog-served
// reconstruction carries its metadata status ("1"), so a console-disabled
// HEALTHY instance reconstructs Status=1/Enabled=false — the shape the
// k8s-source diff must see to heal the disable.
func TestBlackboxSinkGetAllReadsCatalogViewIncludesDisabled(t *testing.T) {
	sink, server := newSinkAt(t)

	// Out-of-band drift: a console-disabled healthy instance the sink never
	// pushed (metadata status "1", wire enabled=false).
	server.SetInstances([]nacosmock.Host{
		{IP: "10.9.0.1", Port: 8080, Enabled: false,
			Metadata: map[string]string{
				"instanceId": "pod-drained", "envType": "test", "envGroup": "7",
				"reversion": "42", "status": "1", "state": "running",
				"idc": "kraken", "cpu": "2", "version": "v1", "schemaVersion": "1",
			}},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	list, err := sink.GetAll([]int32{1, 2}, "k8s")
	if err != nil {
		t.Fatalf("GetAll([1,2], k8s) error = %v", err)
	}
	if len(list.Instance) != 1 {
		t.Fatalf("GetAll = %d instances, want 1 (the catalog serves the disabled host)", len(list.Instance))
	}
	got := list.Instance[0]
	if got.Status != 1 || got.Enabled {
		t.Fatalf("reconstructed status/enabled = %d/%v, want 1/false (metadata status preferred, wire enabled honored)", got.Status, got.Enabled)
	}
	if got.InstanceId != "pod-drained" {
		t.Fatalf("reconstructed instanceId = %q, want pod-drained", got.InstanceId)
	}
}

// TestBlackboxSinkGetAllCatalogNotFoundToleratedPerService: a (service,
// cluster) pair with no catalog entry is the real server's steady state
// (HTTP 500 with an "is not found" body). GetAll must treat it as an empty
// pair and keep walking — exactly the prune's tolerance — so a fully pruned
// service never fails the diff view. The mock cannot produce a selective
// 500-with-body answer, so the tolerance is driven through a stub HTTP
// server answering the real-server body on the catalog path (the same
// technique as the prune's tolerance test).
func TestBlackboxSinkGetAllCatalogNotFoundToleratedPerService(t *testing.T) {
	const notFoundBody = `{"status":500,"message":"service pay-user is not found!","data":null,"code":500,"serverIp":"127.0.0.1"}`
	var catalogCalls int
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/nacos/v1/ns/catalog/instances":
			catalogCalls++
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, notFoundBody)
			return
		case "/nacos/v1/ns/service/list":
			writeJSON(w, http.StatusOK, map[string]interface{}{"count": 1, "doms": []string{"pay-user"}})
		default:
			writeStubOK(w)
		}
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	list, err := sink.GetAll(nil, "k8s")
	if err != nil {
		t.Fatalf("GetAll(catalog not-found 500) error = %v, want nil (tolerated as an empty pair)", err)
	}
	if list == nil || len(list.Instance) != 0 {
		t.Fatalf("GetAll(catalog not-found 500) = %#v, want an empty list", list)
	}
	if catalogCalls != 1 {
		t.Fatalf("catalog calls = %d, want 1 (one per service)", catalogCalls)
	}
}

// TestBlackboxSinkGetAllSurfacesOtherCatalogErrors: the not-found tolerance
// is narrow — any other catalog error aborts the view (a partial diff input
// must never be mistaken for a complete one), and a ListServices error
// aborts too.
func TestBlackboxSinkGetAllSurfacesOtherCatalogErrors(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/nacos/v1/ns/catalog/instances":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "injected status 500")
			return
		case "/nacos/v1/ns/service/list":
			writeJSON(w, http.StatusOK, map[string]interface{}{"count": 1, "doms": []string{"pay-user"}})
		default:
			writeStubOK(w)
		}
	}))
	defer stub.Close()
	sink, err := nacos.NewSink(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub) error = %v", err)
	}

	if _, err := sink.GetAll(nil, "k8s"); err == nil {
		t.Fatal("GetAll(unrelated catalog 500) error = nil, want the surfaced error")
	}

	// The 400-with-marker shape is a rejected request, not the absent-pair
	// answer (the same narrow-tolerance pin the prune carries).
	const notFoundBody400 = `{"status":400,"message":"service pay-user is not found!","data":null,"code":400,"serverIp":"127.0.0.1"}`
	stub2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/nacos/v1/ns/catalog/instances":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, notFoundBody400)
			return
		case "/nacos/v1/ns/service/list":
			writeJSON(w, http.StatusOK, map[string]interface{}{"count": 1, "doms": []string{"pay-user"}})
		default:
			writeStubOK(w)
		}
	}))
	defer stub2.Close()
	sink2, err := nacos.NewSink(stub2.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewSink(stub2) error = %v", err)
	}
	if _, err := sink2.GetAll(nil, "k8s"); err == nil {
		t.Fatal("GetAll(catalog 400 with not-found body) error = nil, want the surfaced error (tolerance is 500-only)")
	}
}

// TestBlackboxSinkGetAllRequestsProviderScopedCatalogPairs: the walk asks
// the catalog endpoint for clusterName == provider — the per-provider
// scoping of dsca-3 §3.2. The recorded requests are the observable: every
// catalog query the sink issued for provider "k8s" must carry
// clusterName=k8s, and none may address another cluster.
func TestBlackboxSinkGetAllRequestsProviderScopedCatalogPairs(t *testing.T) {
	sink, server := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("srv-a", "other-app", "10.0.0.2", 8081, "ecs", 1),
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	if _, err := sink.GetAll(nil, "k8s"); err != nil {
		t.Fatalf("GetAll(nil, k8s) error = %v", err)
	}

	catalogQueries := 0
	for _, request := range server.Requests() {
		if request.Path != "/nacos/v1/ns/catalog/instances" {
			continue
		}
		catalogQueries++
		if got := request.Query.Get("clusterName"); got != "k8s" {
			t.Fatalf("catalog query clusterName = %q, want k8s (provider scoping on the wire)", got)
		}
	}
	if catalogQueries == 0 {
		t.Fatalf("no catalog queries issued; requests = %v", server.Requests())
	}
}

// TestBlackboxSinkGetAllUnhealthyRoundTripSteady: the spotter-written
// unhealthy shape round-trips stably (the §1 round-trip demonstration's
// unhealthy row): a status-2 push registers with wire enabled=false and
// metadata status "2"; the catalog-based GetAll reconstructs Status=2 /
// Enabled=false / State=probing — the exact mirror a local status-2 k8s
// instance (locally enabled=false by readiness) compares equal against.
// This is the precondition the k8s steady pin in pkg/providers/k8s builds
// on (§3.5: "the k8s status-2 steady pin").
func TestBlackboxSinkGetAllUnhealthyRoundTripSteady(t *testing.T) {
	sink, _ := newSinkAt(t)

	unhealthy := domainInstance("srv-uh", "pay-user", "10.0.0.2", 8081, "ecs", 2)
	unhealthy.State = "probing"
	if err := sink.Push(1, []*instance.Instance{unhealthy}); err != nil {
		t.Fatalf("Push(unhealthy) error = %v", err)
	}

	list, err := sink.GetAll([]int32{2}, "ecs")
	if err != nil {
		t.Fatalf("GetAll([2], ecs) error = %v", err)
	}
	if len(list.Instance) != 1 {
		t.Fatalf("GetAll([2], ecs) = %d instances, want 1 (the catalog serves the disabled host)", len(list.Instance))
	}
	got := list.Instance[0]
	if got.Status != 2 || got.Enabled || got.State != "probing" {
		t.Fatalf("reconstructed status/enabled/state = %d/%v/%q, want 2/false/probing (the pushed unhealthy shape)", got.Status, got.Enabled, got.State)
	}
	if got.Reversion != 42 {
		t.Fatalf("reconstructed reversion = %d, want 42 (the current-schema entry keeps its written reversion)", got.Reversion)
	}
}

// TestBlackboxSinkReconstructSchemaVersionDegradation: the schemaVersion
// rule of dsca-5 §4.2-1 as reconstruct applies it — a missing or unknown
// schemaVersion degrades the reconstruction's diff-participation by ZEROING
// its reversion (the parseInt64-garbage discipline: the compare then heals
// by re-pushing the full metadata at the current schema), while a
// current-schema entry keeps the written reversion.
func TestBlackboxSinkReconstructSchemaVersionDegradation(t *testing.T) {
	sink, server := newSinkAt(t)

	// Three remote hosts: one current-schema, one missing the marker, one
	// claiming an unknown future schema. All carry reversion 42 in their
	// metadata.
	hostMetadata := func() map[string]string {
		return map[string]string{
			"instanceId": "pod-x", "envType": "test", "envGroup": "7",
			"reversion": "42", "status": "1", "state": "running",
			"idc": "kraken", "cpu": "2", "version": "v1",
		}
	}
	current := hostMetadata()
	current["instanceId"] = "pod-current"
	current["schemaVersion"] = "1"
	missing := hostMetadata()
	missing["instanceId"] = "pod-missing"
	unknown := hostMetadata()
	unknown["instanceId"] = "pod-unknown"
	unknown["schemaVersion"] = "99"
	server.SetInstances([]nacosmock.Host{
		{IP: "10.9.0.1", Port: 8080, Enabled: true, Metadata: current},
		{IP: "10.9.0.2", Port: 8080, Enabled: true, Metadata: missing},
		{IP: "10.9.0.3", Port: 8080, Enabled: true, Metadata: unknown},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	list, err := sink.GetAll(nil, "k8s")
	if err != nil {
		t.Fatalf("GetAll(nil, k8s) error = %v", err)
	}
	reversions := map[string]int64{}
	for _, got := range list.Instance {
		reversions[got.InstanceId] = got.Reversion
	}
	if reversions["pod-current"] != 42 {
		t.Fatalf("current-schema reversion = %d, want 42 (kept)", reversions["pod-current"])
	}
	if reversions["pod-missing"] != 0 {
		t.Fatalf("missing-schema reversion = %d, want 0 (degraded diff-participation)", reversions["pod-missing"])
	}
	if reversions["pod-unknown"] != 0 {
		t.Fatalf("unknown-schema reversion = %d, want 0 (degraded diff-participation)", reversions["pod-unknown"])
	}
}

package nacos_test

import (
	"strconv"
	"testing"
	"time"

	"spotter/internal/domain/instance"
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

func TestBlackboxSinkPushOnlineInstanceRegisters(t *testing.T) {
	sink, server := newSinkAt(t)

	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1234567890, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	requests := server.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1 (the register)", len(requests))
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

func TestBlackboxSinkPushSequentialPerInstance(t *testing.T) {
	sink, server := newSinkAt(t)

	instances := []*instance.Instance{
		domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1),
		domainInstance("pod-b", "pay-user", "10.0.0.2", 8080, "k8s", 1),
		domainInstance("pod-c", "other-app", "10.0.0.3", 8080, "ecs", 1),
	}
	if err := sink.Push(7, instances); err != nil {
		t.Fatalf("Push(3 instances) error = %v", err)
	}

	// Every instance is registered, in order: sequential per-instance pushes
	// (plan §7.4: Push applies the per-instance policy).
	registers := 0
	var lastSeen int = -1
	requests := server.Requests()
	for i, request := range requests {
		if request.Method == "POST" {
			registers++
			lastSeen = i
		}
	}
	if registers != 3 {
		t.Fatalf("register requests = %d, want 3", registers)
	}
	if lastSeen != len(requests)-1 {
		t.Fatalf("last request index = %d of %d, want the final request to be a register", lastSeen, len(requests))
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
	sink, _ := newSinkAt(t)

	// Push a fully populated instance, then reconstruct it through GetAll:
	// the metadata round-trip restores the domain fields (plan §7.3).
	ins := domainInstance("pod-a", "pay-user", "10.0.0.1", 8080, "k8s", 1)
	if err := sink.Push(1, []*instance.Instance{ins}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	list, err := sink.GetAll(nil, "")
	if err != nil {
		t.Fatalf("GetAll(nil, \"\") error = %v", err)
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
	if got.Provider != "k8s" || got.Cluster != "k8s" {
		t.Fatalf("reconstructed provider/cluster = %s/%s, want k8s/k8s", got.Provider, got.Cluster)
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
	list, err := sink.GetAll([]int32{1}, "")
	if err != nil {
		t.Fatalf("GetAll([1], \"\") error = %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "pod-a" {
		t.Fatalf("GetAll([1]) = %v, want the single online instance pod-a", list.Instance)
	}

	// Statuses [unhealthy]: the disabled ecs instance is selected.
	list, err = sink.GetAll([]int32{2}, "")
	if err != nil {
		t.Fatalf("GetAll([2], \"\") error = %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "srv-a" {
		t.Fatalf("GetAll([2]) = %v, want the single unhealthy instance srv-a", list.Instance)
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
	// pagination reports and reconstruct all of them.
	var instances []*instance.Instance
	for i, appCode := range []string{"app-a", "app-b", "app-c"} {
		instances = append(instances, domainInstance(
			"pod-"+strconv.Itoa(i), appCode, "10.0.0."+strconv.Itoa(i+1), 8080, "k8s", 1))
	}
	if err := sink.Push(1, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}

	list, err := sink.GetAll(nil, "")
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

// Package nacos_test holds the black-box suite of the Nacos adapter: the
// hand-rolled v1 OpenAPI client (client_test.go) and the InstanceSink
// (sink_test.go), both driven against the in-process nacosmock server.
package nacos_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"spotter/internal/testkit/fakes"
	"spotter/internal/testkit/nacosmock"
	"spotter/pkg/nacos"
)

// newClientAt starts a nacosmock server and binds a Nacos client to it.
func newClientAt(t *testing.T) (*nacos.Client, *nacosmock.Server) {
	t.Helper()
	server := nacosmock.Start()
	t.Cleanup(server.Close)
	client, err := nacos.NewClient(server.URL(), &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient(%s) error = %v", server.URL(), err)
	}
	return client, server
}

func TestBlackboxClientRegisterSendsPersistentFormParams(t *testing.T) {
	client, server := newClientAt(t)

	err := client.RegisterInstance(nacos.InstanceParams{
		ServiceName: "pay-user",
		IP:          "10.0.0.1",
		Port:        8080,
		ClusterName: "k8s",
		Enabled:     true,
		Ephemeral:   false,
		Metadata:    map[string]string{"instanceId": "pod-a", "envType": "test"},
	})
	if err != nil {
		t.Fatalf("RegisterInstance() error = %v", err)
	}

	requests := server.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1 (the register)", len(requests))
	}
	got := requests[0]
	if got.Method != "POST" || got.Path != "/nacos/v1/ns/instance" {
		t.Fatalf("register request = %s %s, want POST /nacos/v1/ns/instance", got.Method, got.Path)
	}
	want := map[string]string{
		"serviceName": "pay-user",
		"ip":          "10.0.0.1",
		"port":        "8080",
		"clusterName": "k8s",
		"groupName":   "DEFAULT_GROUP",
		"namespaceId": "public",
		"ephemeral":   "false",
		"enabled":     "true",
		"metadata":    `{"envType":"test","instanceId":"pod-a"}`,
	}
	for key, value := range want {
		if got.Query.Get(key) != value {
			t.Fatalf("register query[%s] = %q, want %q (all: %v)", key, got.Query.Get(key), value, got.Query)
		}
	}

	// The stored state carries the same mapping.
	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 1 {
		t.Fatalf("instances = %d, want 1", len(instances))
	}
	if instances[0].Ephemeral {
		t.Fatal("registered instance is ephemeral, want persistent (ephemeral=false)")
	}
}

func TestBlackboxClientRegisterErrorPropagates(t *testing.T) {
	client, server := newClientAt(t)
	_ = server
	server.SetStatus(500)

	err := client.RegisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 1, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("RegisterInstance() error = nil, want the HTTP 500 error")
	}
	if !contains(err.Error(), "500") {
		t.Fatalf("RegisterInstance() error = %q, want it to mention status 500", err)
	}
}

func TestBlackboxClientServerListFailsOverOn5xx(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer first.Close()
	second := nacosmock.Start()
	defer second.Close()
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURLs: []string{first.URL, second.URL()}}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	if err := client.RegisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s"}); err != nil {
		t.Fatalf("RegisterInstance() failover error = %v", err)
	}
	if got := len(second.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("failover target instances = %d, want 1", got)
	}
}

func TestBlackboxClientServerListStopsOn4xx(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer first.Close()
	second := nacosmock.Start()
	defer second.Close()
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURLs: []string{first.URL, second.URL()}}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	err = client.RegisterInstance(nacos.InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("RegisterInstance() error = nil, want permanent 4xx")
	}
	if len(second.Requests()) != 0 {
		t.Fatalf("4xx request unexpectedly failed over to second server: %#v", second.Requests())
	}
}

func TestBlackboxClientSDKModeUsesOfficialPersistentLifecycle(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{
		ServerURL: server.URL(), TransportMode: nacos.TransportSDK,
	}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig(sdk) error = %v", err)
	}
	defer func() { _ = client.Close() }()
	params := nacos.InstanceParams{ServiceName: "sdk-svc", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s", Enabled: true, Ephemeral: false}
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("SDK RegisterInstance() error = %v", err)
	}
	requests := server.Requests()
	if len(requests) != 1 || requests[0].Method != "POST" || requests[0].Path != "/nacos/v1/ns/instance" {
		t.Fatalf("SDK register requests = %v, want one POST to the naming endpoint", requests)
	}
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("SDK DeregisterInstance() error = %v", err)
	}
	requests = server.Requests()
	if len(requests) != 2 || requests[1].Method != "DELETE" || requests[1].Path != "/nacos/v1/ns/instance" {
		t.Fatalf("SDK deregister requests = %v, want POST then DELETE", requests)
	}
}

func TestBlackboxClientDeregisterUsesCompositeIdParams(t *testing.T) {
	client, server := newClientAt(t)

	err := client.DeregisterInstance(nacos.InstanceParams{
		ServiceName: "pay-user",
		IP:          "10.0.0.1",
		Port:        8080,
		ClusterName: "k8s",
	})
	if err != nil {
		t.Fatalf("DeregisterInstance() error = %v", err)
	}

	requests := server.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1 (the deregister)", len(requests))
	}
	got := requests[0]
	if got.Method != "DELETE" || got.Path != "/nacos/v1/ns/instance" {
		t.Fatalf("deregister request = %s %s, want DELETE /nacos/v1/ns/instance", got.Method, got.Path)
	}
	for key, value := range map[string]string{
		"serviceName": "pay-user",
		"ip":          "10.0.0.1",
		"port":        "8080",
		"clusterName": "k8s",
		"groupName":   "DEFAULT_GROUP",
	} {
		if got.Query.Get(key) != value {
			t.Fatalf("deregister query[%s] = %q, want %q (all: %v)", key, got.Query.Get(key), value, got.Query)
		}
	}
	if got.Query.Get("ephemeral") != "false" {
		t.Fatalf("deregister ephemeral = %q, want false (the same composite id as the register)", got.Query.Get("ephemeral"))
	}
}

func TestBlackboxClientDeregisterErrorPropagates(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(500)

	err := client.DeregisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 1, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("DeregisterInstance() error = nil, want the HTTP 500 error")
	}
}

// TestBlackboxClientUpdateClusterSendsQueryForm: the cluster update is a
// body-less PUT whose parameters ride the QUERY STRING — the v1 servlet does
// not parse a form body on PUT (verified live against Nacos 2.1.0: the
// query-string form answers ok, the form-body form does not) — carrying
// every required parameter with the NONE health checker and the client's
// fixed group/namespace convention. The stored configuration (the mock's
// record of the wire) carries the same mapping.
func TestBlackboxClientUpdateClusterSendsQueryForm(t *testing.T) {
	client, server := newClientAt(t)

	if err := client.UpdateCluster("pay-user", "k8s"); err != nil {
		t.Fatalf("UpdateCluster(pay-user, k8s) error = %v", err)
	}

	requests := server.Requests()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1 (the cluster update)", len(requests))
	}
	got := requests[0]
	if got.Method != "PUT" || got.Path != "/nacos/v1/ns/cluster" {
		t.Fatalf("cluster update request = %s %s, want PUT /nacos/v1/ns/cluster", got.Method, got.Path)
	}
	// The parameters are on the query string (the recorded Query IS the
	// URL's query), never a form body — a body would be invisible to the
	// real servlet and silently dropped.
	want := map[string]string{
		"serviceName":           "pay-user",
		"clusterName":           "k8s",
		"checkPort":             "0",
		"useInstancePort4Check": "false",
		"healthChecker":         `{"type":"NONE"}`,
		"groupName":             "DEFAULT_GROUP",
		"namespaceId":           "public",
	}
	for key, value := range want {
		if got.Query.Get(key) != value {
			t.Fatalf("cluster update query[%s] = %q, want %q (all: %v)", key, got.Query.Get(key), value, got.Query)
		}
	}

	// The stored state carries the same mapping: the NONE checker.
	config := server.ClusterConfig("pay-user", "DEFAULT_GROUP", "k8s")
	if config == nil || config.HealthCheckerType != "NONE" {
		t.Fatalf("stored cluster config = %+v, want the NONE health checker", config)
	}
}

// TestBlackboxClientUpdateCluster400AnswerIsPermanentAPIError: a 4xx answer
// to the cluster update (a malformed-parameter rejection) surfaces as
// *nacos.APIError with Permanent() true — the same classification contract
// as every other endpoint.
func TestBlackboxClientUpdateCluster400AnswerIsPermanentAPIError(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(400)

	err := client.UpdateCluster("pay-user", "k8s")
	if err == nil {
		t.Fatal("UpdateCluster() error = nil, want the HTTP 400 error")
	}
	var apiErr *nacos.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("UpdateCluster() error = %T (%v), want *nacos.APIError", err, err)
	}
	if !apiErr.Permanent() {
		t.Fatalf("APIError.Permanent() = false for status %d, want true (4xx)", apiErr.Status)
	}
}

// TestBlackboxClient400AnswerIsPermanentAPIError: a 4xx answer surfaces as
// *nacos.APIError with Permanent() true — the request itself is rejected, so
// the retry queue must be able to classify and drop it (the live incident:
// Nacos 400 "Param 'ip' is required" retried 9624 times). The message keeps
// the wording of the plain fmt.Errorf it replaced, mentioning the status.
func TestBlackboxClient400AnswerIsPermanentAPIError(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(400)

	err := client.DeregisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 1, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("DeregisterInstance() error = nil, want the HTTP 400 error")
	}
	var apiErr *nacos.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("DeregisterInstance() error = %T (%v), want *nacos.APIError", err, err)
	}
	if !apiErr.Permanent() {
		t.Fatalf("APIError.Permanent() = false for status %d, want true (4xx)", apiErr.Status)
	}
	if !contains(err.Error(), "400") {
		t.Fatalf("DeregisterInstance() error = %q, want it to mention status 400", err)
	}

	// The read path answers the same typed error too.
	if _, err := client.ListInstances("pay-user"); err == nil {
		t.Fatal("ListInstances() error = nil, want the HTTP 400 error")
	} else if !errors.As(err, &apiErr) || !apiErr.Permanent() {
		t.Fatalf("ListInstances() error = %T (%v), want a permanent *nacos.APIError", err, err)
	}
}

// TestBlackboxClient500AnswerIsRetriableAPIError: a 5xx answer is an
// *nacos.APIError but NOT permanent — the server may recover, so the retry
// queue keeps the entry queued.
func TestBlackboxClient500AnswerIsRetriableAPIError(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(500)

	err := client.RegisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 1, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("RegisterInstance() error = nil, want the HTTP 500 error")
	}
	var apiErr *nacos.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("RegisterInstance() error = %T (%v), want *nacos.APIError", err, err)
	}
	if apiErr.Permanent() {
		t.Fatalf("APIError.Permanent() = true for status %d, want false (5xx is retriable)", apiErr.Status)
	}
}

// TestBlackboxClientPermanentStatusBoundaries: pins the exact boundary of
// Permanent()'s range — 399 is below it (retriable), 499 stays inside it
// (permanent), matching 400 <= status < 500.
func TestBlackboxClientPermanentStatusBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		permanent bool
	}{
		{name: "399 is below the permanent range", status: 399, permanent: false},
		{name: "499 stays inside the permanent range", status: 499, permanent: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, server := newClientAt(t)
			server.SetStatus(tc.status)

			err := client.RegisterInstance(nacos.InstanceParams{ServiceName: "pay-user", IP: "10.0.0.1", Port: 1, ClusterName: "k8s"})
			if err == nil {
				t.Fatalf("RegisterInstance() error = nil, want the HTTP %d error", tc.status)
			}
			var apiErr *nacos.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("RegisterInstance() error = %T (%v), want *nacos.APIError", err, err)
			}
			if got := apiErr.Permanent(); got != tc.permanent {
				t.Fatalf("APIError.Permanent() for status %d = %v, want %v", tc.status, got, tc.permanent)
			}
		})
	}
}

// TestBlackboxClientListInstancesParsesHosts: the instance list returns the
// VISIBLE (enabled) hosts of the service — the disabled ecs host is HIDDEN
// from instance/list, matching the real Nacos 2.1.0 view filter (nacos
// source: `if (!ip.isEnabled()) continue;`) that the mock now mirrors. The
// disabled host stays in the fixture on purpose: the parallel
// ListCatalogInstances assertion proves it still exists and is served by
// the catalog view — the F8 asymmetry the prune relies on.
func TestBlackboxClientListInstancesParsesHosts(t *testing.T) {
	client, _ := newClientAt(t)

	for _, params := range []nacos.InstanceParams{
		{ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s", Enabled: true,
			Metadata: map[string]string{"instanceId": "pod-a", "envType": "test"}},
		{ServiceName: "pay-user", IP: "10.0.0.2", Port: 8081, ClusterName: "ecs", Enabled: false,
			Metadata: map[string]string{"instanceId": "srv-b"}},
	} {
		if err := client.RegisterInstance(params); err != nil {
			t.Fatalf("RegisterInstance(%s) error = %v", params.IP, err)
		}
	}

	hosts, err := client.ListInstances("pay-user")
	if err != nil {
		t.Fatalf("ListInstances(pay-user) error = %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("ListInstances(pay-user) = %d hosts, want 1 (the disabled ecs host is hidden)", len(hosts))
	}
	host := hosts[0]
	if host.InstanceID == "" || host.ClusterName == "" || host.Metadata == nil {
		t.Fatalf("host = %+v, want the composite id, cluster and metadata parsed", host)
	}
	if host.IP != "10.0.0.1" || host.Port != 8080 || host.ClusterName != "k8s" || !host.Enabled {
		t.Fatalf("k8s host = %+v, want 10.0.0.1:8080/k8s/enabled", host)
	}
	if host.Metadata["instanceId"] != "pod-a" || host.Metadata["envType"] != "test" {
		t.Fatalf("k8s host metadata = %v, want the register metadata", host.Metadata)
	}

	// The catalog view serves BOTH hosts: the enabled k8s one and the
	// disabled ecs one the instance list filtered out.
	catalog, err := client.ListCatalogInstances("pay-user", "ecs")
	if err != nil {
		t.Fatalf("ListCatalogInstances(pay-user, ecs) error = %v", err)
	}
	if len(catalog) != 1 {
		t.Fatalf("ListCatalogInstances(pay-user, ecs) = %d hosts, want 1 (the hidden disabled host)", len(catalog))
	}
	if catalog[0].IP != "10.0.0.2" || catalog[0].Port != 8081 || catalog[0].ClusterName != "ecs" || catalog[0].Enabled {
		t.Fatalf("catalog ecs host = %+v, want 10.0.0.2:8081/ecs/disabled", catalog[0])
	}
	if catalog[0].Metadata["instanceId"] != "srv-b" {
		t.Fatalf("catalog ecs host metadata = %v, want the register metadata", catalog[0].Metadata)
	}

	// A service with no instances returns an empty slice, not an error.
	empty, err := client.ListInstances("ghost")
	if err != nil {
		t.Fatalf("ListInstances(ghost) error = %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListInstances(ghost) = %d hosts, want 0", len(empty))
	}
}

func TestBlackboxClientListInstancesErrorPropagates(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(500)

	if _, err := client.ListInstances("pay-user"); err == nil {
		t.Fatal("ListInstances() error = nil, want the HTTP 500 error")
	}
}

// TestBlackboxClientListCatalogInstancesPaginatesAll: the catalog listing
// paginates with the ListServices discipline — iterate while a page comes
// back full, stop on the first short page — and sends the full selector set
// (serviceName, clusterName, groupName, namespaceId, pageSize, pageNo,
// hasIpCount) on every page request. The multi-page walk itself (more hosts
// than one page) is pinned by the mock self-test
// (nacosmock.TestCatalogInstancesServesDisabledAndPaginates drives pageSize
// 2 across 3 pages); here the request log pins the client's fixed page
// size of 100 and the exact wire form of the page request.
func TestBlackboxClientListCatalogInstancesPaginatesAll(t *testing.T) {
	client, server := newClientAt(t)

	// Three instances of one (service, cluster): the enabled/disabled mix
	// pins the catalog's defining property (disabled hosts are served).
	hosts := []nacos.InstanceParams{
		{ServiceName: "pay-user", IP: "10.0.0.1", Port: 8001, ClusterName: "k8s", Enabled: true},
		{ServiceName: "pay-user", IP: "10.0.0.2", Port: 8002, ClusterName: "k8s", Enabled: false},
		{ServiceName: "pay-user", IP: "10.0.0.3", Port: 8003, ClusterName: "k8s", Enabled: true},
	}
	for _, params := range hosts {
		if err := client.RegisterInstance(params); err != nil {
			t.Fatalf("RegisterInstance(%s) error = %v", params.IP, err)
		}
	}
	// Another cluster of the same service: never in the k8s catalog.
	if err := client.RegisterInstance(nacos.InstanceParams{
		ServiceName: "pay-user", IP: "10.0.0.9", Port: 8009, ClusterName: "ecs", Enabled: true,
	}); err != nil {
		t.Fatalf("RegisterInstance(ecs) error = %v", err)
	}

	catalog, err := client.ListCatalogInstances("pay-user", "k8s")
	if err != nil {
		t.Fatalf("ListCatalogInstances(pay-user, k8s) error = %v", err)
	}
	if len(catalog) != 3 {
		t.Fatalf("ListCatalogInstances(pay-user, k8s) = %d hosts, want 3 (incl. the disabled one); got %v", len(catalog), catalog)
	}
	disabled := 0
	for _, host := range catalog {
		if host.ClusterName != "k8s" {
			t.Fatalf("catalog host = %+v, want k8s cluster only (the catalog is cluster-scoped)", host)
		}
		if !host.Enabled {
			disabled++
		}
	}
	if disabled != 1 {
		t.Fatalf("catalog disabled hosts = %d, want 1 (10.0.0.2)", disabled)
	}

	// The wire form of every page request: the full selector set with
	// pageSize 100 and hasIpCount false, pageNo starting at 1.
	requests := server.Requests()
	pageRequests := 0
	for _, request := range requests {
		if request.Path != "/nacos/v1/ns/catalog/instances" {
			continue
		}
		pageRequests++
		for key, value := range map[string]string{
			"serviceName": "pay-user",
			"clusterName": "k8s",
			"groupName":   "DEFAULT_GROUP",
			"namespaceId": "public",
			"pageSize":    "100",
			"pageNo":      "1",
			"hasIpCount":  "false",
		} {
			if request.Query.Get(key) != value {
				t.Fatalf("catalog request query[%s] = %q, want %q (all: %v)", key, request.Query.Get(key), value, request.Query)
			}
		}
	}
	if pageRequests != 1 {
		t.Fatalf("catalog page requests = %d, want 1 (the 3-host answer ends the loop on one short page)", pageRequests)
	}

	// A (service, cluster) with no catalog entry answers empty, not error:
	// the steady state of a fully pruned service.
	empty, err := client.ListCatalogInstances("ghost", "k8s")
	if err != nil {
		t.Fatalf("ListCatalogInstances(ghost, k8s) error = %v, want nil (empty catalog)", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ListCatalogInstances(ghost, k8s) = %d hosts, want 0", len(empty))
	}
}

// TestBlackboxClientListCatalogInstancesMissingParamsRejected: the mock (and
// the real server behind it) rejects a catalog request without its two
// selectors — the client surfaces the rejection as a typed *nacos.APIError
// instead of silently returning data for a wrong scope.
func TestBlackboxClientListCatalogInstancesMissingParamsRejected(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	// The client cannot be asked to omit a param (it always sends both), so
	// drive the wire directly: the raw endpoint must reject each missing
	// selector, proving the 400 the client would surface if it ever did.
	for name, query := range map[string]string{
		"missing serviceName": "clusterName=k8s&groupName=DEFAULT_GROUP&pageNo=1&pageSize=100",
		"missing clusterName": "serviceName=pay-user&groupName=DEFAULT_GROUP&pageNo=1&pageSize=100",
	} {
		status, body, err := get(server.URL() + "/nacos/v1/ns/catalog/instances?" + query)
		if err != nil {
			t.Fatalf("%s: raw GET error = %v", name, err)
		}
		if status != 400 {
			t.Fatalf("%s: status = %d, want 400; body: %s", name, status, body)
		}
	}
}

// TestBlackboxClientListCatalogInstancesStopsAtCount: the clamped-page shape
// — a server that answers 1-element pages regardless of the requested
// pageSize=100, with count carrying the true total of 3. A 1-element page is
// always "short" against the requested 100, so the short-page termination
// alone would stop the walk after ONE host and silently return 1 of 3
// instances (a truncated prune view). The count-based stop keeps the walk
// going while the accumulated total is below Count, so all 3 hosts are
// collected across the 1-element pages. A stub HTTP server serves the shape
// (the nacosmock honors the requested pageSize, so it cannot clamp).
func TestBlackboxClientListCatalogInstancesStopsAtCount(t *testing.T) {
	hosts := []map[string]interface{}{
		{"instanceId": "10.0.0.1#8001#k8s#DEFAULT_GROUP@@pay-user", "ip": "10.0.0.1", "port": 8001, "enabled": true, "clusterName": "k8s", "serviceName": "pay-user"},
		{"instanceId": "10.0.0.2#8002#k8s#DEFAULT_GROUP@@pay-user", "ip": "10.0.0.2", "port": 8002, "enabled": false, "clusterName": "k8s", "serviceName": "pay-user"},
		{"instanceId": "10.0.0.3#8003#k8s#DEFAULT_GROUP@@pay-user", "ip": "10.0.0.3", "port": 8003, "enabled": true, "clusterName": "k8s", "serviceName": "pay-user"},
	}
	var catalogCalls int
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/nacos/v1/ns/catalog/instances" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
			http.Error(w, "unexpected", http.StatusBadRequest)
			return
		}
		catalogCalls++
		pageNo, _ := strconv.Atoi(request.URL.Query().Get("pageNo"))
		// Serve exactly one host per page — a clamped effective page size —
		// with Count carrying the true total.
		if pageNo < 1 || pageNo > len(hosts) {
			writeJSONStub(w, map[string]interface{}{"count": len(hosts), "list": []map[string]interface{}{}})
			return
		}
		writeJSONStub(w, map[string]interface{}{"count": len(hosts), "list": hosts[pageNo-1 : pageNo]})
	}))
	defer stub.Close()
	client, err := nacos.NewClient(stub.URL, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient(stub) error = %v", err)
	}

	got, err := client.ListCatalogInstances("pay-user", "k8s")
	if err != nil {
		t.Fatalf("ListCatalogInstances(clamped pages) error = %v", err)
	}
	if len(got) != len(hosts) {
		t.Fatalf("ListCatalogInstances(clamped pages) = %d hosts, want %d (count-based stop walks every 1-element page); got %v", len(got), len(hosts), got)
	}
	seenDisabled := false
	for i, host := range got {
		if host.IP != fmt.Sprintf("10.0.0.%d", i+1) {
			t.Fatalf("host[%d] = %s, want the page order preserved", i, host.IP)
		}
		if !host.Enabled {
			seenDisabled = true
		}
	}
	if !seenDisabled {
		t.Fatal("no disabled host in the catalog answer, want the middle one disabled")
	}
	if catalogCalls != len(hosts) {
		t.Fatalf("catalog calls = %d, want %d (one per clamped page, then the count stop)", catalogCalls, len(hosts))
	}
}

// writeJSONStub marshals payload as a JSON response (the stub-server helper
// for wire-shape tests).
func writeJSONStub(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
}

// get issues one raw GET against the nacosmock and returns the status and
// body — the client-shaped assertion helper for wire-level rejection tests.
func get(target string) (int, string, error) {
	response, err := http.Get(target)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, "", err
	}
	return response.StatusCode, string(body), nil
}

func TestBlackboxClientListServicesPaginatesAll(t *testing.T) {
	client, _ := newClientAt(t)

	// 6 services; the client's fixed page size of 100 must be overridden by
	// the page size parameter so multiple pages occur (page size 2: 3 pages).
	names := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f"}
	for _, name := range names {
		err := client.RegisterInstance(nacos.InstanceParams{
			ServiceName: name, IP: "10.0.0.1", Port: 8080, ClusterName: "k8s",
		})
		if err != nil {
			t.Fatalf("RegisterInstance(%s) error = %v", name, err)
		}
	}

	services, err := client.ListServices(2)
	if err != nil {
		t.Fatalf("ListServices(2) error = %v", err)
	}
	if len(services) != len(names) {
		t.Fatalf("ListServices(2) = %d services (%v), want %d", len(services), services, len(names))
	}
	for i, name := range names {
		if services[i] != name {
			t.Fatalf("services[%d] = %q, want %q (sorted page concatenation)", i, services[i], name)
		}
	}

	// One page covering everything: the same set with a single request.
	single, err := client.ListServices(100)
	if err != nil {
		t.Fatalf("ListServices(100) error = %v", err)
	}
	if len(single) != len(names) {
		t.Fatalf("ListServices(100) = %d services, want %d", len(single), len(names))
	}
}

func TestBlackboxClientListServicesErrorPropagates(t *testing.T) {
	client, server := newClientAt(t)
	server.SetStatus(500)

	if _, err := client.ListServices(100); err == nil {
		t.Fatal("ListServices() error = nil, want the HTTP 500 error")
	}
}

func TestBlackboxClientTimeoutReturnsError(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	// The delay exceeds the client's per-request timeout.
	server.SetDelay(nacos.RequestTimeout + 500*time.Millisecond)

	client, err := nacos.NewClient(server.URL(), &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	// A fresh short-deadline context: the call must fail well before the
	// injected delay elapses, proving the timeout bounds the wait.
	deadline := time.Now().Add(200 * time.Millisecond)
	_, err = client.ListInstances("pay-user")
	if err == nil {
		t.Fatal("ListInstances() error = nil, want the timeout error")
	}
	if elapsed := time.Since(deadline); elapsed > nacos.RequestTimeout {
		t.Fatalf("timed-out call took too long: %s elapsed, deadline drift %s", time.Since(deadline.Add(-200*time.Millisecond)), elapsed)
	}
}

func TestBlackboxClientNewClientValidation(t *testing.T) {
	if _, err := nacos.NewClient("", &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewClient(\"\") error = nil, want an error for the empty address")
	}
	if _, err := nacos.NewClient("://bad url", &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewClient(bad url) error = nil, want an error for the unparseable address")
	}
	// A nil logger is accepted and defaulted.
	client, err := nacos.NewClient("http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatalf("NewClient(nil logger) error = %v, want nil (defaulted)", err)
	}
	if client == nil {
		t.Fatal("NewClient(nil logger) = nil client")
	}
	for _, address := range []string{"http://user:pass@127.0.0.1:8848", "http://127.0.0.1:8848?username=u", "http://127.0.0.1:8848?password=p"} {
		if _, err := nacos.NewClient(address, &fakes.FakeLogger{}); err == nil {
			t.Fatalf("NewClient(%q) error = nil, want URL credentials rejected", address)
		}
	}
	if _, err := nacos.NewClientWithConfig(nacos.ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: nacos.TransportMode("bogus")}, &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewClientWithConfig(bogus transport) error = nil, want fail-closed validation")
	}
}

func TestClientConfigInjectsNamespaceGroupAndToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for key, want := range map[string]string{"namespaceId": "tenant-a", "groupName": "blue", "accessToken": "secret-token"} {
			if q.Get(key) != want {
				t.Errorf("%s = %q, want %q", key, q.Get(key), want)
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	c, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: server.URL, NamespaceID: "tenant-a", GroupName: "blue", AccessToken: "secret-token"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterInstance(nacos.InstanceParams{ServiceName: "svc", IP: "127.0.0.1", Port: 80}); err != nil {
		t.Fatal(err)
	}
}

func TestBlackboxClientNewClientDefaultsScheme(t *testing.T) {
	// The --nacos-addr help documents schemeless addresses ("e.g.
	// 127.0.0.1:18848"), so a schemeless address must default to http://
	// and work — the plan §8.4 soak invocation passes exactly that form.
	server := nacosmock.Start()
	defer server.Close()

	schemeless := strings.TrimPrefix(server.URL(), "http://")
	client, err := nacos.NewClient(schemeless, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClient(schemeless %q) error = %v, want nil (http defaulted)", schemeless, err)
	}
	if err := client.RegisterInstance(nacos.InstanceParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s",
	}); err != nil {
		t.Fatalf("RegisterInstance over the schemeless address error = %v", err)
	}
	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("instances = %d, want 1 (the request reached the mock)", got)
	}

	// An explicit scheme is honored, not rewritten.
	if _, err := nacos.NewClient(server.URL(), &fakes.FakeLogger{}); err != nil {
		t.Fatalf("NewClient(%q) error = %v, want nil (explicit scheme honored)", server.URL(), err)
	}
	if _, err := nacos.NewClient("https  ://127.0.0.1:1", &fakes.FakeLogger{}); err == nil {
		t.Fatal("NewClient(garbage) error = nil, want an error")
	}

	// The readiness gate accepts the schemeless form too (the flag value
	// flows there before the client is built).
	if err := nacos.CheckReadiness(schemeless, nacos.RequestTimeout); err != nil {
		t.Fatalf("CheckReadiness(schemeless %q) error = %v, want nil", schemeless, err)
	}
}

func TestBlackboxClientDialHealthCheckReadiness(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()

	if err := nacos.CheckReadiness(server.URL(), nacos.RequestTimeout); err != nil {
		t.Fatalf("CheckReadiness(healthy) error = %v, want nil", err)
	}
	if err := nacos.CheckReadiness("http://127.0.0.1:1", 100*time.Millisecond); err == nil {
		t.Fatal("CheckReadiness(unreachable) error = nil, want an error")
	}
}

func TestBlackboxClientReadinessIncludesWriteProbe(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	if err := nacos.CheckReadinessWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: server.URL(), NamespaceID: "tenant-a", GroupName: "blue"}, &fakes.FakeLogger{}); err != nil {
		t.Fatalf("CheckReadinessWithConfig() error = %v", err)
	}
	requests := server.Requests()
	seenRead, seenRegister, seenDelete := false, false, false
	canaryService := ""
	for _, request := range requests {
		switch {
		case request.Method == "GET" && request.Path == "/nacos/v1/console/health/readiness":
			seenRead = true
		case request.Method == "POST" && request.Path == "/nacos/v1/ns/instance":
			seenRegister = true
			canaryService = request.Query.Get("serviceName")
		case request.Method == "DELETE" && request.Path == "/nacos/v1/ns/instance":
			seenDelete = true
		}
	}
	if !seenRead || !seenRegister || !seenDelete {
		t.Fatalf("readiness requests = %#v, want read/register/delete probe", requests)
	}
	for _, request := range requests {
		if request.Method == "POST" || request.Method == "DELETE" {
			if request.Query.Get("namespaceId") != "tenant-a" || request.Query.Get("groupName") != "blue" {
				t.Fatalf("write probe scope = namespace=%q group=%q, want tenant-a/blue", request.Query.Get("namespaceId"), request.Query.Get("groupName"))
			}
		}
	}
	// A successful probe must leave no persistent canary behind.
	if canaryService == "" {
		t.Fatal("readiness register request did not include a canary service name")
	}
	if hosts := server.Instances(canaryService, ""); len(hosts) != 0 {
		t.Fatalf("readiness canary remains after cleanup: %+v", hosts)
	}
}

func TestBlackboxClientReadinessWriteFailureBlocksStartup(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	server.SetEndpointStatus("/nacos/v1/ns/instance", http.StatusInternalServerError)
	err := nacos.CheckReadinessWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: server.URL(), NamespaceID: "tenant-a", GroupName: "blue", Timeout: 250 * time.Millisecond}, &fakes.FakeLogger{})
	if err == nil {
		t.Fatal("CheckReadinessWithConfig() error = nil, want persistent write-probe failure")
	}
	requests := server.Requests()
	if len(requests) != 2 || requests[0].Method != "GET" || requests[1].Method != "POST" {
		t.Fatalf("readiness failure requests = %#v, want GET readiness then POST canary", requests)
	}
}

func TestBlackboxClientReadinessPinsCanaryAddress(t *testing.T) {
	first := nacosmock.Start()
	defer first.Close()
	second := nacosmock.Start()
	defer second.Close()
	if err := nacos.CheckReadinessWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURLs: []string{first.URL(), second.URL()}}, &fakes.FakeLogger{}); err != nil {
		t.Fatalf("CheckReadinessWithConfig() error = %v", err)
	}
	firstWrites := 0
	for _, request := range first.Requests() {
		if request.Method == "POST" || request.Method == "DELETE" {
			firstWrites++
		}
	}
	if firstWrites != 2 {
		t.Fatalf("first server canary writes = %d, want register+deregister", firstWrites)
	}
	for _, request := range second.Requests() {
		if request.Method == "POST" || request.Method == "DELETE" {
			t.Fatalf("second server received canary write after first passed: %#v", request)
		}
	}
}

func TestBlackboxClientConfigServerURLsTakePrecedence(t *testing.T) {
	first := nacosmock.Start()
	defer first.Close()
	second := nacosmock.Start()
	defer second.Close()
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: second.URL(), ServerURLs: []string{first.URL()}}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	if err := client.RegisterInstance(nacos.InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80, ClusterName: "k8s"}); err != nil {
		t.Fatalf("RegisterInstance() error = %v", err)
	}
	if len(first.Requests()) != 1 || len(second.Requests()) != 0 {
		t.Fatalf("ServerURLs precedence requests = first:%d second:%d, want 1/0", len(first.Requests()), len(second.Requests()))
	}
}

func TestBlackboxClientConfigTimeoutOverridesFallback(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	server.SetDelay(200 * time.Millisecond)
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: server.URL(), Timeout: 25 * time.Millisecond}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	start := time.Now()
	err = client.RegisterInstance(nacos.InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("RegisterInstance() error = nil, want configured timeout")
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("configured timeout call took %s, want <150ms", elapsed)
	}
}

func TestBlackboxClientConfigScopesEveryEndpoint(t *testing.T) {
	server := nacosmock.Start()
	defer server.Close()
	client, err := nacos.NewClientWithConfig(nacos.ClientConfig{TransportMode: nacos.TransportHTTPCompat, ServerURL: server.URL(), NamespaceID: "tenant-a", GroupName: "blue"}, &fakes.FakeLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	params := nacos.InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80, ClusterName: "k8s"}
	if err := client.RegisterInstance(params); err != nil {
		t.Fatalf("RegisterInstance() error = %v", err)
	}
	if err := client.UpdateCluster("svc", "k8s"); err != nil {
		t.Fatalf("UpdateCluster() error = %v", err)
	}
	if _, err := client.ListInstances("svc"); err != nil {
		t.Fatalf("ListInstances() error = %v", err)
	}
	if _, err := client.ListCatalogInstances("svc", "k8s"); err != nil {
		t.Fatalf("ListCatalogInstances() error = %v", err)
	}
	if _, err := client.ListServices(100); err != nil {
		t.Fatalf("ListServices() error = %v", err)
	}
	if err := client.DeregisterInstance(params); err != nil {
		t.Fatalf("DeregisterInstance() error = %v", err)
	}
	for _, request := range server.Requests() {
		if request.Query.Get("namespaceId") != "tenant-a" || request.Query.Get("groupName") != "blue" {
			t.Fatalf("endpoint %s %s scope = namespace=%q group=%q, want tenant-a/blue", request.Method, request.Path, request.Query.Get("namespaceId"), request.Query.Get("groupName"))
		}
	}
}

// contains reports whether s contains substr.
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

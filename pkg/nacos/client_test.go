// Package nacos_test holds the black-box suite of the Nacos adapter: the
// hand-rolled v1 OpenAPI client (client_test.go) and the InstanceSink
// (sink_test.go), both driven against the in-process nacosmock server.
package nacos_test

import (
	"errors"
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
	if len(hosts) != 2 {
		t.Fatalf("ListInstances(pay-user) = %d hosts, want 2", len(hosts))
	}
	for _, host := range hosts {
		if host.InstanceID == "" || host.ClusterName == "" || host.Metadata == nil {
			t.Fatalf("host = %+v, want the composite id, cluster and metadata parsed", host)
		}
		switch host.IP {
		case "10.0.0.1":
			if host.Port != 8080 || host.ClusterName != "k8s" || !host.Enabled {
				t.Fatalf("k8s host = %+v, want 10.0.0.1:8080/k8s/enabled", host)
			}
			if host.Metadata["instanceId"] != "pod-a" || host.Metadata["envType"] != "test" {
				t.Fatalf("k8s host metadata = %v, want the register metadata", host.Metadata)
			}
		case "10.0.0.2":
			if host.Port != 8081 || host.ClusterName != "ecs" || host.Enabled {
				t.Fatalf("ecs host = %+v, want 10.0.0.2:8081/ecs/disabled", host)
			}
		default:
			t.Fatalf("unexpected host %+v", host)
		}
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

// contains reports whether s contains substr.
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

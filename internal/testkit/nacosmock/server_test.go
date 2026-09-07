package nacosmock

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestRegisterUpsertsWithoutDuplicate(t *testing.T) {
	server := Start()
	defer server.Close()

	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP",
		Ephemeral: false, Enabled: true, Metadata: `{"instanceId":"pod-a"}`,
	})
	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP",
		Ephemeral: false, Enabled: true, Metadata: `{"instanceId":"pod-b"}`,
	})

	instances := server.Instances("pay-user", "k8s")
	if len(instances) != 1 {
		t.Fatalf("Instances(pay-user, k8s) = %d instances, want 1 (upsert, no duplicate)", len(instances))
	}
	// The last registration wins, exactly like the real register API.
	if got := instances[0].Metadata["instanceId"]; got != "pod-b" {
		t.Fatalf("upserted metadata instanceId = %q, want pod-b (last write wins)", got)
	}
}

func TestRegisterRejectsMissingParams(t *testing.T) {
	server := Start()
	defer server.Close()

	tests := []struct {
		name string
		form map[string]string
	}{
		{name: "missing serviceName", form: map[string]string{"ip": "10.0.0.1", "port": "8080", "clusterName": "k8s"}},
		{name: "missing ip", form: map[string]string{"serviceName": "pay-user", "port": "8080", "clusterName": "k8s"}},
		{name: "missing clusterName", form: map[string]string{"serviceName": "pay-user", "ip": "10.0.0.1", "port": "8080"}},
		{name: "invalid port", form: map[string]string{"serviceName": "pay-user", "ip": "10.0.0.1", "port": "http", "clusterName": "k8s"}},
		{name: "invalid metadata JSON", form: map[string]string{
			"serviceName": "pay-user", "ip": "10.0.0.1", "port": "8080", "clusterName": "k8s",
			"metadata": "{not-json",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body := doForm(t, server, http.MethodPost, "/nacos/v1/ns/instance", tt.form)
			if status != http.StatusBadRequest {
				t.Fatalf("POST /nacos/v1/ns/instance status = %d, want %d; body: %s", status, http.StatusBadRequest, body)
			}
		})
	}
}

func TestDeregisterRemovesInstance(t *testing.T) {
	server := Start()
	defer server.Close()

	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
	})
	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.2", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
	})
	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.3", Port: 8080,
		ClusterName: "ecs", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
	})

	// DELETE addresses exactly one composite id (ip, port, cluster, group,
	// service); the other k8s instance and the ecs instance survive.
	status, body := doForm(t, server, http.MethodDelete, "/nacos/v1/ns/instance", map[string]string{
		"serviceName": "pay-user", "ip": "10.0.0.1", "port": "8080",
		"clusterName": "k8s", "groupName": "DEFAULT_GROUP",
	})
	if status != http.StatusOK {
		t.Fatalf("DELETE /nacos/v1/ns/instance status = %d, want %d; body: %s", status, http.StatusOK, body)
	}

	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("k8s cluster instances = %d, want 1 (only 10.0.0.2 remains)", got)
	}
	if got := len(server.Instances("pay-user", "ecs")); got != 1 {
		t.Fatalf("ecs cluster instances = %d, want 1 (untouched by the k8s delete)", got)
	}
}

func TestDeregisterMissingParamsRejected(t *testing.T) {
	server := Start()
	defer server.Close()

	status, _ := doForm(t, server, http.MethodDelete, "/nacos/v1/ns/instance", map[string]string{"serviceName": "pay-user"})
	if status != http.StatusBadRequest {
		t.Fatalf("DELETE with missing params status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestListInstancesReturnsServiceInstances(t *testing.T) {
	server := Start()
	defer server.Close()

	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP",
		Ephemeral: false, Enabled: true, Metadata: `{"instanceId":"pod-a","envType":"test"}`,
	})
	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.3", Port: 8081,
		ClusterName: "ecs", GroupName: "DEFAULT_GROUP",
		Ephemeral: false, Enabled: false, Metadata: `{"instanceId":"srv-b","envType":"test"}`,
	})

	// The list endpoint answers for the whole service (group-scoped), and the
	// hosts shape matches the F0-observed v2.1.0 response.
	resp := listInstances(t, server, "pay-user", "DEFAULT_GROUP")
	if resp.Count != 2 {
		t.Fatalf("count = %d, want 2", resp.Count)
	}
	if len(resp.Hosts) != 2 {
		t.Fatalf("len(hosts) = %d, want 2", len(resp.Hosts))
	}
	for _, host := range resp.Hosts {
		if host.InstanceID == "" {
			t.Fatalf("host %v has an empty instanceId, want the composite id", host)
		}
		if host.Ephemeral {
			t.Fatalf("host %s ephemeral = true, want false (persistent)", host.InstanceID)
		}
		switch host.IP {
		case "10.0.0.1":
			if host.Port != 8080 || host.ClusterName != "k8s" || !host.Enabled || !host.Healthy {
				t.Fatalf("k8s host = %+v, want 10.0.0.1:8080/k8s/enabled/healthy", host)
			}
			if host.Metadata["instanceId"] != "pod-a" || host.Metadata["envType"] != "test" {
				t.Fatalf("k8s host metadata = %v, want the registered map", host.Metadata)
			}
		case "10.0.0.3":
			if host.Port != 8081 || host.ClusterName != "ecs" || host.Enabled {
				t.Fatalf("ecs host = %+v, want 10.0.0.3:8081/ecs/disabled", host)
			}
		default:
			t.Fatalf("unexpected host ip %q", host.IP)
		}
	}

	// A service with no instances answers with an empty hosts array, not 404.
	empty := listInstances(t, server, "ghost", "DEFAULT_GROUP")
	if empty.Count != 0 || len(empty.Hosts) != 0 {
		t.Fatalf("ghost service list = %+v, want count 0 and no hosts", empty)
	}
}

func TestListInstancesRequiresServiceParam(t *testing.T) {
	server := Start()
	defer server.Close()

	status, _ := doForm(t, server, http.MethodGet, "/nacos/v1/ns/instance/list", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("GET instance/list without serviceName status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestServiceListPaginates(t *testing.T) {
	server := Start()
	defer server.Close()

	// 5 services; pageSize 2 must need 3 pages (2+2+1) and end on the short
	// page. Service names are returned sorted for determinism.
	names := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e"}
	for _, name := range names {
		registerInstance(t, server, registerParams{
			ServiceName: name, IP: "10.0.0.1", Port: 8080,
			ClusterName: "k8s", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
		})
	}

	page1 := serviceList(t, server, 1, 2)
	if page1.Count != 5 {
		t.Fatalf("page1 count = %d, want 5 (the total service count)", page1.Count)
	}
	if !reflect.DeepEqual(page1.Doms, []string{"svc-a", "svc-b"}) {
		t.Fatalf("page1 doms = %v, want [svc-a svc-b]", page1.Doms)
	}

	page2 := serviceList(t, server, 2, 2)
	if !reflect.DeepEqual(page2.Doms, []string{"svc-c", "svc-d"}) {
		t.Fatalf("page2 doms = %v, want [svc-c svc-d]", page2.Doms)
	}

	// The short page ends iteration.
	page3 := serviceList(t, server, 3, 2)
	if !reflect.DeepEqual(page3.Doms, []string{"svc-e"}) {
		t.Fatalf("page3 doms = %v, want [svc-e]", page3.Doms)
	}

	// Beyond the end: an empty page.
	page4 := serviceList(t, server, 4, 2)
	if len(page4.Doms) != 0 {
		t.Fatalf("page4 doms = %v, want empty", page4.Doms)
	}

	// A pageNo past the end must still answer 200, so the client's
	// pagination loop terminates on a short page, not on an error.
	invalid := serviceList(t, server, 99, 2)
	if len(invalid.Doms) != 0 {
		t.Fatalf("page99 doms = %v, want empty", invalid.Doms)
	}
}

func TestServiceListRejectsMissingParams(t *testing.T) {
	server := Start()
	defer server.Close()

	status, _ := doForm(t, server, http.MethodGet, "/nacos/v1/ns/service/list", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("GET service/list without pageNo status = %d, want %d", status, http.StatusBadRequest)
	}
}

func TestRequestsRecording(t *testing.T) {
	server := Start()
	defer server.Close()

	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP",
		Ephemeral: false, Enabled: true, Metadata: `{"instanceId":"pod-a"}`,
	})
	_ = listInstances(t, server, "pay-user", "DEFAULT_GROUP")
	_ = serviceList(t, server, 1, 100)

	requests := server.Requests()
	if len(requests) != 3 {
		t.Fatalf("Requests() = %d entries, want 3 (register, list, service list)", len(requests))
	}

	// The register call: POST with the form on the query string.
	reg := requests[0]
	if reg.Method != http.MethodPost || reg.Path != "/nacos/v1/ns/instance" {
		t.Fatalf("register request = %s %s, want POST /nacos/v1/ns/instance", reg.Method, reg.Path)
	}
	if reg.Query.Get("serviceName") != "pay-user" || reg.Query.Get("ip") != "10.0.0.1" || reg.Query.Get("port") != "8080" {
		t.Fatalf("register query = %v, want the service/ip/port form", reg.Query)
	}
	if reg.Query.Get("clusterName") != "k8s" || reg.Query.Get("groupName") != "DEFAULT_GROUP" {
		t.Fatalf("register query = %v, want the cluster and group form", reg.Query)
	}
	if reg.Query.Get("ephemeral") != "false" {
		t.Fatalf("register ephemeral = %q, want false (the persistent-instance contract)", reg.Query.Get("ephemeral"))
	}
	var metadata map[string]string
	if err := json.Unmarshal([]byte(reg.Query.Get("metadata")), &metadata); err != nil {
		t.Fatalf("register metadata = %q, want URL-encoded JSON: %v", reg.Query.Get("metadata"), err)
	}
	if metadata["instanceId"] != "pod-a" {
		t.Fatalf("register metadata = %v, want instanceId pod-a", metadata)
	}

	// The instance list: GET with serviceName/groupName.
	list := requests[1]
	if list.Method != http.MethodGet || list.Path != "/nacos/v1/ns/instance/list" {
		t.Fatalf("list request = %s %s, want GET /nacos/v1/ns/instance/list", list.Method, list.Path)
	}
	if list.Query.Get("serviceName") != "pay-user" || list.Query.Get("groupName") != "DEFAULT_GROUP" {
		t.Fatalf("list query = %v, want the service/group form", list.Query)
	}

	// The service list: GET with pageNo/pageSize.
	svc := requests[2]
	if svc.Method != http.MethodGet || svc.Path != "/nacos/v1/ns/service/list" {
		t.Fatalf("service list request = %s %s, want GET /nacos/v1/ns/service/list", svc.Method, svc.Path)
	}
	if svc.Query.Get("pageNo") != "1" || svc.Query.Get("pageSize") != "100" {
		t.Fatalf("service list query = %v, want pageNo 1 / pageSize 100", svc.Query)
	}

	// Snapshots are independent: mutating one does not corrupt the server's
	// record.
	requests[0].Query.Set("serviceName", "tampered")
	if server.Requests()[0].Query.Get("serviceName") != "pay-user" {
		t.Fatal("Requests() snapshot leaked a mutation to the server's record")
	}
}

func TestFailureInjection(t *testing.T) {
	server := Start()
	defer server.Close()

	// SetStatus(500): every endpoint answers 500 with a JSON error body.
	server.SetStatus(http.StatusInternalServerError)
	status, body := doForm(t, server, http.MethodPost, "/nacos/v1/ns/instance", map[string]string{
		"serviceName": "pay-user", "ip": "10.0.0.1", "port": "8080", "clusterName": "k8s",
	})
	if status != http.StatusInternalServerError {
		t.Fatalf("injected status: POST answered %d, want 500", status)
	}
	if body == "" {
		t.Fatal("injected 500 body is empty, want a JSON error document")
	}

	// Clearing restores normal behavior.
	server.SetStatus(0)
	registerInstance(t, server, registerParams{
		ServiceName: "pay-user", IP: "10.0.0.1", Port: 8080,
		ClusterName: "k8s", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
	})
	if got := len(server.Instances("pay-user", "k8s")); got != 1 {
		t.Fatalf("instances after recovery = %d, want 1", got)
	}
}

func TestSetDelayObserved(t *testing.T) {
	server := Start()
	defer server.Close()
	server.SetDelay(50 * time.Millisecond)

	before := time.Now()
	_ = serviceList(t, server, 1, 100)
	elapsed := time.Since(before)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("SetDelay(50ms): request took %s, want >= 50ms", elapsed)
	}
}

func TestSetInstancesSeedsState(t *testing.T) {
	server := Start()
	defer server.Close()

	// SetInstances writes out-of-band state (drift / pre-seeded remote), in
	// the same shapes the list endpoint serves.
	server.SetInstances([]Host{
		{
			IP: "10.9.9.9", Port: 9100, ClusterName: "k8s",
			Enabled: true, Healthy: true, Ephemeral: false, Metadata: map[string]string{"instanceId": "ghost"},
		},
	}, "DEFAULT_GROUP", "pay-user", "k8s")

	resp := listInstances(t, server, "pay-user", "DEFAULT_GROUP")
	if resp.Count != 1 || len(resp.Hosts) != 1 {
		t.Fatalf("list after SetInstances = %+v, want one host", resp)
	}
	host := resp.Hosts[0]
	if host.IP != "10.9.9.9" || host.Port != 9100 || host.ClusterName != "k8s" {
		t.Fatalf("seeded host = %+v, want 10.9.9.9:9100/k8s", host)
	}
	if host.Metadata["instanceId"] != "ghost" {
		t.Fatalf("seeded host metadata = %v, want instanceId ghost", host.Metadata)
	}
	// The seeded instance participates in the service list and in the
	// composite-id derivation used by SetInstances.
	svc := serviceList(t, server, 1, 100)
	if len(svc.Doms) != 1 || svc.Doms[0] != "pay-user" {
		t.Fatalf("service list after SetInstances = %v, want [pay-user]", svc.Doms)
	}
}

func TestInstanceIDComposite(t *testing.T) {
	server := Start()
	defer server.Close()

	server.SetInstances([]Host{{IP: "10.0.0.1", Port: 8080, Metadata: map[string]string{}}}, "DEFAULT_GROUP", "pay-user", "k8s")
	resp := listInstances(t, server, "pay-user", "DEFAULT_GROUP")
	if got := resp.Hosts[0].InstanceID; got != "10.0.0.1#8080#k8s#DEFAULT_GROUP@@pay-user" {
		t.Fatalf("instanceId = %q, want the F0 composite format", got)
	}
}

func TestHealthReadiness(t *testing.T) {
	server := Start()
	defer server.Close()

	status, body := doForm(t, server, http.MethodGet, "/nacos/v1/console/health/readiness", nil)
	if status != http.StatusOK {
		t.Fatalf("readiness status = %d, want %d; body: %s", status, http.StatusOK, body)
	}
}

func TestConcurrentAccess(t *testing.T) {
	server := Start()
	defer server.Close()

	const writers = 8
	const writes = 25
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < writes; j++ {
				port := 8000 + j
				registerInstance(t, server, registerParams{
					ServiceName: "pay-user", IP: "10.0.0.1", Port: port,
					ClusterName: "k8s", GroupName: "DEFAULT_GROUP", Ephemeral: false, Enabled: true,
				})
				_ = listInstances(t, server, "pay-user", "DEFAULT_GROUP")
				_ = server.Instances("pay-user", "k8s")
				_ = server.Requests()
			}
		}(i)
	}
	wg.Wait()

	if got := len(server.Instances("pay-user", "k8s")); got != writes {
		t.Fatalf("instances after concurrent registration = %d, want %d (one per distinct port)", got, writes)
	}
}

func TestCloseIdempotent(t *testing.T) {
	server := Start()
	server.Close()
	server.Close()
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

type registerParams struct {
	ServiceName string
	IP          string
	Port        int
	ClusterName string
	GroupName   string
	Ephemeral   bool
	Enabled     bool
	Metadata    string // raw JSON, URL-encoded on the wire
}

func registerInstance(t *testing.T, server *Server, params registerParams) {
	t.Helper()
	form := map[string]string{
		"serviceName": params.ServiceName,
		"ip":          params.IP,
		"port":        strconv.Itoa(params.Port),
		"clusterName": params.ClusterName,
		"groupName":   params.GroupName,
		"ephemeral":   strconv.FormatBool(params.Ephemeral),
		"enabled":     strconv.FormatBool(params.Enabled),
	}
	if params.Metadata != "" {
		form["metadata"] = params.Metadata
	}
	status, body := doForm(t, server, http.MethodPost, "/nacos/v1/ns/instance", form)
	if status != http.StatusOK {
		t.Fatalf("register %s/%s status = %d, want %d; body: %s",
			params.ServiceName, params.IP, status, http.StatusOK, body)
	}
	if body != "ok" {
		t.Fatalf("register body = %q, want the Nacos success text %q", body, "ok")
	}
}

// doForm issues one request against the server with the given form values
// URL-encoded onto the query string, mirroring the v1 OpenAPI convention the
// production client uses, and returns the status code and body.
func doForm(t *testing.T, server *Server, method, path string, form map[string]string) (int, string) {
	t.Helper()
	target := server.URL() + path
	if len(form) > 0 {
		values := make(url.Values, len(form))
		for key, value := range form {
			values.Set(key, value)
		}
		target += "?" + values.Encode()
	}
	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		t.Fatalf("http.NewRequest(%s %s) error = %v", method, target, err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("%s %s error = %v", method, target, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s %s body: %v", method, target, err)
	}
	return response.StatusCode, string(body)
}

func listInstances(t *testing.T, server *Server, serviceName, groupName string) instanceListResponse {
	t.Helper()
	status, body := doForm(t, server, http.MethodGet, "/nacos/v1/ns/instance/list", map[string]string{
		"serviceName": serviceName,
		"groupName":   groupName,
	})
	if status != http.StatusOK {
		t.Fatalf("GET instance/list(%s) status = %d, want %d; body: %s", serviceName, status, http.StatusOK, body)
	}
	var resp instanceListResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("GET instance/list(%s) body is not the hosts JSON shape: %v; body: %s", serviceName, err, body)
	}
	return resp
}

func serviceList(t *testing.T, server *Server, pageNo, pageSize int) serviceListResponse {
	t.Helper()
	status, body := doForm(t, server, http.MethodGet, "/nacos/v1/ns/service/list", map[string]string{
		"pageNo":   strconv.Itoa(pageNo),
		"pageSize": strconv.Itoa(pageSize),
	})
	if status != http.StatusOK {
		t.Fatalf("GET service/list(%d, %d) status = %d, want %d; body: %s", pageNo, pageSize, status, http.StatusOK, body)
	}
	var resp serviceListResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("GET service/list body is not the doms JSON shape: %v; body: %s", err, body)
	}
	return resp
}

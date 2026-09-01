package consulmock

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/hashicorp/consul/api"
)

func TestServerSupportsConsulClient(t *testing.T) {
	server := Start()
	defer server.Close()

	if server.URL() != server.Address() {
		t.Fatalf("Address() = %q, want URL() %q", server.Address(), server.URL())
	}
	if !strings.HasPrefix(server.Address(), "http://127.0.0.1:") {
		t.Fatalf("Address() = %q, want loopback HTTP URL", server.Address())
	}

	client := newClient(t, server)

	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatalf("get default leader: %v", err)
	}
	if leader != "127.0.0.1:8300" {
		t.Fatalf("default leader = %q, want %q", leader, "127.0.0.1:8300")
	}

	server.SetLeader("127.0.0.2:8300")
	leader, err = client.Status().Leader()
	if err != nil {
		t.Fatalf("get updated leader: %v", err)
	}
	if leader != "127.0.0.2:8300" {
		t.Fatalf("updated leader = %q, want %q", leader, "127.0.0.2:8300")
	}

	services := map[string][]string{
		"payments": {"microservice", "v1"},
		"search":   {"microservice"},
	}
	server.SetServices(services)
	services["payments"][0] = "changed"
	services["added"] = []string{"changed"}

	gotServices, _, err := client.Catalog().Services(nil)
	if err != nil {
		t.Fatalf("get services: %v", err)
	}
	wantServices := map[string][]string{
		"payments": {"microservice", "v1"},
		"search":   {"microservice"},
	}
	if !reflect.DeepEqual(gotServices, wantServices) {
		t.Fatalf("services = %#v, want %#v", gotServices, wantServices)
	}
	gotServices["payments"][0] = "response changed"
	gotServicesAgain, _, err := client.Catalog().Services(nil)
	if err != nil {
		t.Fatalf("get services again: %v", err)
	}
	if !reflect.DeepEqual(gotServicesAgain, wantServices) {
		t.Fatalf("services after response mutation = %#v, want %#v", gotServicesAgain, wantServices)
	}
}

func TestServerHealthStateTracksIndex(t *testing.T) {
	server := Start()
	defer server.Close()
	client := newClient(t, server)

	checks := []*api.HealthCheck{{
		Node:        "node-1",
		CheckID:     "service:payments-1",
		Name:        "Service 'payments' check",
		Status:      api.HealthPassing,
		ServiceID:   "payments-1",
		ServiceName: "payments",
		ServiceTags: []string{"microservice"},
	}}
	server.SetHealthState(checks)
	checks[0].Node = "changed"
	checks[0].ServiceTags[0] = "changed"

	initialIndex := server.Index()
	gotChecks, meta, err := client.Health().State(api.HealthAny, &api.QueryOptions{
		WaitIndex: initialIndex,
	})
	if err != nil {
		t.Fatalf("get health state: %v", err)
	}
	if meta.LastIndex != initialIndex {
		t.Fatalf("health index = %d, want %d", meta.LastIndex, initialIndex)
	}
	if len(gotChecks) != 1 || gotChecks[0].Node != "node-1" || gotChecks[0].ServiceTags[0] != "microservice" {
		t.Fatalf("health checks = %#v, want copied input", gotChecks)
	}

	gotChecks[0].Node = "response changed"
	nextIndex := server.AdvanceIndex()
	if nextIndex != initialIndex+1 || server.Index() != nextIndex {
		t.Fatalf("advanced index = %d and current index = %d, want %d", nextIndex, server.Index(), initialIndex+1)
	}
	gotChecksAgain, nextMeta, err := client.Health().State(api.HealthAny, &api.QueryOptions{
		WaitIndex: initialIndex,
	})
	if err != nil {
		t.Fatalf("get advanced health state: %v", err)
	}
	if nextMeta.LastIndex != nextIndex {
		t.Fatalf("advanced health index = %d, want %d", nextMeta.LastIndex, nextIndex)
	}
	if gotChecksAgain[0].Node != "node-1" {
		t.Fatalf("health state retained response mutation: node = %q", gotChecksAgain[0].Node)
	}
}

func TestServerHealthServiceFiltersAndCapturesQuery(t *testing.T) {
	server := Start()
	defer server.Close()
	client := newClient(t, server)

	entries := []*api.ServiceEntry{
		serviceEntry("payments-1", []string{"microservice", "v1"}, api.HealthPassing),
		serviceEntry("payments-2", []string{"other"}, api.HealthPassing),
		serviceEntry("payments-3", []string{"microservice"}, api.HealthCritical),
	}
	server.SetEntries("payments", entries)
	entries[0].Service.ID = "changed"
	entries[0].Service.Tags[0] = "changed"

	gotEntries, meta, err := client.Health().Service("payments", "microservice", true, nil)
	if err != nil {
		t.Fatalf("get service health: %v", err)
	}
	if meta.LastIndex != server.Index() {
		t.Fatalf("service health index = %d, want %d", meta.LastIndex, server.Index())
	}
	if len(gotEntries) != 1 || gotEntries[0].Service.ID != "payments-1" {
		t.Fatalf("filtered entries = %#v, want only payments-1", gotEntries)
	}
	gotEntries[0].Service.ID = "response changed"

	gotEntriesAgain, _, err := client.Health().Service("payments", "microservice", true, nil)
	if err != nil {
		t.Fatalf("get service health again: %v", err)
	}
	if gotEntriesAgain[0].Service.ID != "payments-1" {
		t.Fatalf("service entries retained response mutation: ID = %q", gotEntriesAgain[0].Service.ID)
	}

	requests := server.Requests()
	request := lastRequestForPath(t, requests, "/v1/health/service/payments")
	if request.Method != "GET" {
		t.Fatalf("request method = %q, want GET", request.Method)
	}
	if got := request.Query.Get("tag"); got != "microservice" {
		t.Fatalf("tag query = %q, want microservice", got)
	}
	if got := request.Query.Get("passing"); got != "1" {
		t.Fatalf("passing query = %q, want 1", got)
	}

	request.Query.Set("tag", "changed")
	requestAgain := lastRequestForPath(t, server.Requests(), "/v1/health/service/payments")
	if got := requestAgain.Query.Get("tag"); got != "microservice" {
		t.Fatalf("captured request retained caller mutation: tag = %q", got)
	}
}

func TestServerConcurrentAccessAndClose(t *testing.T) {
	server := Start()
	client := newClient(t, server)

	const workers = 6
	const iterations = 20
	errors := make(chan error, workers*2)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(2)
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				id := fmt.Sprintf("service-%d-%d", worker, iteration)
				server.SetLeader("127.0.0.1:" + strconv.Itoa(8300+worker))
				server.SetServices(map[string][]string{id: {"microservice"}})
				server.SetEntries(id, []*api.ServiceEntry{
					serviceEntry(id, []string{"microservice"}, api.HealthPassing),
				})
				server.SetHealthState([]*api.HealthCheck{{CheckID: id, Status: api.HealthPassing}})
				server.AdvanceIndex()
			}
		}()
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				if _, err := client.Status().Leader(); err != nil {
					errors <- err
					return
				}
				if _, _, err := client.Catalog().Services(nil); err != nil {
					errors <- err
					return
				}
				if _, _, err := client.Health().State(api.HealthAny, nil); err != nil {
					errors <- err
					return
				}
				_ = server.Index()
				_ = server.Requests()
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent client request: %v", err)
	}

	var closeWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		closeWG.Add(1)
		go func() {
			defer closeWG.Done()
			server.Close()
		}()
	}
	closeWG.Wait()
	server.Close()
}

func newClient(t *testing.T, server *Server) *api.Client {
	t.Helper()
	config := api.DefaultConfig()
	config.Address = server.Address()
	client, err := api.NewClient(config)
	if err != nil {
		t.Fatalf("create Consul client: %v", err)
	}
	return client
}

func serviceEntry(id string, tags []string, status string) *api.ServiceEntry {
	return &api.ServiceEntry{
		Node: &api.Node{Node: "node-1", Address: "127.0.0.1"},
		Service: &api.AgentService{
			ID:      id,
			Service: "payments",
			Tags:    tags,
			Address: "127.0.0.1",
			Port:    8080,
		},
		Checks: api.HealthChecks{{
			CheckID:   "service:" + id,
			ServiceID: id,
			Status:    status,
		}},
	}
}

func lastRequestForPath(t *testing.T, requests []Request, path string) Request {
	t.Helper()
	for i := len(requests) - 1; i >= 0; i-- {
		if requests[i].Path == path {
			return requests[i]
		}
	}
	t.Fatalf("no request captured for %s", path)
	return Request{}
}

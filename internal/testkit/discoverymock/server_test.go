package discoverymock

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"spotter/internal/domain/instance"
)

func TestServerInvokesDiscoveryMethods(t *testing.T) {
	server, err := Start()
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer server.Close()

	conn, err := server.DialContext(context.Background())
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}
	defer conn.Close()

	server.SetResponseCode(23, "rejected")
	input := testInstance("syn", 1, "ecs")
	var syncResponse instance.CommonResponse
	if err := conn.Invoke(context.Background(), "/service.v2.InstanceService/SynInstance", &instance.SynInstancesRequest{
		Instance: []*instance.Instance{input},
	}, &syncResponse); err != nil {
		t.Fatalf("invoke SynInstance: %v", err)
	}
	if syncResponse.Code != 23 || syncResponse.Msg != "rejected" {
		t.Fatalf("SynInstance response = %#v, want code 23 and rejected", syncResponse)
	}

	var syncAllResponse instance.CommonResponse
	if err := conn.Invoke(context.Background(), "/service.v2.InstanceService/SynAllInstance", &instance.SynAllInstancesRequest{
		Instance: []*instance.Instance{testInstance("all", 1, "ecs")},
	}, &syncAllResponse); err != nil {
		t.Fatalf("invoke SynAllInstance: %v", err)
	}
	if syncAllResponse.Code != 23 || syncAllResponse.Msg != "rejected" {
		t.Fatalf("SynAllInstance response = %#v, want code 23 and rejected", syncAllResponse)
	}

	instances := []*instance.Instance{
		testInstance("wanted", 7, "ecs"),
		testInstance("other-provider", 7, "k8s"),
		testInstance("other-status", 8, "ecs"),
	}
	server.SetInstances(instances)
	instances[0].Label["changed"] = "caller"
	instances[0].Ports[0].Port = 9999

	var list instance.InstanceList
	if err := conn.Invoke(context.Background(), "/service.v2.InstanceService/GetAllInstance", &instance.GetAllInstancesRequest{
		Status:   7,
		Provider: "ecs",
	}, &list); err != nil {
		t.Fatalf("invoke GetAllInstance: %v", err)
	}
	if len(list.Instance) != 1 || list.Instance[0].InstanceId != "wanted" {
		t.Fatalf("filtered instances = %#v, want wanted only", list.Instance)
	}
	if list.Instance[0].Ports[0].Port != 8080 || list.Instance[0].Label["role"] != "api" {
		t.Fatalf("SetInstances retained caller mutation: %#v", list.Instance[0])
	}

	list.Instance[0].Label["role"] = "response"
	list.Instance[0].Ports[0].Port = 1234
	var listAgain instance.InstanceList
	if err := conn.Invoke(context.Background(), "/service.v2.InstanceService/GetAllInstance", &instance.GetAllInstancesRequest{
		Status: 7,
	}, &listAgain); err != nil {
		t.Fatalf("invoke GetAllInstance without provider: %v", err)
	}
	if len(listAgain.Instance) != 2 {
		t.Fatalf("status-only instances = %#v, want two instances", listAgain.Instance)
	}
	if listAgain.Instance[0].Label["role"] != "api" || listAgain.Instance[0].Ports[0].Port != 8080 {
		t.Fatalf("response mutation leaked into server state: %#v", listAgain.Instance[0])
	}

	calls := server.Calls()
	if len(calls) != 4 {
		t.Fatalf("captured %d calls, want 4", len(calls))
	}
	if calls[0].Method != "SynInstance" || calls[1].Method != "SynAllInstance" {
		t.Fatalf("captured methods = %#v, want SynInstance and SynAllInstance first", calls)
	}
	if !reflect.DeepEqual(calls[0].Instances, []*instance.Instance{testInstance("syn", 1, "ecs")}) {
		t.Fatalf("captured SynInstance input = %#v, want deep copy", calls[0].Instances)
	}
	if calls[2].Status != 7 || calls[2].Provider != "ecs" {
		t.Fatalf("captured filtered request = %#v, want status/provider", calls[2])
	}
	if calls[3].Status != 7 || calls[3].Provider != "" {
		t.Fatalf("captured status-only request = %#v, want empty provider", calls[3])
	}

	calls[0].Instances[0].Label["role"] = "calls mutation"
	if server.Calls()[0].Instances[0].Label["role"] != "api" {
		t.Fatal("Calls returned a view into server state")
	}
}

func TestServerSupportsConcurrentCallsAndIdempotentClose(t *testing.T) {
	server, err := Start()
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	conn, err := server.DialContext(context.Background())
	if err != nil {
		t.Fatalf("dial server: %v", err)
	}

	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			var response instance.CommonResponse
			errs <- conn.Invoke(context.Background(), "/service.v2.InstanceService/SynInstance", &instance.SynInstancesRequest{
				Instance: []*instance.Instance{testInstance("concurrent", int32(i), "ecs")},
			}, &response)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent invoke: %v", err)
		}
	}
	if got := len(server.Calls()); got != workers {
		t.Fatalf("captured calls = %d, want %d", got, workers)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
	server.Close()
	server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closedConn, err := server.DialContext(ctx)
	if err == nil {
		closedConn.Close()
		t.Fatal("DialContext after Close succeeded")
	}
}

func testInstance(id string, status int32, provider string) *instance.Instance {
	return &instance.Instance{
		InstanceId: id,
		Status:     status,
		Provider:   provider,
		Ports: []*instance.PortInfo{{
			Name: "http",
			Port: 8080,
		}},
		Label: map[string]string{
			"role": "api",
		},
		Image: map[string]string{
			"name": "base",
		},
	}
}

package discoverycenter

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/fakes"
	v2 "spotter/pkg/beehive/service/v2"
)

// The black-box tier drives the exported surface of package discoverycenter
// (NewClient, NewDiscoveryCenter, Push, PushAll, GetAll) against the
// in-process discoverymock gRPC server, using only fakes for the logger and
// the notifier. All listeners are bufconn: nothing leaves the process.

// blackboxServiceClient adapts a *grpc.ClientConn to the v2.InstanceServiceClient
// contract by delegating to conn.Invoke with the same method paths as the
// unexported grpcServiceClient in package discoverycenter. The discoverymock
// JSON codec handles the wire format. This mirrors the wrapper pattern used
// by tests/e2e (see tests/e2e/consul_pipeline_test.go), which is the only way
// an external black-box consumer can reach Client without Dial.
type blackboxServiceClient struct {
	conn *grpc.ClientConn
}

func (c *blackboxServiceClient) SynInstance(ctx context.Context, request *v2.SynInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *blackboxServiceClient) SynAllInstance(ctx context.Context, request *v2.SynAllInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *blackboxServiceClient) GetAllInstance(ctx context.Context, request *v2.GetAllInstancesRequest, opts ...grpc.CallOption) (*v2.InstanceList, error) {
	response := new(v2.InstanceList)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/GetAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

// newBlackboxRegistry assembles the exported client/registry stack over a
// fresh discoverymock and returns the pieces the tests assert against.
func newBlackboxRegistry(t *testing.T, disablePush bool) (*discoverymock.Server, *DiscoveryCenter, *fakes.FakeLogger, *fakes.FakeNotifier) {
	t.Helper()

	server, err := discoverymock.Start()
	if err != nil {
		t.Fatalf("discoverymock.Start() error = %v", err)
	}
	t.Cleanup(server.Close)

	conn, err := server.DialContext(context.Background())
	if err != nil {
		t.Fatalf("discoverymock.DialContext() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client, err := NewClient(&blackboxServiceClient{conn: conn}, nil, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}
	registry, err := NewDiscoveryCenter(client, logger, notifier, disablePush)
	if err != nil {
		t.Fatalf("NewDiscoveryCenter() error = %v", err)
	}
	return server, registry, logger, notifier
}

// TestBlackboxPushSuccessSendsExactInstancesAndReturnsNil: a successful Push
// forwards the exact instances to SynInstance on the wire, returns nil, and
// emits no notification.
func TestBlackboxPushSuccessSendsExactInstancesAndReturnsNil(t *testing.T) {
	server, registry, _, notifier := newBlackboxRegistry(t, false)
	instances := []*v2.Instance{{
		InstanceId: "payments-1",
		AppCode:    "payments",
		Provider:   "ecs",
		EnvCode:    "test#0",
		Ip:         "127.0.0.1",
		Reversion:  42,
		Status:     1,
		Ports:      []*v2.PortInfo{{Name: "http", Protocol: "http", Port: 8080}},
	}}

	if err := registry.Push(123, instances); err != nil {
		t.Fatalf("Push() error = %v, want nil", err)
	}

	calls := server.Calls()
	if len(calls) != 1 {
		t.Fatalf("discoverymock calls = %#v, want exactly one SynInstance call", calls)
	}
	if calls[0].Method != "SynInstance" {
		t.Fatalf("method = %q, want SynInstance", calls[0].Method)
	}
	got := calls[0].Instances
	if len(got) != 1 {
		t.Fatalf("SynInstance payload = %#v, want exactly one instance", got)
	}
	if got[0].InstanceId != "payments-1" || got[0].AppCode != "payments" || got[0].Provider != "ecs" {
		t.Fatalf("SynInstance instance = %#v, want the exact submitted instance", got[0])
	}
	if got[0].Reversion != 42 || got[0].Ip != "127.0.0.1" || got[0].EnvCode != "test#0" {
		t.Fatalf("SynInstance instance = %#v, want the exact submitted fields", got[0])
	}
	if len(got[0].Ports) != 1 || got[0].Ports[0].Port != 8080 || got[0].Ports[0].Name != "http" || got[0].Ports[0].Protocol != "http" {
		t.Fatalf("SynInstance ports = %#v, want the exact submitted ports", got[0].Ports)
	}
	if notifications := notifier.Notifications(); len(notifications) != 0 {
		t.Fatalf("notifications = %#v, want none on success", notifications)
	}
}

// TestBlackboxPushNonzeroCodeReturnsErrorWithCodeAndMsgAndNotifies: when the
// mock answers with a nonzero response code, Push returns an error carrying
// both the code and the message, and the notifier receives exactly one
// notification titled "Failed to sync data incrementally".
func TestBlackboxPushNonzeroCodeReturnsErrorWithCodeAndMsgAndNotifies(t *testing.T) {
	server, registry, _, notifier := newBlackboxRegistry(t, false)
	server.SetResponseCode(17, "rejected")

	err := registry.Push(123, []*v2.Instance{{InstanceId: "instance-1"}})
	if err == nil {
		t.Fatal("Push() error = nil, want non-nil for nonzero response code")
	}
	wantMsg := "SynInstance failed with code: 17,error: rejected"
	if err.Error() != wantMsg {
		t.Fatalf("Push() error = %q, want %q (response code and message included)", err.Error(), wantMsg)
	}

	notifications := notifier.Notifications()
	if len(notifications) != 1 {
		t.Fatalf("notifications = %#v, want exactly one", notifications)
	}
	if notifications[0].Title != "Failed to sync data incrementally" {
		t.Fatalf("notification title = %q, want %q", notifications[0].Title, "Failed to sync data incrementally")
	}
	if notifications[0].Content != wantMsg {
		t.Fatalf("notification content = %q, want %q", notifications[0].Content, wantMsg)
	}
}

// TestBlackboxPushAllNonzeroCodeReturnsErrorAndNotifies: the same contract as
// the incremental push, but through PushAll: nonzero code produces an error
// with code and message plus exactly one "Failed to sync all data" notice.
func TestBlackboxPushAllNonzeroCodeReturnsErrorAndNotifies(t *testing.T) {
	server, registry, _, notifier := newBlackboxRegistry(t, false)
	server.SetResponseCode(23, "full rejected")

	err := registry.PushAll(456, []*v2.Instance{{InstanceId: "instance-all"}})
	if err == nil {
		t.Fatal("PushAll() error = nil, want non-nil for nonzero response code")
	}
	wantMsg := "SynAllInstance failed with code: 23,error: full rejected"
	if err.Error() != wantMsg {
		t.Fatalf("PushAll() error = %q, want %q", err.Error(), wantMsg)
	}

	notifications := notifier.Notifications()
	if len(notifications) != 1 {
		t.Fatalf("notifications = %#v, want exactly one", notifications)
	}
	if notifications[0].Title != "Failed to sync all data" {
		t.Fatalf("notification title = %q, want %q", notifications[0].Title, "Failed to sync all data")
	}
	if notifications[0].Content != wantMsg {
		t.Fatalf("notification content = %q, want %q", notifications[0].Content, wantMsg)
	}
}

// TestBlackboxGetAllAccumulatesProviderFilteredInstancesAcrossStatuses:
// GetAll issues one GetAllInstance RPC per status and accumulates only the
// instances that match the requested provider, across both statuses.
func TestBlackboxGetAllAccumulatesProviderFilteredInstancesAcrossStatuses(t *testing.T) {
	server, registry, _, _ := newBlackboxRegistry(t, false)
	server.SetInstances([]*instance.Instance{
		{InstanceId: "enabled-ecs", Provider: "ecs", Status: 1},
		{InstanceId: "disabled-ecs", Provider: "ecs", Status: 2},
		{InstanceId: "enabled-k8s", Provider: "k8s", Status: 1},
		{InstanceId: "disabled-k8s", Provider: "k8s", Status: 2},
	})

	list, err := registry.GetAll([]int32{1, 2}, "ecs")
	if err != nil {
		t.Fatalf("GetAll() error = %v, want nil", err)
	}

	if len(list.Instance) != 2 {
		t.Fatalf("GetAll() instances = %#v, want the two ecs instances", list.Instance)
	}
	seen := map[string]int32{}
	for _, item := range list.Instance {
		seen[item.InstanceId] = item.Status
	}
	for _, want := range []string{"enabled-ecs", "disabled-ecs"} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("GetAll() missing %q, got %#v", want, list.Instance)
		}
	}
	if _, ok := seen["enabled-k8s"]; ok {
		t.Fatalf("GetAll() returned a k8s instance, want provider-filtered ecs only: %#v", list.Instance)
	}

	calls := server.Calls()
	if len(calls) != 2 {
		t.Fatalf("discoverymock calls = %#v, want one GetAllInstance call per status", calls)
	}
	for i, status := range []int32{1, 2} {
		if calls[i].Method != "GetAllInstance" {
			t.Fatalf("call %d method = %q, want GetAllInstance", i, calls[i].Method)
		}
		if calls[i].Status != status {
			t.Fatalf("call %d status = %d, want %d", i, calls[i].Status, status)
		}
		if calls[i].Provider != "ecs" {
			t.Fatalf("call %d provider = %q, want ecs", i, calls[i].Provider)
		}
	}
}

// TestBlackboxDisablePushNeverReachesRPC: with disablePush set, Push returns
// nil and the discoverymock records no RPC of any method.
func TestBlackboxDisablePushNeverReachesRPC(t *testing.T) {
	server, registry, _, notifier := newBlackboxRegistry(t, true)

	if err := registry.Push(123, []*v2.Instance{{InstanceId: "instance-1"}}); err != nil {
		t.Fatalf("Push() error = %v, want nil when push is disabled", err)
	}

	calls := server.Calls()
	if len(calls) != 0 {
		t.Fatalf("discoverymock calls = %#v, want no RPC when push is disabled", calls)
	}
	if notifications := notifier.Notifications(); len(notifications) != 0 {
		t.Fatalf("notifications = %#v, want none when push is disabled", notifications)
	}
}

package discoverycenter

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/fakes"
)

func TestNewClientRejectsNilService(t *testing.T) {
	client, err := NewClient(nil, nil, nil)
	if err == nil {
		t.Fatal("NewClient() error = nil, want non-nil")
	}
	if client != nil {
		t.Fatalf("NewClient() client = %#v, want nil", client)
	}
}

func TestClientSyncCapturesCallAndRecordsMetrics(t *testing.T) {
	server, client := newMockClient(t)
	metrics := fakes.NewFakeMetricsRecorder()
	client.metrics = metrics
	instances := []*instance.Instance{{
		InstanceId: "instance-1",
		Provider:   "ecs",
		Status:     1,
		Reversion:  42,
	}}

	response, err := client.Sync(instances)
	if err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if response.GetCode() != 0 || response.GetMsg() != "" {
		t.Fatalf("Sync() response = %#v, want successful response", response)
	}

	calls := server.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(calls))
	}
	if calls[0].Method != "SynInstance" {
		t.Fatalf("method = %q, want SynInstance", calls[0].Method)
	}
	if len(calls[0].Instances) != 1 || calls[0].Instances[0].InstanceId != "instance-1" || calls[0].Instances[0].Reversion != 42 {
		t.Fatalf("captured instances = %#v, want submitted instance", calls[0].Instances)
	}
	if len(metrics.SyncOnceDurations()) != 1 {
		t.Fatalf("sync duration observations = %d, want 1", len(metrics.SyncOnceDurations()))
	}
	if metrics.SyncOnceCount() != 1 {
		t.Fatalf("sync once count = %d, want 1", metrics.SyncOnceCount())
	}
}

func TestClientSyncReturnsErrorForNonzeroResponseCode(t *testing.T) {
	server, client := newMockClient(t)
	server.SetResponseCode(17, "rejected")

	response, err := client.Sync([]*instance.Instance{{InstanceId: "instance-1"}})
	if err == nil {
		t.Fatal("Sync() error = nil, want non-nil")
	}
	if got, want := err.Error(), "SynInstance failed with code: 17,error: rejected"; got != want {
		t.Fatalf("Sync() error = %q, want %q", got, want)
	}
	if response.GetCode() != 17 || response.GetMsg() != "rejected" {
		t.Fatalf("Sync() response = %#v, want configured response", response)
	}
}

func TestClientSyncAllCapturesCall(t *testing.T) {
	server, client := newMockClient(t)
	instances := []*instance.Instance{{InstanceId: "instance-all", Provider: "k8s"}}

	response, err := client.SyncAll(instances)
	if err != nil {
		t.Fatalf("SyncAll() error = %v", err)
	}
	if response.GetCode() != 0 {
		t.Fatalf("SyncAll() code = %d, want 0", response.GetCode())
	}
	calls := server.Calls()
	if len(calls) != 1 || calls[0].Method != "SynAllInstance" {
		t.Fatalf("calls = %#v, want one SynAllInstance call", calls)
	}
	if len(calls[0].Instances) != 1 || calls[0].Instances[0].InstanceId != "instance-all" {
		t.Fatalf("captured instances = %#v, want submitted instance", calls[0].Instances)
	}
}

func TestClientGetAllAccumulatesStatusesForProvider(t *testing.T) {
	server, client := newMockClient(t)
	server.SetInstances([]*instance.Instance{
		{InstanceId: "enabled-ecs", Provider: "ecs", Status: 1},
		{InstanceId: "disabled-ecs", Provider: "ecs", Status: 2},
		{InstanceId: "enabled-k8s", Provider: "k8s", Status: 1},
	})

	list, err := client.GetAll([]int32{1, 2}, "ecs")
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(list.Instance) != 2 || list.Instance[0].InstanceId != "enabled-ecs" || list.Instance[1].InstanceId != "disabled-ecs" {
		t.Fatalf("GetAll() instances = %#v, want both ecs statuses", list.Instance)
	}

	calls := server.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls = %d, want 2", len(calls))
	}
	for i, status := range []int32{1, 2} {
		if calls[i].Method != "GetAllInstance" || calls[i].Status != status || calls[i].Provider != "ecs" {
			t.Fatalf("call %d = %#v, want status %d and provider ecs", i, calls[i], status)
		}
	}
}

func TestClientPropagatesRPCErrorWithTenSecondDeadline(t *testing.T) {
	service := &deadlineService{err: context.DeadlineExceeded}
	client, err := NewClient(service, nil, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	started := time.Now()
	_, err = client.Sync(nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Sync() error = %v, want context deadline exceeded", err)
	}
	if service.deadline.IsZero() {
		t.Fatal("Sync() did not set a deadline")
	}
	remaining := service.deadline.Sub(started)
	if remaining < 9*time.Second || remaining > 11*time.Second {
		t.Fatalf("Sync() deadline offset = %v, want approximately 10s", remaining)
	}
}

func TestDialPropagatesContextErrorWithoutPanic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	client, err := Dial(
		ctx,
		"discoverymock",
		nil,
		nil,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}),
	)
	if err == nil {
		t.Fatal("Dial() error = nil, want non-nil")
	}
	if client != nil {
		t.Fatalf("Dial() client = %#v, want nil", client)
	}
}

// TestDialZeroOptionsForcesJSONCodecAgainstTCPServer is the B2 regression
// test (plan §8.3): the production zero-option Dial branch must force the
// JSON codec, so the real client can talk to a server speaking the
// documented service.v2 wire contract (plain mirror structs, not proto
// messages). The server is the discoverymock TCP variant — the same shape
// the soak harness uses as its Atlas stand-in — so the test proves the
// exact production path end to end: Dial (no options) -> Sync -> SyncAll ->
// GetAll, with every call observed and every instance field round-tripped.
func TestDialZeroOptionsForcesJSONCodecAgainstTCPServer(t *testing.T) {
	server, err := discoverymock.StartTCP("127.0.0.1:0")
	if err != nil {
		t.Fatalf("discoverymock.StartTCP() error = %v", err)
	}
	t.Cleanup(server.Close)
	addr := server.Addr()
	if addr == "" {
		t.Fatal("discoverymock.StartTCP() served no address")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, addr, &fakes.FakeLogger{}, nil) // zero options: the production path
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	pushed := []*instance.Instance{{
		InstanceId: "i-b2-1",
		AppCode:    "b2-app",
		Ip:         "10.1.2.3",
		EnvType:    "test",
		Provider:   instance.ProviderK8s,
		Status:     instance.InstanceStatusOnline,
		Reversion:  7,
		Ports:      []*instance.PortInfo{{Name: "http", Protocol: "http", Port: 8080}},
	}}
	if _, err := client.Sync(pushed); err != nil {
		t.Fatalf("Sync() error = %v (the zero-option dial must use the JSON codec)", err)
	}
	if _, err := client.SyncAll(pushed); err != nil {
		t.Fatalf("SyncAll() error = %v", err)
	}
	list, err := client.GetAll([]int32{instance.InstanceStatusOnline}, instance.ProviderK8s)
	if err != nil {
		t.Fatalf("GetAll() error = %v", err)
	}
	if len(list.Instance) != 0 {
		t.Fatalf("GetAll() against an empty mock returned %d instances, want 0", len(list.Instance))
	}

	calls := server.Calls()
	if len(calls) != 3 {
		t.Fatalf("server observed %d calls, want 3 (SynInstance + SynAllInstance + GetAllInstance)", len(calls))
	}
	for _, call := range calls {
		if call.Method == "GetAllInstance" {
			if call.Provider != instance.ProviderK8s || call.Status != instance.InstanceStatusOnline {
				t.Fatalf("GetAllInstance call did not round-trip the request: provider=%q status=%d", call.Provider, call.Status)
			}
			continue
		}
		if len(call.Instances) != 1 || call.Instances[0].InstanceId != "i-b2-1" || call.Instances[0].Ip != "10.1.2.3" {
			t.Fatalf("call %s did not round-trip the pushed instance: %+v", call.Method, call.Instances)
		}
	}
	if calls[0].Method != "SynInstance" || calls[1].Method != "SynAllInstance" || calls[2].Method != "GetAllInstance" {
		t.Fatalf("unexpected call order: %s, %s, %s", calls[0].Method, calls[1].Method, calls[2].Method)
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	server, err := discoverymock.Start()
	if err != nil {
		t.Fatalf("discoverymock.Start() error = %v", err)
	}
	t.Cleanup(server.Close)
	conn, err := server.DialContext(context.Background())
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	client, err := NewClient(&grpcServiceClient{conn: conn}, nil, nil)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.conn = conn

	if err := client.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func newMockClient(t *testing.T) (*discoverymock.Server, *Client) {
	t.Helper()
	server, err := discoverymock.Start()
	if err != nil {
		t.Fatalf("discoverymock.Start() error = %v", err)
	}
	t.Cleanup(server.Close)
	conn, err := server.DialContext(context.Background())
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := NewClient(&grpcServiceClient{conn: conn}, &fakes.FakeLogger{}, fakes.NewFakeMetricsRecorder())
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return server, client
}

type deadlineService struct {
	deadline time.Time
	err      error
}

func (s *deadlineService) SynInstance(ctx context.Context, _ *instance.SynInstancesRequest, _ ...grpc.CallOption) (*instance.CommonResponse, error) {
	s.deadline, _ = ctx.Deadline()
	return nil, s.err
}

func (s *deadlineService) SynAllInstance(context.Context, *instance.SynAllInstancesRequest, ...grpc.CallOption) (*instance.CommonResponse, error) {
	return nil, errors.New("unexpected SynAllInstance call")
}

func (s *deadlineService) GetAllInstance(context.Context, *instance.GetAllInstancesRequest, ...grpc.CallOption) (*instance.InstanceList, error) {
	return nil, errors.New("unexpected GetAllInstance call")
}

//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"

	"spotter/config"
	"spotter/internal/domain/instance"
	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/discoverymock"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"spotter/pkg/providers/consul"
	"spotter/pkg/worker"
)

// serviceClient adapts a *grpc.ClientConn to the v2.InstanceServiceClient
// contract by delegating to conn.Invoke with the same method paths as the
// unexported grpcServiceClient in pkg/discoverycenter. The discoverymock
// JSON codec handles the wire format.
type serviceClient struct {
	conn *grpc.ClientConn
}

func (c *serviceClient) SynInstance(ctx context.Context, request *v2.SynInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *serviceClient) SynAllInstance(ctx context.Context, request *v2.SynAllInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *serviceClient) GetAllInstance(ctx context.Context, request *v2.GetAllInstancesRequest, opts ...grpc.CallOption) (*v2.InstanceList, error) {
	response := new(v2.InstanceList)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/GetAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

// testE2EConsulPipeline assembles the full pipeline from exported API only:
//
//	consulmock (loopback HTTP) -> NewConsulProvider (real monitor, real
//	conversion, real filters) -> DefaultWorker -> DiscoveryCenter ->
//	discoverymock (bufconn gRPC)
//
// The consul mock serves a catalog with one "microservice" service and one
// healthy endpoint; the monitor's watch loop observes the index change, the
// debounced instance handler drives syncInstance -> extractDiff ->
// worker.Handle -> DiscoveryCenter.Push, and the test asserts the SynInstance
// payload received by the discovery mock.
func testE2EConsulPipeline(t *testing.T) {
	t.Helper()

	// The consul provider (and the notice bridge it uses) log through the
	// legacy pkg/log global and notify through the pkg/notice global. Both
	// globals must be initialized before Run() is called, and the legacy
	// logger writes app.log into its configured directory, so point that
	// directory at a per-test temporary directory to keep the repository
	// clean (docs/testing.md section 7, "legacy globals").
	legacyDir := t.TempDir()
	config.LogFilePath = legacyDir + string(os.PathSeparator)
	config.LogToStd = false
	if err := log.LoggerInit(); err != nil {
		t.Fatalf("log.LoggerInit() error = %v", err)
	}
	notice.InitNoticeClient("test")

	// --- Consul side: catalog with one microservice and one healthy endpoint.
	consulServer := consulmock.Start()
	defer consulServer.Close()
	consulServer.SetServices(map[string][]string{
		"payments": {"microservice", "v2"},
	})
	consulServer.SetEntries("payments", []*api.ServiceEntry{newPaymentsEntry()})

	// --- Discovery side: in-memory gRPC server over bufconn.
	discovery, err := discoverymock.Start()
	if err != nil {
		t.Fatalf("discoverymock.Start() error = %v", err)
	}
	defer discovery.Close()

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := discovery.DialContext(dialCtx)
	if err != nil {
		t.Fatalf("discoverymock.DialContext() error = %v", err)
	}
	defer func() { _ = conn.Close() }()

	client, err := discoverycenter.NewClient(&serviceClient{conn: conn}, nil, nil)
	if err != nil {
		t.Fatalf("discoverycenter.NewClient() error = %v", err)
	}
	registry, err := discoverycenter.NewDiscoveryCenter(client, nil, nil, false)
	if err != nil {
		t.Fatalf("discoverycenter.NewDiscoveryCenter() error = %v", err)
	}

	// --- Worker: the real DefaultWorker over the registry.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w, err := worker.NewResourceWorker(ctx, registry, nil, nil)
	if err != nil {
		t.Fatalf("worker.NewResourceWorker() error = %v", err)
	}

	// --- Provider: the real consul provider, pointed at the consul mock.
	provider, err := consul.NewConsulProvider(ctx, w, 0, []string{consulServer.Address()})
	if err != nil {
		t.Fatalf("consul.NewConsulProvider() error = %v", err)
	}

	// Run the provider in the background; Run blocks on the monitor loop,
	// and the bounded context stops it when the test completes.
	providerDone := make(chan error, 1)
	go func() {
		providerDone <- provider.Run()
	}()

	// The watch loop polls /v1/health/state/any; the changed X-Consul-Index
	// triggers the 50ms-debounced instance handler, which syncs the changed
	// endpoints into the pipeline above.
	consulServer.AdvanceIndex()

	awaitSynInstance(t, discovery)

	// Keep the pipeline running briefly so the monitor's debounced change
	// detection (watch -> 50ms idle -> InstanceChanged -> syncInstance) also
	// completes within the test window, not only the full-push leg.
	time.Sleep(500 * time.Millisecond)

	// Cancel the bounded context so the provider monitor and the worker
	// retry loop stop; Run returns on the canceled errgroup context.
	cancel()
	select {
	case <-providerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("provider Run() did not return after context cancel")
	}
}

// newPaymentsEntry builds the healthy "microservice" endpoint served by the
// consul mock for the payments service. The meta fields are exactly the ones
// the conversion path in pkg/providers/consul/convertion.go consumes:
// ports, envType, envGroup, appCode, version and instanceId, plus a passing
// service check whose CheckID is "service:<appcode>" so convertSatus and
// convertState report online/running.
func newPaymentsEntry() *api.ServiceEntry {
	return &api.ServiceEntry{
		Node: &api.Node{
			Node:    "node-1",
			Address: "127.0.0.1",
		},
		Service: &api.AgentService{
			ID:      "payments-1",
			Service: "payments",
			Tags:    []string{"microservice"},
			Address: "127.0.0.1",
			Meta: map[string]string{
				"ports":      `[{"name":"http","protocol":"http","port":8080}]`,
				"envType":    "test",
				"envGroup":   "0",
				"appCode":    "payments",
				"version":    "v1.0.0",
				"instanceId": "payments-1",
			},
			ModifyIndex: 42,
		},
		Checks: api.HealthChecks{
			{
				CheckID: "service:payments",
				Status:  api.HealthPassing,
			},
		},
	}
}

// awaitSynInstance polls the discovery mock until a SynInstance call is
// observed, then asserts the converted instance fields.
func awaitSynInstance(t *testing.T, server *discoverymock.Server) {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)
	var calls []discoverymock.Call
	for time.Now().Before(deadline) {
		calls = server.Calls()
		for _, call := range calls {
			if call.Method != "SynInstance" {
				continue
			}
			assertPaymentsInstance(t, call)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for SynInstance on the discovery mock; calls = %#v", calls)
}

// assertPaymentsInstance verifies the instance payload received by the
// discovery mock against the fixture served by the consul mock.
func assertPaymentsInstance(t *testing.T, call discoverymock.Call) {
	t.Helper()

	if len(call.Instances) != 1 {
		t.Fatalf("SynInstance instances = %#v, want exactly one instance", call.Instances)
	}
	got := call.Instances[0]
	if got.InstanceId != "payments-1" {
		t.Errorf("InstanceId = %q, want %q", got.InstanceId, "payments-1")
	}
	if got.AppCode != "payments" {
		t.Errorf("AppCode = %q, want %q", got.AppCode, "payments")
	}
	if got.EnvType != "test" || got.EnvGroup != "0" {
		t.Errorf("EnvType/EnvGroup = %q/%q, want test/0", got.EnvType, got.EnvGroup)
	}
	if got.EnvCode != "test#0" {
		t.Errorf("EnvCode = %q, want %q", got.EnvCode, "test#0")
	}
	if got.Provider != "ecs" {
		t.Errorf("Provider = %q, want ecs", got.Provider)
	}
	if got.Status != instance.InstanceStatusOnline {
		t.Errorf("Status = %d, want %d", got.Status, instance.InstanceStatusOnline)
	}
	if got.State != "running" {
		t.Errorf("State = %q, want running", got.State)
	}
	if got.Ip != "127.0.0.1" {
		t.Errorf("Ip = %q, want 127.0.0.1", got.Ip)
	}
	if got.Reversion != 42 {
		t.Errorf("Reversion = %d, want 42", got.Reversion)
	}
	if len(got.Ports) != 1 || got.Ports[0] == nil {
		t.Fatalf("Ports = %#v, want one port entry", got.Ports)
	}
	if got.Ports[0].Name != "http" || got.Ports[0].Protocol != "http" || got.Ports[0].Port != 8080 {
		t.Errorf("Ports[0] = %#v, want name http, protocol http, port 8080", got.Ports[0])
	}
}

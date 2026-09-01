package discoverycenter

import (
	"context"
	"reflect"
	"testing"

	"spotter/internal/testkit/discoverymock"
	"spotter/internal/testkit/fakes"
	v2 "spotter/pkg/beehive/service/v2"
)

func TestNewDiscoveryCenterRejectsNilClient(t *testing.T) {
	registry, err := NewDiscoveryCenter(nil, nil, nil, false)
	if err == nil {
		t.Fatal("NewDiscoveryCenter() error = nil, want non-nil")
	}
	if registry != nil {
		t.Fatalf("NewDiscoveryCenter() registry = %#v, want nil", registry)
	}
}

func TestDiscoveryCenterDisablePushSkipsRPC(t *testing.T) {
	server, registry, _, _ := newRegistryFixture(t, true)

	if err := registry.Push(123, []*v2.Instance{{InstanceId: "instance-1"}}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if calls := server.Calls(); len(calls) != 0 {
		t.Fatalf("RPC calls = %#v, want none", calls)
	}
}

func TestDiscoveryCenterPushSuccess(t *testing.T) {
	server, registry, _, notifier := newRegistryFixture(t, false)
	instances := []*v2.Instance{{InstanceId: "instance-1", Reversion: 42}}

	if err := registry.Push(123, instances); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	calls := server.Calls()
	if len(calls) != 1 || calls[0].Method != "SynInstance" {
		t.Fatalf("RPC calls = %#v, want one SynInstance call", calls)
	}
	if len(calls[0].Instances) != 1 || calls[0].Instances[0].InstanceId != "instance-1" {
		t.Fatalf("RPC instances = %#v, want submitted instance", calls[0].Instances)
	}
	if got := notifier.Notifications(); len(got) != 0 {
		t.Fatalf("notifications = %#v, want none", got)
	}
}

func TestDiscoveryCenterPushErrorNotifiesExactContent(t *testing.T) {
	server, registry, _, notifier := newRegistryFixture(t, false)
	server.SetResponseCode(17, "rejected")

	err := registry.Push(123, []*v2.Instance{{InstanceId: "instance-1"}})
	if err == nil {
		t.Fatal("Push() error = nil, want non-nil")
	}
	want := []fakes.Notification{{
		Title:   "Failed to sync data incrementally",
		Content: "SynInstance failed with code: 17,error: rejected",
	}}
	if got := notifier.Notifications(); !reflect.DeepEqual(got, want) {
		t.Fatalf("notifications = %#v, want %#v", got, want)
	}
}

func TestDiscoveryCenterPushAllErrorNotifiesExactTitle(t *testing.T) {
	server, registry, _, notifier := newRegistryFixture(t, false)
	server.SetResponseCode(23, "full rejected")

	err := registry.PushAll(123, []*v2.Instance{{InstanceId: "instance-1"}})
	if err == nil {
		t.Fatal("PushAll() error = nil, want non-nil")
	}
	want := []fakes.Notification{{
		Title:   "Failed to sync all data",
		Content: "SynAllInstance failed with code: 23,error: full rejected",
	}}
	if got := notifier.Notifications(); !reflect.DeepEqual(got, want) {
		t.Fatalf("notifications = %#v, want %#v", got, want)
	}
}

func TestDiscoveryCenterCloseDelegatesToClient(t *testing.T) {
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
	registry, err := NewDiscoveryCenter(client, nil, nil, false)
	if err != nil {
		t.Fatalf("NewDiscoveryCenter() error = %v", err)
	}

	if err := registry.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func newRegistryFixture(t *testing.T, disablePush bool) (*discoverymock.Server, *DiscoveryCenter, *fakes.FakeLogger, *fakes.FakeNotifier) {
	t.Helper()
	server, client := newMockClient(t)
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}
	registry, err := NewDiscoveryCenter(client, logger, notifier, disablePush)
	if err != nil {
		t.Fatalf("NewDiscoveryCenter() error = %v", err)
	}
	return server, registry, logger, notifier
}

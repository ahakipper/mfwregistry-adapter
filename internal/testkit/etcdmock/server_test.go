package etcdmock

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
)

// newClient connects a real clientv3 client to the embedded server.
func newClient(t *testing.T, server *Server) *clientv3.Client {
	t.Helper()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: server.ClientEndpoints(),
	})
	if err != nil {
		t.Fatalf("clientv3.New() error = %v, want nil", err)
	}
	return client
}

// TestServerSupportsClientV3RoundTrip boots one embedded etcd and drives a
// real clientv3 Put/Get/Delete round trip through its advertised endpoint,
// which is the contract the e2e elector tests build on.
func TestServerSupportsClientV3RoundTrip(t *testing.T) {
	started := time.Now()
	server, err := Start()
	if err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	bootDuration := time.Since(started)
	defer server.Close()
	t.Logf("embedded etcd boot: %s", bootDuration)
	if bootDuration > readyTimeout {
		t.Fatalf("boot took %s, want within %s", bootDuration, readyTimeout)
	}

	endpoints := server.ClientEndpoints()
	if len(endpoints) != 1 {
		t.Fatalf("ClientEndpoints() = %#v, want exactly one endpoint", endpoints)
	}

	client := newClient(t, server)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Put(ctx, "etcdmock/key", "value-1"); err != nil {
		t.Fatalf("Put() error = %v, want nil", err)
	}
	response, err := client.Get(ctx, "etcdmock/key")
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if len(response.Kvs) != 1 || string(response.Kvs[0].Value) != "value-1" {
		t.Fatalf("Get() kvs = %#v, want one entry value-1", response.Kvs)
	}

	if _, err := client.Delete(ctx, "etcdmock/key"); err != nil {
		t.Fatalf("Delete() error = %v, want nil", err)
	}
	response, err = client.Get(ctx, "etcdmock/key")
	if err != nil {
		t.Fatalf("Get() after delete error = %v, want nil", err)
	}
	if response.Count != 0 {
		t.Fatalf("Get().Count after delete = %d, want 0", response.Count)
	}
}

// TestServerCloseIsIdempotentAndRemovesDataDir verifies Close stops the
// server (subsequent client requests fail), removes the data directory, and
// is safe to call repeatedly and concurrently.
func TestServerCloseIsIdempotentAndRemovesDataDir(t *testing.T) {
	server, err := Start()
	if err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}

	client := newClient(t, server)
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, "etcdmock/close", "value"); err != nil {
		t.Fatalf("Put() before close error = %v, want nil", err)
	}

	dir := server.Dir()
	if dir == "" {
		t.Fatal("Dir() = empty, want the embedded member's data directory")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("data dir %s missing before Close (err = %v)", dir, err)
	}

	server.Close()

	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data dir %s still present after Close (err = %v)", dir, err)
	}

	// The listener is gone, so new requests against the old endpoint fail.
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer requestCancel()
	if _, err := client.Get(requestCtx, "etcdmock/close"); err == nil {
		t.Fatal("Get() after server close error = nil, want connection error")
	}

	// Repeated and concurrent Close calls must be no-ops.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Close()
		}()
	}
	wg.Wait()
	server.Close()
}

// TestServerOptionsCustomizeMember verifies WithName and WithLogLevel are
// honored: the member serves requests under the custom name and still boots.
func TestServerOptionsCustomizeMember(t *testing.T) {
	server, err := Start(WithName("etcdmock-custom"), WithLogLevel("error"))
	if err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	defer server.Close()

	client := newClient(t, server)
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	members, err := client.Cluster.MemberList(ctx)
	if err != nil {
		t.Fatalf("MemberList() error = %v, want nil", err)
	}
	if len(members.Members) != 1 {
		t.Fatalf("MemberList() members = %d, want the single embedded member", len(members.Members))
	}
	if members.Members[0].Name != "etcdmock-custom" {
		t.Fatalf("member name = %q, want etcdmock-custom", members.Members[0].Name)
	}
}

// TestStartTwiceSequentially verifies a second embedded member can boot
// after the first one was closed, each on fresh ports with a fresh data
// dir (the e2e tier starts one member per test).
func TestStartTwiceSequentially(t *testing.T) {
	first, err := Start()
	if err != nil {
		t.Fatalf("first Start() error = %v, want nil", err)
	}
	firstEndpoints := first.ClientEndpoints()
	first.Close()

	second, err := Start()
	if err != nil {
		t.Fatalf("second Start() error = %v, want nil", err)
	}
	defer second.Close()

	if second.ClientEndpoints()[0] == firstEndpoints[0] {
		// Not a failure (ports may be reused once freed), but noteworthy.
		t.Logf("second member reused the first member's port %s", firstEndpoints[0])
	}

	client := newClient(t, second)
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Put(ctx, "etcdmock/second", "value"); err != nil {
		t.Fatalf("Put() against second member error = %v, want nil", err)
	}
}

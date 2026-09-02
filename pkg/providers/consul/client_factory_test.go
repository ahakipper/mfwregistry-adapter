package consul

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"spotter/internal/ports"
	"spotter/internal/testkit/consulmock"
)

func TestNewClientFactoryRejectsNoUsableAddresses(t *testing.T) {
	tests := []struct {
		name  string
		addrs []string
	}{
		{name: "nil", addrs: nil},
		{name: "empty", addrs: []string{}},
		{name: "blank only", addrs: []string{"", " \t", "\n"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			factory, err := NewClientFactory(test.addrs, nil)
			if err == nil {
				t.Fatal("NewClientFactory() error = nil, want a no usable addresses error")
			}
			if factory != nil {
				t.Fatalf("NewClientFactory() factory = %#v, want nil", factory)
			}
			if !strings.Contains(err.Error(), "no usable") {
				t.Fatalf("NewClientFactory() error = %q, want a clear no usable addresses error", err)
			}
		})
	}
}

func TestNewClientFactorySkipsBlankAddressesAndTrimsValidAddress(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{"", " \t", "  " + server.Address() + "\n"}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}
	if len(factory.addrs) != 1 || factory.addrs[0] != server.Address() {
		t.Fatalf("factory addresses = %#v, want only trimmed address %q", factory.addrs, server.Address())
	}

	client, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("ConsulClientFactory() error = %v", err)
	}
	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatalf("selected client leader: %v", err)
	}
	if leader == "" {
		t.Fatal("selected client leader is empty")
	}
	assertLeaderRequests(t, server, 2)
}

func TestNewClientFactoryDeduplicatesNormalizedAddressesInOrder(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()

	second := consulmock.Start()
	defer second.Close()

	factory, err := NewClientFactory([]string{
		"  " + first.Address(),
		second.Address(),
		first.Address() + "\n",
		"\t" + second.Address() + " ",
	}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	want := []string{first.Address(), second.Address()}
	if len(factory.addrs) != len(want) {
		t.Fatalf("factory addresses = %#v, want %#v", factory.addrs, want)
	}
	for i := range want {
		if factory.addrs[i] != want[i] {
			t.Fatalf("factory address %d = %q, want %q", i, factory.addrs[i], want[i])
		}
	}
	assertLeaderRequests(t, first, 0)
	assertLeaderRequests(t, second, 0)
}

func TestClientFactorySelectsFirstValidAddress(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("127.0.0.1:8301")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("127.0.0.1:8302")

	factory, err := NewClientFactory([]string{first.Address(), second.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("ConsulClientFactory() error = %v", err)
	}
	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatalf("selected client leader: %v", err)
	}
	if leader != "127.0.0.1:8301" {
		t.Fatalf("selected client leader = %q, want first address leader", leader)
	}
	assertLeaderRequests(t, first, 2)
	assertLeaderRequests(t, second, 0)
}

func TestClientFactoryFailsOverFromDeadAddress(t *testing.T) {
	dead := consulmock.Start()
	deadAddress := dead.Address()
	dead.Close()

	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{deadAddress, server.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("ConsulClientFactory() error = %v", err)
	}
	if client == nil {
		t.Fatal("ConsulClientFactory() client = nil, want mock server client")
	}
	assertLeaderRequests(t, server, 1)
}

func TestClientFactoryReturnsErrorWhenAllAddressesInvalid(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("")

	factory, err := NewClientFactory([]string{first.Address(), second.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err == nil {
		t.Fatal("ConsulClientFactory() error = nil, want no valid client error")
	}
	if client != nil {
		t.Fatalf("ConsulClientFactory() client = %#v, want nil", client)
	}
	if !strings.Contains(err.Error(), "no valid Consul client") {
		t.Fatalf("ConsulClientFactory() error = %q, want a clear all-invalid error", err)
	}
	for _, want := range []string{first.Address(), second.Address(), "empty leader"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ConsulClientFactory() error = %q, want it to contain %q", err, want)
		}
	}
	assertLeaderRequests(t, first, 1)
	assertLeaderRequests(t, second, 1)
}

func TestClientFactoryAggregateErrorRetainsFirstProbeCause(t *testing.T) {
	first := consulmock.Start()
	firstAddress := first.Address()
	first.Close()

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("")

	factory, err := NewClientFactory([]string{firstAddress, second.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	client, err := factory.ConsulClientFactory()
	if err == nil {
		t.Fatal("ConsulClientFactory() error = nil, want aggregate probe error")
	}
	if client != nil {
		t.Fatalf("ConsulClientFactory() client = %#v, want nil", client)
	}
	for _, want := range []string{firstAddress, second.Address(), "check Consul leader", "empty leader"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("ConsulClientFactory() error = %q, want it to contain %q", err, want)
		}
	}

	var urlError *url.Error
	if !errors.As(err, &urlError) {
		t.Fatalf("errors.As(%T, *url.Error) = false, want first probe cause", err)
	}
	if !errors.Is(err, urlError) {
		t.Fatalf("errors.Is(error, first probe cause) = false, want true")
	}
	assertLeaderRequests(t, second, 1)
}

func TestClientFactoryReusesHealthyCachedClient(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{server.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	first, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("first ConsulClientFactory() error = %v", err)
	}
	server.SetLeader("127.0.0.2:8300")
	second, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("second ConsulClientFactory() error = %v", err)
	}

	if second != first {
		t.Fatal("second ConsulClientFactory() returned a new client, want cached client")
	}
	assertLeaderRequests(t, server, 2)
}

func TestClientFactoryWarmCacheFailsOverWithoutRetryingFailedAddress(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("127.0.0.1:8301")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("127.0.0.1:8302")

	factory, err := NewClientFactory([]string{first.Address(), second.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	cached, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("first ConsulClientFactory() error = %v", err)
	}
	assertLeaderRequests(t, first, 1)
	assertLeaderRequests(t, second, 0)

	first.SetLeader("")
	fallback, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("second ConsulClientFactory() error = %v", err)
	}
	if fallback == cached {
		t.Fatal("second ConsulClientFactory() returned failed cached client")
	}
	assertLeaderRequests(t, first, 2)
	assertLeaderRequests(t, second, 1)

	leader, err := fallback.Status().Leader()
	if err != nil {
		t.Fatalf("fallback client leader: %v", err)
	}
	if leader != "127.0.0.1:8302" {
		t.Fatalf("fallback client leader = %q, want second address leader", leader)
	}
	assertLeaderRequests(t, second, 2)
}

func TestClientFactoryWarmCacheDeduplicatesFailedAddressBeforeFailover(t *testing.T) {
	first := consulmock.Start()
	defer first.Close()
	first.SetLeader("127.0.0.1:8301")

	second := consulmock.Start()
	defer second.Close()
	second.SetLeader("127.0.0.1:8302")

	factory, err := NewClientFactory([]string{
		first.Address(),
		"  " + first.Address() + "\n",
		second.Address(),
	}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	cached, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("first ConsulClientFactory() error = %v", err)
	}
	assertLeaderRequests(t, first, 1)
	assertLeaderRequests(t, second, 0)

	first.SetLeader("")
	fallback, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("second ConsulClientFactory() error = %v", err)
	}
	if fallback == cached {
		t.Fatal("second ConsulClientFactory() returned failed cached client")
	}
	assertLeaderRequests(t, first, 2)
	assertLeaderRequests(t, second, 1)

	leader, err := fallback.Status().Leader()
	if err != nil {
		t.Fatalf("fallback client leader: %v", err)
	}
	if leader != "127.0.0.1:8302" {
		t.Fatalf("fallback client leader = %q, want second address leader", leader)
	}
	assertLeaderRequests(t, first, 2)
	assertLeaderRequests(t, second, 2)
}

func TestClientFactoryConcurrentCallsAreSafe(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{server.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}

	const workers = 32
	start := make(chan struct{})
	errors := make(chan error, workers)
	clients := make(chan interface{}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			client, callErr := factory.ConsulClientFactory()
			if callErr != nil {
				errors <- callErr
				return
			}
			clients <- client
		}()
	}
	close(start)
	wg.Wait()
	close(errors)
	close(clients)

	for callErr := range errors {
		t.Fatalf("concurrent ConsulClientFactory() error = %v", callErr)
	}
	count := 0
	for client := range clients {
		if client == nil {
			t.Fatal("concurrent ConsulClientFactory() client = nil")
		}
		count++
	}
	if count != workers {
		t.Fatalf("successful concurrent calls = %d, want %d", count, workers)
	}

	factory.mu.RLock()
	cached := len(factory.clients)
	factory.mu.RUnlock()
	if cached != 1 {
		t.Fatalf("cached clients = %d, want 1", cached)
	}
	requests := server.Requests()
	if len(requests) == 0 {
		t.Fatal("server requests = 0, want at least one leader probe")
	}
	assertLeaderRequestShape(t, requests)
}

func TestClientFactoryNilLoggerUsesNopLogger(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NewClientFactory([]string{server.Address()}, nil)
	if err != nil {
		t.Fatalf("NewClientFactory() error = %v", err)
	}
	if _, ok := factory.logger.(ports.NopLogger); !ok {
		t.Fatalf("factory logger = %T, want ports.NopLogger", factory.logger)
	}
	if _, err := factory.ConsulClientFactory(); err != nil {
		t.Fatalf("ConsulClientFactory() with nil logger error = %v", err)
	}
	assertLeaderRequests(t, server, 1)
}

func TestClientFactoryDeprecatedConstructorDelegates(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()

	factory, err := NeweClientFacotorySimple([]string{server.Address()})
	if err != nil {
		t.Fatalf("NeweClientFacotorySimple() error = %v", err)
	}
	if _, ok := factory.logger.(ports.NopLogger); !ok {
		t.Fatalf("factory logger = %T, want ports.NopLogger", factory.logger)
	}
	client, err := factory.ConsulClientFactory()
	if err != nil {
		t.Fatalf("ConsulClientFactory() error = %v", err)
	}
	leader, err := client.Status().Leader()
	if err != nil {
		t.Fatalf("deprecated factory client leader: %v", err)
	}
	if leader == "" {
		t.Fatal("deprecated factory client leader is empty")
	}
	assertLeaderRequests(t, server, 2)
}

func assertLeaderRequests(t *testing.T, server *consulmock.Server, want int) {
	t.Helper()

	requests := server.Requests()
	if len(requests) != want {
		t.Fatalf("server requests = %d, want %d leader probes", len(requests), want)
	}
	assertLeaderRequestShape(t, requests)
}

func assertLeaderRequestShape(t *testing.T, requests []consulmock.Request) {
	t.Helper()

	for i, request := range requests {
		if request.Method != http.MethodGet {
			t.Fatalf("request %d method = %q, want GET", i, request.Method)
		}
		if request.Path != "/v1/status/leader" {
			t.Fatalf("request %d path = %q, want /v1/status/leader", i, request.Path)
		}
		if len(request.Query) != 0 {
			t.Fatalf("request %d query = %v, want empty query", i, request.Query)
		}
	}
}

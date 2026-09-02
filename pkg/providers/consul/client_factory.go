package consul

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/hashicorp/consul/api"

	"spotter/internal/ports"
)

type ConsulClientFactory interface {
	ConsulClientFactory() (*api.Client, error)
}

type ClientFactorySimple struct {
	addrs   []string
	clients map[string]*api.Client
	logger  ports.Logger
	mu      sync.RWMutex
}

func NewClientFactory(addrs []string, logger ports.Logger) (*ClientFactorySimple, error) {
	usableAddrs := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		usableAddrs = append(usableAddrs, addr)
	}
	if len(usableAddrs) == 0 {
		return nil, errors.New("Consul client factory has no usable addresses")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}

	return &ClientFactorySimple{
		addrs:   usableAddrs,
		clients: make(map[string]*api.Client),
		logger:  logger,
	}, nil
}

// NeweClientFacotorySimple is deprecated. Use NewClientFactory instead.
func NeweClientFacotorySimple(addrs []string) (*ClientFactorySimple, error) {
	return NewClientFactory(addrs, nil)
}

// ConsulClientFactory returns the first healthy cached or configured Consul client.
func (cfs *ClientFactorySimple) ConsulClientFactory() (*api.Client, error) {
	attempted := make(map[string]struct{}, len(cfs.addrs))
	failures := make([]error, 0, len(cfs.addrs))

	for _, cached := range cfs.cachedClients() {
		attempted[cached.addr] = struct{}{}
		leader, err := cached.client.Status().Leader()
		if err == nil && leader != "" {
			return cached.client, nil
		}
		cfs.removeCachedClient(cached.addr, cached.client)
		if err != nil {
			err = fmt.Errorf("check cached Consul leader at %q: %w", cached.addr, err)
		} else {
			err = fmt.Errorf("cached Consul address %q returned an empty leader", cached.addr)
		}
		failures = append(failures, err)
		cfs.warnf("%v", err)
	}

	for _, addr := range cfs.addrs {
		if _, ok := attempted[addr]; ok {
			continue
		}
		attempted[addr] = struct{}{}

		config := api.DefaultConfig()
		config.Address = addr

		client, err := api.NewClient(config)
		if err != nil {
			err = fmt.Errorf("create Consul client for %q: %w", addr, err)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}

		leader, err := client.Status().Leader()
		if err != nil {
			err = fmt.Errorf("check Consul leader at %q: %w", addr, err)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}
		if leader == "" {
			err = fmt.Errorf("Consul address %q returned an empty leader", addr)
			failures = append(failures, err)
			cfs.warnf("%v", err)
			continue
		}

		cfs.cacheClient(addr, client)
		return client, nil
	}

	if len(failures) == 0 {
		return nil, errors.New("no valid Consul client found")
	}
	return nil, &clientProbeAggregateError{failures: failures}
}

type clientProbeAggregateError struct {
	failures []error
}

func (err *clientProbeAggregateError) Error() string {
	messages := make([]string, len(err.failures))
	for i, failure := range err.failures {
		messages[i] = failure.Error()
	}
	return fmt.Sprintf("no valid Consul client found: %s", strings.Join(messages, "; "))
}

func (err *clientProbeAggregateError) Unwrap() error {
	if len(err.failures) == 0 {
		return nil
	}
	return err.failures[0]
}

type cachedClient struct {
	addr   string
	client *api.Client
}

func (cfs *ClientFactorySimple) cachedClients() []cachedClient {
	cfs.mu.RLock()
	defer cfs.mu.RUnlock()

	clients := make([]cachedClient, 0, len(cfs.clients))
	for _, addr := range cfs.addrs {
		if client := cfs.clients[addr]; client != nil {
			clients = append(clients, cachedClient{addr: addr, client: client})
		}
	}
	return clients
}

func (cfs *ClientFactorySimple) removeCachedClient(addr string, client *api.Client) {
	cfs.mu.Lock()
	if cfs.clients[addr] == client {
		delete(cfs.clients, addr)
	}
	cfs.mu.Unlock()
}

func (cfs *ClientFactorySimple) cacheClient(addr string, client *api.Client) {
	cfs.mu.Lock()
	cfs.clients[addr] = client
	cfs.mu.Unlock()
}

func (cfs *ClientFactorySimple) warnf(format string, args ...interface{}) {
	if cfs.logger != nil {
		cfs.logger.Warnf(format, args...)
	}
}

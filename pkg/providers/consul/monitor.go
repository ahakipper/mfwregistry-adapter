package consul

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/errgroup"

	"spotter/internal/ports"
	"spotter/pkg/notice"
)

// Monitor handles service and instance changes.
type Monitor interface {
	Start(ctx context.Context) error
	GetServices() (services map[string][]string, err error)
	GetServiceEntries(name string, q *api.QueryOptions) (endpoints []*api.ServiceEntry, err error)
	AppendServiceHandler(ServiceHandler)
	AppendInstanceHandler(InstanceHandler)
}

// InstanceHandler processes service instance change events.
type InstanceHandler func(instance *api.CatalogService) error

// ServiceHandler processes service change events.
type ServiceHandler func(instances []*api.CatalogService) error

type consulMonitor struct {
	clientFactory ConsulClientFactory
	logger        ports.Logger
	notifier      ports.Notifier
	clock         ports.Clock

	handlersMu       sync.RWMutex
	instanceHandlers []InstanceHandler
	serviceHandlers  []ServiceHandler
}

const (
	refreshIdleTime    time.Duration = 50 * time.Millisecond
	periodicCheckTime  time.Duration = 50 * time.Millisecond
	blockQueryWaitTime time.Duration = 5 * time.Second

	tagMicroservice string = "microservice"
)

type nopNotifier struct{}

func (nopNotifier) Notify(string, string) {}

// legacyNotifier forwards monitor notifications to the legacy global notifier.
// It is a temporary compatibility bridge until the provider composition phase
// wires a ports.Notifier implementation through construction.
type legacyNotifier struct{}

func (legacyNotifier) Notify(title, content string) {
	notice.Notice(title, content)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// NewConsulMonitor watches for changes in Consul services and catalog services.
func NewConsulMonitor(clientF ConsulClientFactory, logger ports.Logger, notifier ports.Notifier, clock ports.Clock) (Monitor, error) {
	if isNilInterface(clientF) {
		return nil, errors.New("new consul monitor with nil consul client")
	}
	if isNilInterface(logger) {
		logger = ports.NopLogger{}
	}
	if isNilInterface(notifier) {
		notifier = nopNotifier{}
	}
	if isNilInterface(clock) {
		clock = realClock{}
	}

	return &consulMonitor{
		clientFactory:    clientF,
		logger:           logger,
		notifier:         notifier,
		clock:            clock,
		instanceHandlers: make([]InstanceHandler, 0),
		serviceHandlers:  make([]ServiceHandler, 0),
	}, nil
}

func isNilInterface(value interface{}) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (m *consulMonitor) Start(ctx context.Context) error {
	change := make(chan struct{}, 64)

	eg, groupCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer m.logger.Info("consul monitor watching action stopped")
		return m.watchConsul(groupCtx, change)
	})
	eg.Go(func() error {
		defer m.logger.Info("consul monitor update record action stopped")
		return m.updateRecord(groupCtx, change)
	})
	return eg.Wait()
}

// watchConsul watches Consul service, node, and health changes.
func (m *consulMonitor) watchConsul(ctx context.Context, change chan<- struct{}) error {
	var consulWaitIndex uint64

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		client, err := m.clientFactory.ConsulClientFactory()
		if err != nil {
			m.logger.Errorf("get consul client: %v", err)
			m.notifier.Notify("Failed to initialize the consul client while watching for consul data changes", err.Error())
			if !m.wait(ctx, blockQueryWaitTime) {
				return nil
			}
			continue
		}

		queryOptions := (&api.QueryOptions{
			WaitIndex: consulWaitIndex,
			WaitTime:  blockQueryWaitTime,
		}).WithContext(ctx)
		_, queryMeta, err := client.Health().State(api.HealthAny, queryOptions)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			m.logger.Warnf("could not fetch services: %s", err.Error())
			m.notifier.Notify("Failed to fetch data from consul while watching for consul data changes", err.Error())
		} else if queryMeta != nil && consulWaitIndex != queryMeta.LastIndex {
			consulWaitIndex = queryMeta.LastIndex
			select {
			case change <- struct{}{}:
			case <-ctx.Done():
				return nil
			}
		}

		if !m.wait(ctx, periodicCheckTime) {
			return nil
		}
	}
}

func (m *consulMonitor) wait(ctx context.Context, duration time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-m.clock.After(duration):
		return true
	}
}

func (m *consulMonitor) updateRecord(ctx context.Context, change <-chan struct{}) error {
	var lastChange time.Time
	periodicCheck := m.clock.After(periodicCheckTime)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-periodicCheck:
			if !lastChange.IsZero() && m.clock.Now().Sub(lastChange) >= refreshIdleTime {
				m.logger.Infof("consul service changed")
				m.updateInstanceRecord()
				lastChange = time.Time{}
			}
			periodicCheck = m.clock.After(periodicCheckTime)
		case <-change:
			lastChange = m.clock.Now()
			periodicCheck = m.clock.After(periodicCheckTime)
		}
	}
}

func (m *consulMonitor) updateServiceRecord() {
	var obj []*api.CatalogService
	for _, handler := range m.serviceHandlerSnapshot() {
		go func(handler ServiceHandler) {
			if err := handler(obj); err != nil {
				m.logger.Warnf("Error executing service handler function: %v", err)
			}
		}(handler)
	}
}

func (m *consulMonitor) updateInstanceRecord() {
	obj := &api.CatalogService{}
	for _, handler := range m.instanceHandlerSnapshot() {
		go func(handler InstanceHandler) {
			if err := handler(obj); err != nil {
				m.notifier.Notify("Failed to handle the consul instance change", err.Error())
				m.logger.Warnf("Error executing instance handler function: %v", err)
			}
		}(handler)
	}
}

func (m *consulMonitor) AppendServiceHandler(handler ServiceHandler) {
	m.handlersMu.Lock()
	m.serviceHandlers = append(m.serviceHandlers, handler)
	m.handlersMu.Unlock()
}

func (m *consulMonitor) AppendInstanceHandler(handler InstanceHandler) {
	m.handlersMu.Lock()
	m.instanceHandlers = append(m.instanceHandlers, handler)
	m.handlersMu.Unlock()
}

func (m *consulMonitor) serviceHandlerSnapshot() []ServiceHandler {
	m.handlersMu.RLock()
	handlers := append([]ServiceHandler(nil), m.serviceHandlers...)
	m.handlersMu.RUnlock()
	return handlers
}

func (m *consulMonitor) instanceHandlerSnapshot() []InstanceHandler {
	m.handlersMu.RLock()
	handlers := append([]InstanceHandler(nil), m.instanceHandlers...)
	m.handlersMu.RUnlock()
	return handlers
}

func (m *consulMonitor) GetServices() (map[string][]string, error) {
	client, err := m.clientFactory.ConsulClientFactory()
	if err != nil {
		m.logger.Errorf("get consul client: %v", err)
		return nil, err
	}
	services, _, err := client.Catalog().Services(nil)
	if err != nil {
		m.logger.Warnf("Could not retrieve services from consul: %v", err)
		return nil, err
	}
	return services, nil
}

func (m *consulMonitor) GetServiceEntries(name string, q *api.QueryOptions) ([]*api.ServiceEntry, error) {
	client, err := m.clientFactory.ConsulClientFactory()
	if err != nil {
		m.logger.Errorf("get consul client: %v", err)
		return nil, err
	}
	endpoints, _, err := client.Health().Service(name, tagMicroservice, true, q)
	if err != nil {
		m.logger.Warnf("Could not retrieve service catalog from consul: %v", err)
		return nil, err
	}
	return endpoints, nil
}

var _ ports.Clock = realClock{}

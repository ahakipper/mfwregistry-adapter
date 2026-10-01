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
)

// Monitor handles service and instance changes.
type Monitor interface {
	Start(ctx context.Context) error
	GetServices() (services map[string][]string, err error)
	GetServiceEntries(name string, q *api.QueryOptions) (endpoints []*api.ServiceEntry, err error)
	AppendServiceHandler(ServiceHandler)
	AppendInstanceHandler(InstanceHandler)
}

// InstanceChangeHandler receives a change notification without fabricated
// CatalogService payload. It is additive for compatibility with InstanceHandler.
type InstanceChangeHandler func() error

// TimestampedInstanceChangeHandler receives the real time at which the
// blocking Consul watch returned a changed response. The timestamp is kept
// separate from InstanceChangeHandler so legacy fixtures and callers retain
// their payload-free API.
type TimestampedInstanceChangeHandler func(watchAt time.Time) error

// InstanceHandler processes service instance change events.
type InstanceHandler func(instance *api.CatalogService) error

// ServiceHandler processes service change events.
type ServiceHandler func(instances []*api.CatalogService) error

type consulMonitor struct {
	clientFactory ConsulClientFactory
	logger        ports.Logger
	notifier      ports.Notifier
	clock         ports.Clock

	handlersMu          sync.RWMutex
	instanceHandlers    []InstanceHandler
	changeHandlers      []InstanceChangeHandler
	timedChangeHandlers []TimestampedInstanceChangeHandler
	serviceHandlers     []ServiceHandler
	handlersWG          sync.WaitGroup
	watchTimestamps     chan time.Time
}

// consulPassingOnly keeps the source contract explicit: desired catalog reads
// include only Consul entries whose checks are passing. Unhealthy instances
// therefore disappear from desired state and are cleaned up through deletion;
// repeated healthy-empty snapshots remain protected by CompareAndFlush's
// confirmation gate.
const consulPassingOnly = true

const (
	refreshIdleTime    time.Duration = 50 * time.Millisecond
	periodicCheckTime  time.Duration = 50 * time.Millisecond
	blockQueryWaitTime time.Duration = 5 * time.Second
	watchChangeBurst                 = 2
	watchChangeRefill                = 15 * time.Second

	tagMicroservice string = "microservice"
)

type nopNotifier struct{}

func (nopNotifier) Notify(string, string) {}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// consulWatchIndex tracks the index returned by the health-state query and
// the index sent on the next request. Consul can return an empty/zero index
// or an index older than the one previously observed (for example after a
// Raft snapshot restore). Invalid indexes must never become change tokens.
type consulWatchIndex struct {
	waitIndex uint64
	lastIndex uint64
}

// observe records one query result. A rollback is a real change notification,
// but resets the next query to index zero so Consul can establish a fresh
// blocking baseline. The fresh response is compared with the rolled-back
// index, so it cannot produce a duplicate notification.
func (i *consulWatchIndex) observe(queryMeta *api.QueryMeta) (changed, retryImmediately bool) {
	if queryMeta == nil || queryMeta.LastIndex == 0 {
		// Consul requires a positive index after the initial request. Retain an
		// explicit floor after every invalid response to prevent an immediate
		// zero-index retry loop.
		i.waitIndex = 1
		return false, false
	}

	lastIndex := queryMeta.LastIndex
	if i.lastIndex != 0 && lastIndex < i.lastIndex {
		i.lastIndex = lastIndex
		i.waitIndex = 0
		return true, true
	}

	changed = i.lastIndex != lastIndex
	i.lastIndex = lastIndex
	i.waitIndex = lastIndex
	return changed, false
}

// consulChangeLimiter is a small token bucket for the blocking-query watch.
// Two changes may be delivered back-to-back; once the burst is exhausted, a
// token is replenished every watchChangeRefill. This keeps normal sparse
// changes immediate while bounding load during an index-change storm.
type consulChangeLimiter struct {
	burst      int
	refill     time.Duration
	tokens     int
	lastRefill time.Time
}

func newConsulChangeLimiter(burst int, refill time.Duration) *consulChangeLimiter {
	return &consulChangeLimiter{
		burst:  burst,
		refill: refill,
		tokens: burst,
	}
}

// waitForChange waits until a token is available. It returns false only when
// the monitor context is cancelled while rate-limited.
func (l *consulChangeLimiter) waitForChange(ctx context.Context, clock ports.Clock) bool {
	if l == nil || l.burst <= 0 || l.refill <= 0 {
		return true
	}

	now := clock.Now()
	if l.tokens > 0 {
		l.tokens--
		if l.tokens == 0 {
			l.lastRefill = now
		}
		return true
	}

	if l.lastRefill.IsZero() {
		l.lastRefill = now
	}
	if elapsed := now.Sub(l.lastRefill); elapsed >= l.refill {
		refilled := int(elapsed / l.refill)
		if refilled > l.burst {
			refilled = l.burst
		}
		l.tokens = refilled
		l.lastRefill = l.lastRefill.Add(time.Duration(refilled) * l.refill)
		if l.tokens > 0 {
			l.tokens--
			if l.tokens == 0 {
				l.lastRefill = now
			}
			return true
		}
	}

	// Once the burst is exhausted, wait for one complete refill interval
	// from this attempt. Keeping the interval discrete prevents a fractional
	// token from shortening the promised rapid-change delay.
	waitFor := l.refill
	select {
	case <-ctx.Done():
		return false
	case <-clock.After(waitFor):
		// The newly replenished token is consumed by this change. Re-anchor
		// the refill clock at the wake-up time so the next change cannot use
		// the same token a second time.
		l.tokens = 0
		l.lastRefill = clock.Now()
		return true
	}
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
	m.handlersMu.Lock()
	m.watchTimestamps = make(chan time.Time, cap(change))
	m.handlersMu.Unlock()

	eg, groupCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer m.logger.Info("consul monitor watching action stopped")
		return m.watchConsul(groupCtx, change)
	})
	eg.Go(func() error {
		defer m.logger.Info("consul monitor update record action stopped")
		return m.updateRecord(groupCtx, change)
	})
	err := eg.Wait()
	// update*Record dispatches handlers asynchronously. Wait before returning
	// so the provider can safely release its worker pool on shutdown without a
	// late callback racing a final Submit.
	m.handlersWG.Wait()
	return err
}

// watchConsul watches Consul service, node, and health changes.
func (m *consulMonitor) watchConsul(ctx context.Context, change chan<- struct{}) error {
	var consulIndex consulWatchIndex
	changeLimiter := newConsulChangeLimiter(watchChangeBurst, watchChangeRefill)

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
			WaitIndex: consulIndex.waitIndex,
			WaitTime:  blockQueryWaitTime,
		}).WithContext(ctx)
		_, queryMeta, err := client.Health().State(api.HealthAny, queryOptions)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			m.logger.Warnf("could not fetch services: %s", err.Error())
			m.notifier.Notify("Failed to fetch data from consul while watching for consul data changes", err.Error())
		} else {
			changed, retryImmediately := consulIndex.observe(queryMeta)
			if changed {
				watchAt := m.clock.Now()
				// A rollback must re-establish the blocking baseline without
				// delay; only ordinary rapid changes consume rate-limit tokens.
				if !retryImmediately && !changeLimiter.waitForChange(ctx, m.clock) {
					return nil
				}
				timestamps := m.watchTimestampChannel()
				if timestamps != nil {
					select {
					case timestamps <- watchAt:
					case <-ctx.Done():
						return nil
					}
				}
				select {
				case change <- struct{}{}:
				case <-ctx.Done():
					return nil
				}
			}
			if retryImmediately {
				continue
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
	var firstWatchAt time.Time
	periodicCheck := m.clock.After(periodicCheckTime)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-periodicCheck:
			if !lastChange.IsZero() && m.clock.Now().Sub(lastChange) >= refreshIdleTime {
				m.logger.Infof("consul service changed")
				m.updateInstanceRecordAt(firstWatchAt)
				lastChange = time.Time{}
				firstWatchAt = time.Time{}
			}
			periodicCheck = m.clock.After(periodicCheckTime)
		case <-change:
			if watchAt := m.takeWatchTimestamp(); !watchAt.IsZero() && (firstWatchAt.IsZero() || watchAt.Before(firstWatchAt)) {
				firstWatchAt = watchAt
			}
			lastChange = m.clock.Now()
			periodicCheck = m.clock.After(periodicCheckTime)
		}
	}
}

func (m *consulMonitor) updateServiceRecord() {
	var obj []*api.CatalogService
	handlers := m.serviceHandlerSnapshot()
	m.handlersWG.Add(len(handlers))
	for _, handler := range handlers {
		go func(handler ServiceHandler) {
			defer m.handlersWG.Done()
			if err := handler(obj); err != nil {
				m.logger.Warnf("Error executing service handler function: %v", err)
			}
		}(handler)
	}
}

func (m *consulMonitor) updateInstanceRecord() {
	m.updateInstanceRecordAt(time.Time{})
}

func (m *consulMonitor) updateInstanceRecordAt(watchAt time.Time) {
	changeHandlers := m.changeHandlerSnapshot()
	if len(changeHandlers) > 0 {
		m.handlersWG.Add(len(changeHandlers))
		for _, handler := range changeHandlers {
			go func(h InstanceChangeHandler) {
				defer m.handlersWG.Done()
				if err := h(); err != nil {
					m.notifier.Notify("Failed to handle the consul instance change", err.Error())
					m.logger.Warnf("Error executing instance change handler: %v", err)
				}
			}(handler)
		}
	}
	timedHandlers := m.timedChangeHandlerSnapshot()
	if !watchAt.IsZero() && len(timedHandlers) > 0 {
		m.handlersWG.Add(len(timedHandlers))
		for _, handler := range timedHandlers {
			go func(h TimestampedInstanceChangeHandler) {
				defer m.handlersWG.Done()
				if err := h(watchAt); err != nil {
					m.notifier.Notify("Failed to handle the consul instance change", err.Error())
					m.logger.Warnf("Error executing timestamped instance handler: %v", err)
				}
			}(handler)
		}
	}
	handlers := m.instanceHandlerSnapshot()
	obj := &api.CatalogService{}
	m.handlersWG.Add(len(handlers))
	for _, handler := range handlers {
		go func(handler InstanceHandler) {
			defer m.handlersWG.Done()
			if err := handler(obj); err != nil {
				m.notifier.Notify("Failed to handle the consul instance change", err.Error())
				m.logger.Warnf("Error executing instance handler function: %v", err)
			}
		}(handler)
	}
}

func (m *consulMonitor) AppendInstanceChangeHandler(handler InstanceChangeHandler) {
	m.handlersMu.Lock()
	m.changeHandlers = append(m.changeHandlers, handler)
	m.handlersMu.Unlock()
}

func (m *consulMonitor) AppendTimestampedInstanceChangeHandler(handler TimestampedInstanceChangeHandler) {
	m.handlersMu.Lock()
	m.timedChangeHandlers = append(m.timedChangeHandlers, handler)
	m.handlersMu.Unlock()
}

func (m *consulMonitor) timedChangeHandlerSnapshot() []TimestampedInstanceChangeHandler {
	m.handlersMu.RLock()
	defer m.handlersMu.RUnlock()
	return append([]TimestampedInstanceChangeHandler(nil), m.timedChangeHandlers...)
}

func (m *consulMonitor) watchTimestampChannel() chan time.Time {
	m.handlersMu.RLock()
	defer m.handlersMu.RUnlock()
	return m.watchTimestamps
}

func (m *consulMonitor) takeWatchTimestamp() time.Time {
	timestamps := m.watchTimestampChannel()
	if timestamps == nil {
		return time.Time{}
	}
	select {
	case watchAt := <-timestamps:
		return watchAt
	default:
		return time.Time{}
	}
}
func (m *consulMonitor) changeHandlerSnapshot() []InstanceChangeHandler {
	m.handlersMu.RLock()
	defer m.handlersMu.RUnlock()
	return append([]InstanceChangeHandler(nil), m.changeHandlers...)
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
	endpoints, _, err := client.Health().Service(name, tagMicroservice, consulPassingOnly, q)
	if err != nil {
		m.logger.Warnf("Could not retrieve service catalog from consul: %v", err)
		return nil, err
	}
	return endpoints, nil
}

var _ ports.Clock = realClock{}

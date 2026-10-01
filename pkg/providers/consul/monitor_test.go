package consul

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"

	"spotter/internal/ports"
	"spotter/internal/testkit/consulmock"
	"spotter/internal/testkit/fakes"
)

type staticConsulClientFactory struct {
	client *api.Client
	err    error
	calls  chan struct{}
}

func (f *staticConsulClientFactory) ConsulClientFactory() (*api.Client, error) {
	if f.calls != nil {
		f.calls <- struct{}{}
	}
	return f.client, f.err
}

type observedClock struct {
	*fakes.FakeClock
	afterCalls chan time.Duration
}

func (c *observedClock) After(d time.Duration) <-chan time.Time {
	ch := c.FakeClock.After(d)
	c.afterCalls <- d
	return ch
}

func TestNewConsulMonitorRejectsNilFactory(t *testing.T) {
	monitor, err := NewConsulMonitor(nil, nil, nil, nil)
	if err == nil {
		t.Fatal("NewConsulMonitor() error = nil, want nil factory error")
	}
	if monitor != nil {
		t.Fatalf("NewConsulMonitor() monitor = %#v, want nil", monitor)
	}
	if err.Error() != "new consul monitor with nil consul client" {
		t.Fatalf("NewConsulMonitor() error = %q", err)
	}
}

func TestNewConsulMonitorRejectsTypedNilFactory(t *testing.T) {
	var factory *ClientFactorySimple
	monitor, err := NewConsulMonitor(factory, nil, nil, nil)
	if err == nil {
		t.Fatal("NewConsulMonitor() error = nil, want typed-nil factory error")
	}
	if monitor != nil {
		t.Fatalf("NewConsulMonitor() monitor = %#v, want nil", monitor)
	}
	if err.Error() != "new consul monitor with nil consul client" {
		t.Fatalf("NewConsulMonitor() error = %q", err)
	}
}

func TestNewConsulMonitorDefaultsTypedNilDependencies(t *testing.T) {
	var logger *fakes.FakeLogger
	var notifier *fakes.FakeNotifier
	var clock *fakes.FakeClock

	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, logger, notifier, clock)
	monitor.logger.Info("discarded")
	monitor.notifier.Notify("discarded", "discarded")

	select {
	case <-monitor.clock.After(0):
	case <-time.After(2 * time.Second):
		t.Fatal("default clock After(0) did not become ready")
	}
	if monitor.clock.Now().IsZero() {
		t.Fatal("default clock Now() returned zero time")
	}
}

func TestConsulMonitorGetServicesUsesFactoryClient(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	server.SetServices(map[string][]string{
		"payments": {"microservice", "v2"},
	})

	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: newConsulClient(t, server.Address())}, nil, nil, nil)
	services, err := monitor.GetServices()
	if err != nil {
		t.Fatalf("GetServices() error = %v", err)
	}
	want := map[string][]string{"payments": {"microservice", "v2"}}
	if !reflect.DeepEqual(services, want) {
		t.Fatalf("GetServices() = %#v, want %#v", services, want)
	}

	requests := server.Requests()
	if len(requests) != 1 || requests[0].Path != "/v1/catalog/services" {
		t.Fatalf("GetServices() requests = %#v, want one catalog services request", requests)
	}
}

func TestConsulMonitorGetServiceEntriesUsesMicroserviceTagAndPassingOnly(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	server.SetEntries("payments", []*api.ServiceEntry{
		{
			Service: &api.AgentService{ID: "passing", Service: "payments", Tags: []string{"microservice"}},
			Checks:  api.HealthChecks{&api.HealthCheck{Status: api.HealthPassing}},
		},
		{
			Service: &api.AgentService{ID: "wrong-tag", Service: "payments", Tags: []string{"legacy"}},
			Checks:  api.HealthChecks{&api.HealthCheck{Status: api.HealthPassing}},
		},
		{
			Service: &api.AgentService{ID: "critical", Service: "payments", Tags: []string{"microservice"}},
			Checks:  api.HealthChecks{&api.HealthCheck{Status: api.HealthCritical}},
		},
	})

	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: newConsulClient(t, server.Address())}, nil, nil, nil)
	entries, err := monitor.GetServiceEntries("payments", &api.QueryOptions{Datacenter: "dc-test"})
	if err != nil {
		t.Fatalf("GetServiceEntries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Service.ID != "passing" {
		t.Fatalf("GetServiceEntries() = %#v, want only passing microservice entry", entries)
	}

	requests := server.Requests()
	if len(requests) != 1 {
		t.Fatalf("GetServiceEntries() request count = %d, want 1", len(requests))
	}
	request := requests[0]
	if request.Path != "/v1/health/service/payments" {
		t.Fatalf("GetServiceEntries() path = %q", request.Path)
	}
	if got := request.Query["tag"]; !reflect.DeepEqual(got, []string{"microservice"}) {
		t.Fatalf("GetServiceEntries() tag query = %#v, want [microservice]", got)
	}
	if got := request.Query.Get("passing"); got != "1" {
		t.Fatalf("GetServiceEntries() passing query = %q, want 1", got)
	}
	if got := request.Query.Get("dc"); got != "dc-test" {
		t.Fatalf("GetServiceEntries() dc query = %q, want dc-test", got)
	}
}

func TestConsulMonitorRecordsCatalogAndHealthRequestOutcomes(t *testing.T) {
	server := consulmock.Start()
	server.SetServices(map[string][]string{"payments": {"microservice"}})
	server.SetEntries("payments", []*api.ServiceEntry{{
		Service: &api.AgentService{ID: "passing", Service: "payments", Tags: []string{"microservice"}},
		Checks:  api.HealthChecks{&api.HealthCheck{Status: api.HealthPassing}},
	}})
	factory := &staticConsulClientFactory{client: newConsulClient(t, server.Address())}
	monitor := newTestConsulMonitor(t, factory, nil, nil, nil)
	recorder := fakes.NewFakeMetricsRecorder()
	monitor.SetMetricSource("catalog-a")
	monitor.SetConsulRequestMetricsRecorder(recorder)
	if _, err := monitor.GetServices(); err != nil {
		t.Fatalf("GetServices() error = %v", err)
	}
	if _, err := monitor.GetServiceEntries("payments", nil); err != nil {
		t.Fatalf("GetServiceEntries() error = %v", err)
	}
	server.Close()
	if _, err := monitor.GetServices(); err == nil {
		t.Fatal("GetServices() after close error = nil")
	}
	if _, err := monitor.GetServiceEntries("payments", nil); err == nil {
		t.Fatal("GetServiceEntries() after close error = nil")
	}

	observations := recorder.ConsulRequestObservations()
	if len(observations) != 4 {
		t.Fatalf("request observations = %#v, want two success and two error observations", observations)
	}
	want := []struct {
		operation string
		outcome   string
	}{
		{operation: "catalog_services", outcome: "success"},
		{operation: "health_service", outcome: "success"},
		{operation: "catalog_services", outcome: "error"},
		{operation: "health_service", outcome: "error"},
	}
	for i, expected := range want {
		got := observations[i]
		if got.Source != "catalog-a" || got.Operation != expected.operation || got.Outcome != expected.outcome || got.Duration < 0 {
			t.Fatalf("request observation %d = %#v, want source catalog-a/%s/%s/non-negative duration", i, got, expected.operation, expected.outcome)
		}
	}
}

func TestConsulMonitorRecordsBlockingHealthStateCancellation(t *testing.T) {
	client, requests, _ := newScriptedHealthStateClient(t)
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, nil, nil, nil)
	recorder := fakes.NewFakeMetricsRecorder()
	monitor.SetMetricSource("catalog-b")
	monitor.SetConsulRequestMetricsRecorder(recorder)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- monitor.watchConsul(ctx, make(chan struct{}, 1)) }()
	assertWatchRequest(t, requests, "")
	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
	observations := recorder.ConsulRequestObservations()
	if len(observations) != 1 {
		t.Fatalf("blocking request observations = %#v, want one", observations)
	}
	if got := observations[0]; got.Source != "catalog-b" || got.Operation != "health_state" || got.Outcome != "cancel" || got.Duration < 0 {
		t.Fatalf("blocking request observation = %#v, want catalog-b/health_state/cancel/non-negative duration", got)
	}
}

func TestConsulMonitorFactoryFailureNotifiesWaitsFiveSecondsAndCancelUnblocks(t *testing.T) {
	factoryErr := errors.New("factory unavailable")
	factory := &staticConsulClientFactory{err: factoryErr, calls: make(chan struct{}, 1)}
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(100, 0)),
		afterCalls: make(chan time.Duration, 4),
	}
	monitor := newTestConsulMonitor(t, factory, logger, notifier, clock)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- monitor.watchConsul(ctx, make(chan struct{}))
	}()

	awaitSignal(t, factory.calls, "factory call")
	if got := awaitDuration(t, clock.afterCalls); got != blockQueryWaitTime {
		t.Fatalf("factory retry wait = %v, want %v", got, blockQueryWaitTime)
	}
	wantNotice := []fakes.Notification{{
		Title:   "Failed to initialize the consul client while watching for consul data changes",
		Content: factoryErr.Error(),
	}}
	if got := notifier.Notifications(); !reflect.DeepEqual(got, wantNotice) {
		t.Fatalf("factory failure notices = %#v, want %#v", got, wantNotice)
	}

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulMonitorHealthStateUsesWaitIndexAndFiveSecondWait(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(150, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	factory := &staticConsulClientFactory{
		client: newConsulClient(t, server.Address()),
		calls:  make(chan struct{}, 4),
	}
	monitor := newTestConsulMonitor(t, factory, nil, nil, clock)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	change := make(chan struct{}, 4)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	awaitSignal(t, factory.calls, "first factory call")
	awaitRequestCount(t, server, 1)
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("periodic wait = %v, want %v", got, periodicCheckTime)
	}
	clock.Advance(periodicCheckTime)
	awaitSignal(t, factory.calls, "second factory call")
	awaitRequestCount(t, server, 2)

	requests := server.Requests()
	request := requests[1]
	if got := request.Path; got != "/v1/health/state/any" {
		t.Fatalf("Health.State path = %q", got)
	}
	if got := request.Query.Get("index"); got != "1" {
		t.Fatalf("Health.State index query = %q, want 1", got)
	}
	if got := request.Query.Get("wait"); got != "5000ms" {
		t.Fatalf("Health.State wait query = %q, want 5000ms", got)
	}

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulWatchIndexRejectsMissingAndZeroMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		meta *api.QueryMeta
	}{
		{name: "missing"},
		{name: "zero", meta: &api.QueryMeta{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, lastIndex := range []uint64{0, 10} {
				index := consulWatchIndex{waitIndex: lastIndex, lastIndex: lastIndex}
				changed, reset := index.observe(tc.meta)
				if changed || reset {
					t.Fatalf("observe(%#v) = changed %v, reset %v, want neither", tc.meta, changed, reset)
				}
				if index.waitIndex != 1 || index.lastIndex != lastIndex {
					t.Fatalf("index after invalid metadata = %#v, want wait 1 and last %d", index, lastIndex)
				}
			}
		})
	}
}

func TestConsulMonitorZeroIndexUsesBlockingFloorWithoutChange(t *testing.T) {
	client, requests, responses := newScriptedHealthStateClient(t)
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(175, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, nil, nil, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	change := make(chan struct{}, 4)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	assertWatchRequest(t, requests, "")
	for attempt := 0; attempt < 2; attempt++ {
		responses <- healthStateResponse{index: 0}
		assertPeriodicWatchWait(t, clock)
		assertNoWatchChange(t, change)
		assertNoWatchRequest(t, requests)
		clock.Advance(periodicCheckTime)
		assertWatchRequest(t, requests, "1")
	}

	responses <- healthStateResponse{index: 10}
	awaitSignal(t, change, "first valid index change")
	assertPeriodicWatchWait(t, clock)
	assertNoWatchChange(t, change)
	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulMonitorWatchConsulPublishesHealthStateReturnTimestamp(t *testing.T) {
	client, requests, responses := newScriptedHealthStateClient(t)
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(190, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, nil, nil, clock)
	monitor.watchTimestamps = make(chan time.Time, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	change := make(chan struct{}, 1)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	assertWatchRequest(t, requests, "")
	responses <- healthStateResponse{index: 10}
	awaitSignal(t, change, "watch change")
	watchAt := monitor.takeWatchTimestamp()
	if watchAt.IsZero() || !watchAt.Equal(clock.Now()) {
		t.Fatalf("watch origin = %v, want fake clock time %v", watchAt, clock.Now())
	}
	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulMonitorIndexRollbackRetriesImmediatelyAndSignalsOnce(t *testing.T) {
	client, requests, responses := newScriptedHealthStateClient(t)
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(180, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, nil, nil, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	change := make(chan struct{}, 4)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	assertWatchRequest(t, requests, "")
	responses <- healthStateResponse{index: 10}
	awaitSignal(t, change, "initial index change")
	assertPeriodicWatchWait(t, clock)
	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "10")
	responses <- healthStateResponse{index: 20}
	awaitSignal(t, change, "second index change")
	assertPeriodicWatchWait(t, clock)
	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "20")

	responses <- healthStateResponse{index: 15}
	awaitSignal(t, change, "rollback change")
	// The reset query must start without advancing the periodic retry clock.
	assertWatchRequest(t, requests, "")
	responses <- healthStateResponse{index: 15}
	assertPeriodicWatchWait(t, clock)
	assertNoWatchChange(t, change)
	assertNoWatchRequest(t, requests)
	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "15")
	responses <- healthStateResponse{index: 15}
	assertPeriodicWatchWait(t, clock)
	assertNoWatchChange(t, change)

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulMonitorUnchangedTimeoutAndErrorRetainIndexAndRateLimit(t *testing.T) {
	client, requests, responses := newScriptedHealthStateClient(t)
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(185, 0)),
		afterCalls: make(chan time.Duration, 16),
	}
	logger, notifier := &fakes.FakeLogger{}, &fakes.FakeNotifier{}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, logger, notifier, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	change := make(chan struct{}, 4)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	assertWatchRequest(t, requests, "")
	responses <- healthStateResponse{index: 10}
	awaitSignal(t, change, "initial index change")
	assertPeriodicWatchWait(t, clock)

	for attempt := 0; attempt < 3; attempt++ {
		clock.Advance(periodicCheckTime)
		assertWatchRequest(t, requests, "10")
		responses <- healthStateResponse{index: 10}
		assertPeriodicWatchWait(t, clock)
		assertNoWatchChange(t, change)
		assertNoWatchRequest(t, requests)
	}

	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "10")
	// A blocking query may consume its whole wait period and return the same
	// index. Its completion is not a change notification.
	clock.Advance(blockQueryWaitTime)
	responses <- healthStateResponse{index: 10}
	assertPeriodicWatchWait(t, clock)
	assertNoWatchChange(t, change)
	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "10")
	responses <- healthStateResponse{status: http.StatusInternalServerError}
	assertPeriodicWatchWait(t, clock)
	assertNoWatchChange(t, change)
	assertNoWatchRequest(t, requests)
	if got := notifier.Notifications(); len(got) != 1 || got[0].Title != "Failed to fetch data from consul while watching for consul data changes" {
		t.Fatalf("health failure notices = %#v, want one fetch failure notice", got)
	}
	if !hasLog(logger.Entries(), "warn", "could not fetch services:") {
		t.Fatalf("missing health failure log: %#v", logger.Entries())
	}
	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "10")

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulChangeLimiterAllowsBurstThenRefills(t *testing.T) {
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(190, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	limiter := newConsulChangeLimiter(watchChangeBurst, watchChangeRefill)
	ctx := context.Background()

	if !limiter.waitForChange(ctx, clock) || !limiter.waitForChange(ctx, clock) {
		t.Fatal("limiter rejected one of the configured burst tokens")
	}

	third := make(chan bool, 1)
	go func() { third <- limiter.waitForChange(ctx, clock) }()
	if got := awaitDuration(t, clock.afterCalls); got != watchChangeRefill {
		t.Fatalf("third rapid change delay = %v, want %v", got, watchChangeRefill)
	}
	select {
	case <-third:
		t.Fatal("third rapid change bypassed the refill delay")
	default:
	}
	clock.Advance(watchChangeRefill)
	select {
	case ok := <-third:
		if !ok {
			t.Fatal("third rapid change was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for third change after refill")
	}

	fourth := make(chan bool, 1)
	go func() { fourth <- limiter.waitForChange(ctx, clock) }()
	if got := awaitDuration(t, clock.afterCalls); got != watchChangeRefill {
		t.Fatalf("fourth rapid change delay = %v, want %v", got, watchChangeRefill)
	}
	select {
	case <-fourth:
		t.Fatal("fourth rapid change reused the consumed token")
	default:
	}
	clock.Advance(watchChangeRefill)
	select {
	case ok := <-fourth:
		if !ok {
			t.Fatal("fourth rapid change was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fourth change after refill")
	}

	// A sparse change after one refill interval proceeds without another
	// limiter sleep.
	clock.Advance(watchChangeRefill)
	if !limiter.waitForChange(ctx, clock) {
		t.Fatal("sparse change was rejected after token refill")
	}
}

func TestConsulChangeLimiterContextCancellation(t *testing.T) {
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(195, 0)),
		afterCalls: make(chan time.Duration, 4),
	}
	limiter := newConsulChangeLimiter(watchChangeBurst, watchChangeRefill)
	if !limiter.waitForChange(context.Background(), clock) || !limiter.waitForChange(context.Background(), clock) {
		t.Fatal("limiter failed to consume burst tokens")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if limiter.waitForChange(ctx, clock) {
		t.Fatal("rate-limited change succeeded after context cancellation")
	}
}

func TestConsulMonitorRapidChangesAreRateLimitedAfterBurst(t *testing.T) {
	client, requests, responses := newScriptedHealthStateClient(t)
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(200, 0)),
		afterCalls: make(chan time.Duration, 16),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: client}, nil, nil, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	change := make(chan struct{}, 4)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	assertWatchRequest(t, requests, "")
	responses <- healthStateResponse{index: 10}
	awaitSignal(t, change, "first rapid change")
	assertPeriodicWatchWait(t, clock)

	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "10")
	responses <- healthStateResponse{index: 20}
	awaitSignal(t, change, "second rapid change")
	assertPeriodicWatchWait(t, clock)

	clock.Advance(periodicCheckTime)
	assertWatchRequest(t, requests, "20")
	responses <- healthStateResponse{index: 30}
	if got := awaitDuration(t, clock.afterCalls); got != watchChangeRefill {
		t.Fatalf("third rapid change delay = %v, want %v", got, watchChangeRefill)
	}
	assertNoWatchChange(t, change)
	clock.Advance(watchChangeRefill)
	awaitSignal(t, change, "third change after rate limit")
	assertPeriodicWatchWait(t, clock)

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

func TestConsulMonitorWatchConsulCancelUnblocksFullChangeChannel(t *testing.T) {
	server := consulmock.Start()
	defer server.Close()
	factory := &staticConsulClientFactory{
		client: newConsulClient(t, server.Address()),
		calls:  make(chan struct{}, 1),
	}
	monitor := newTestConsulMonitor(t, factory, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	change := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- monitor.watchConsul(ctx, change) }()

	awaitSignal(t, factory.calls, "factory call")
	awaitRequestCount(t, server, 1)
	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("watchConsul() error = %v", err)
	}
}

type healthStateResponse struct {
	index  uint64
	status int
}

func newScriptedHealthStateClient(t *testing.T) (*api.Client, <-chan url.Values, chan<- healthStateResponse) {
	t.Helper()
	requests := make(chan url.Values, 1)
	responses := make(chan healthStateResponse, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		select {
		case requests <- request.URL.Query():
		case <-request.Context().Done():
			return
		}
		var response healthStateResponse
		select {
		case response = <-responses:
		case <-request.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Consul-Index", strconv.FormatUint(response.index, 10))
		status := response.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(server.Close)
	return newConsulClient(t, server.URL), requests, responses
}

func assertWatchRequest(t *testing.T, requests <-chan url.Values, wantIndex string) {
	t.Helper()
	select {
	case query := <-requests:
		if got := query.Get("index"); got != wantIndex {
			t.Fatalf("Health.State index query = %q, want %q", got, wantIndex)
		}
		if got := query.Get("wait"); got != "5000ms" {
			t.Fatalf("Health.State wait query = %q, want 5000ms", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for health state request")
	}
}

func assertNoWatchRequest(t *testing.T, requests <-chan url.Values) {
	t.Helper()
	select {
	case query := <-requests:
		t.Fatalf("Health.State bypassed periodic retry wait: %#v", query)
	default:
	}
}

func assertNoWatchChange(t *testing.T, change <-chan struct{}) {
	t.Helper()
	select {
	case <-change:
		t.Fatal("unexpected Consul change notification")
	default:
	}
}

func assertPeriodicWatchWait(t *testing.T, clock *observedClock) {
	t.Helper()
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("periodic watch wait = %v, want %v", got, periodicCheckTime)
	}
}
func TestConsulMonitorDebouncesChangeForFiftyMilliseconds(t *testing.T) {
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(200, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, nil, nil, clock)
	called := make(chan struct{}, 1)
	monitor.AppendInstanceHandler(func(*api.CatalogService) error {
		called <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	change := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- monitor.updateRecord(ctx, change)
	}()

	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("periodic check wait = %v, want %v", got, periodicCheckTime)
	}
	change <- struct{}{}
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("periodic check wait after change = %v, want %v", got, periodicCheckTime)
	}

	clock.Advance(refreshIdleTime - time.Millisecond)
	select {
	case <-called:
		t.Fatal("handler called before refresh idle time elapsed")
	default:
	}
	clock.Advance(time.Millisecond)
	awaitSignal(t, called, "instance handler call")

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("updateRecord() error = %v", err)
	}
}

func TestConsulMonitorDebounceResetsForMultipleChangesAndRunsOnce(t *testing.T) {
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(250, 0)),
		afterCalls: make(chan time.Duration, 8),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, nil, nil, clock)
	calls := make(chan struct{}, 2)
	monitor.AppendInstanceHandler(func(*api.CatalogService) error {
		calls <- struct{}{}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	change := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- monitor.updateRecord(ctx, change) }()
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("initial periodic check = %v, want %v", got, periodicCheckTime)
	}

	sendChange := func() {
		delivered := make(chan struct{})
		go func() { change <- struct{}{}; close(delivered) }()
		awaitSignal(t, delivered, "change delivery")
		if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
			t.Fatalf("reset periodic check = %v, want %v", got, periodicCheckTime)
		}
	}
	sendChange()
	clock.Advance(30 * time.Millisecond)
	sendChange()
	clock.Advance(20 * time.Millisecond)
	select {
	case <-calls:
		t.Fatal("handler called before the latest idle window elapsed")
	default:
	}
	clock.Advance(29 * time.Millisecond)
	select {
	case <-calls:
		t.Fatal("handler called before 50ms of idle time")
	default:
	}
	clock.Advance(time.Millisecond)
	awaitSignal(t, calls, "debounced handler call")
	select {
	case <-calls:
		t.Fatal("duplicate handler call after one debounced change")
	default:
	}

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("updateRecord() error = %v", err)
	}
}

func TestConsulMonitorDebouncePreservesEarliestWatchOrigin(t *testing.T) {
	clock := &observedClock{
		FakeClock:  fakes.NewFakeClock(time.Unix(300, 0)),
		afterCalls: make(chan time.Duration, 12),
	}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, nil, nil, clock)
	monitor.watchTimestamps = make(chan time.Time, 2)
	origins := make(chan time.Time, 1)
	monitor.AppendTimestampedInstanceChangeHandler(func(watchAt time.Time) error {
		origins <- watchAt
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	change := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- monitor.updateRecord(ctx, change) }()
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("initial periodic check = %v, want %v", got, periodicCheckTime)
	}

	first := clock.Now().Add(-200 * time.Millisecond)
	second := clock.Now().Add(-100 * time.Millisecond)
	monitor.watchTimestamps <- first
	change <- struct{}{}
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("first reset periodic check = %v, want %v", got, periodicCheckTime)
	}
	clock.Advance(30 * time.Millisecond)
	monitor.watchTimestamps <- second
	change <- struct{}{}
	if got := awaitDuration(t, clock.afterCalls); got != periodicCheckTime {
		t.Fatalf("second reset periodic check = %v, want %v", got, periodicCheckTime)
	}
	clock.Advance(50 * time.Millisecond)
	select {
	case got := <-origins:
		if !got.Equal(first) {
			t.Fatalf("coalesced watch origin = %v, want earliest %v", got, first)
		}
	case <-time.After(time.Second):
		t.Fatal("timed handler did not run after debounce")
	}
	select {
	case extra := <-origins:
		t.Fatalf("duplicate timed handler origin = %v", extra)
	default:
	}

	cancel()
	if err := awaitError(t, done); err != nil {
		t.Fatalf("updateRecord() error = %v", err)
	}
}

func TestConsulMonitorTimestampedHandlerRequiresOrigin(t *testing.T) {
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, nil, nil, nil)
	called := make(chan struct{}, 1)
	monitor.AppendTimestampedInstanceChangeHandler(func(time.Time) error {
		called <- struct{}{}
		return nil
	})
	monitor.updateInstanceRecord()
	select {
	case <-called:
		t.Fatal("timestamped handler ran without a watch origin")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestConsulMonitorHandlerSnapshotsAreSafeDuringConcurrentUpdates(t *testing.T) {
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, nil, nil, nil)
	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			monitor.AppendInstanceHandler(func(*api.CatalogService) error { return nil })
		}()
		go func() {
			defer wg.Done()
			monitor.AppendServiceHandler(func([]*api.CatalogService) error { return nil })
		}()
	}
	wg.Wait()
	monitor.updateInstanceRecord()
	monitor.updateServiceRecord()
}

func TestConsulMonitorHandlerErrorsNotifyAndLogExactMessages(t *testing.T) {
	logger := &fakes.FakeLogger{}
	notifier := &fakes.FakeNotifier{}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{}, logger, notifier, nil)
	monitor.AppendInstanceHandler(func(*api.CatalogService) error {
		return errors.New("instance handler failed")
	})
	monitor.AppendServiceHandler(func([]*api.CatalogService) error {
		return errors.New("service handler failed")
	})
	monitor.updateInstanceRecord()
	monitor.updateServiceRecord()
	awaitNotification(t, notifier, "Failed to handle the consul instance change")
	awaitLog(t, logger, "warn", "Error executing instance handler function: instance handler failed")
	awaitLog(t, logger, "warn", "Error executing service handler function: service handler failed")

	wantNotice := []fakes.Notification{{
		Title:   "Failed to handle the consul instance change",
		Content: "instance handler failed",
	}}
	if got := notifier.Notifications(); !reflect.DeepEqual(got, wantNotice) {
		t.Fatalf("handler notices = %#v, want %#v", got, wantNotice)
	}
	entries := logger.Entries()
	if !hasLog(entries, "warn", "Error executing instance handler function: instance handler failed") {
		t.Fatalf("missing instance handler log in %#v", entries)
	}
	if !hasLog(entries, "warn", "Error executing service handler function: service handler failed") {
		t.Fatalf("missing service handler log in %#v", entries)
	}
}

func TestConsulMonitorGetServicesFactoryAndAPIErrorsPropagateAndLog(t *testing.T) {
	factoryErr := errors.New("factory unavailable")
	logger := &fakes.FakeLogger{}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{err: factoryErr}, logger, nil, nil)
	if _, err := monitor.GetServices(); !errors.Is(err, factoryErr) {
		t.Fatalf("GetServices() error = %v, want factory error", err)
	}
	if !hasLog(logger.Entries(), "error", "get consul client: factory unavailable") {
		t.Fatalf("missing factory error log: %#v", logger.Entries())
	}

	server := consulmock.Start()
	address := server.Address()
	server.Close()
	apiLogger := &fakes.FakeLogger{}
	apiMonitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: newConsulClient(t, address)}, apiLogger, nil, nil)
	if _, err := apiMonitor.GetServices(); err == nil {
		t.Fatal("GetServices() error = nil, want API error")
	}
	if !hasLog(apiLogger.Entries(), "warn", "Could not retrieve services from consul:") {
		t.Fatalf("missing API error log: %#v", apiLogger.Entries())
	}
}

func TestConsulMonitorGetServiceEntriesFactoryAndAPIErrorsPropagateAndLog(t *testing.T) {
	factoryErr := errors.New("factory unavailable")
	logger := &fakes.FakeLogger{}
	monitor := newTestConsulMonitor(t, &staticConsulClientFactory{err: factoryErr}, logger, nil, nil)
	if _, err := monitor.GetServiceEntries("payments", nil); !errors.Is(err, factoryErr) {
		t.Fatalf("GetServiceEntries() error = %v, want factory error", err)
	}
	if !hasLog(logger.Entries(), "error", "get consul client: factory unavailable") {
		t.Fatalf("missing factory error log: %#v", logger.Entries())
	}

	server := consulmock.Start()
	address := server.Address()
	server.Close()
	apiLogger := &fakes.FakeLogger{}
	apiMonitor := newTestConsulMonitor(t, &staticConsulClientFactory{client: newConsulClient(t, address)}, apiLogger, nil, nil)
	if _, err := apiMonitor.GetServiceEntries("payments", nil); err == nil {
		t.Fatal("GetServiceEntries() error = nil, want API error")
	}
	if !hasLog(apiLogger.Entries(), "warn", "Could not retrieve service catalog from consul:") {
		t.Fatalf("missing API error log: %#v", apiLogger.Entries())
	}
}
func newTestConsulMonitor(t *testing.T, factory ConsulClientFactory, logger ports.Logger, notifier ports.Notifier, clock ports.Clock) *consulMonitor {
	t.Helper()
	monitor, err := NewConsulMonitor(factory, logger, notifier, clock)
	if err != nil {
		t.Fatalf("NewConsulMonitor() error = %v", err)
	}
	concrete, ok := monitor.(*consulMonitor)
	if !ok {
		t.Fatalf("NewConsulMonitor() type = %T, want *consulMonitor", monitor)
	}
	return concrete
}

func newConsulClient(t *testing.T, address string) *api.Client {
	t.Helper()
	config := api.DefaultConfig()
	config.Address = address
	client, err := api.NewClient(config)
	if err != nil {
		t.Fatalf("api.NewClient() error = %v", err)
	}
	return client
}

func awaitSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func awaitDuration(t *testing.T, ch <-chan time.Duration) time.Duration {
	t.Helper()
	select {
	case duration := <-ch:
		return duration
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for clock After call")
		return 0
	}
}

func awaitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for goroutine to stop")
		return nil
	}
}

func awaitRequestCount(t *testing.T, server *consulmock.Server, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if len(server.Requests()) >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %d Consul requests", want)
		case <-ticker.C:
		}
	}
}

func hasLog(entries []fakes.LogEntry, level, message string) bool {
	for _, entry := range entries {
		if entry.Level == level && strings.Contains(entry.Message, message) {
			return true
		}
	}
	return false
}

// awaitNotification polls the fake notifier until the wanted title is captured.
func awaitNotification(t *testing.T, notifier *fakes.FakeNotifier, title string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, notification := range notifier.Notifications() {
			if notification.Title == title {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for notification %q, captured %#v", title, notifier.Notifications())
}

// awaitLog polls the fake logger until the wanted entry is captured.
func awaitLog(t *testing.T, logger *fakes.FakeLogger, level, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hasLog(logger.Entries(), level, message) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s log %q, captured %#v", level, message, logger.Entries())
}

func TestMonitorDispatchesLegacyAndPayloadFreeHandlers(t *testing.T) {
	logger, notifier := &fakes.FakeLogger{}, &fakes.FakeNotifier{}
	monitor := &consulMonitor{logger: logger, notifier: notifier}
	legacy, modern := make(chan struct{}, 1), make(chan struct{}, 1)
	monitor.AppendInstanceHandler(func(*api.CatalogService) error { legacy <- struct{}{}; return errors.New("legacy") })
	monitor.AppendInstanceChangeHandler(func() error { modern <- struct{}{}; return errors.New("modern") })
	monitor.updateInstanceRecord()
	select {
	case <-legacy:
	case <-time.After(time.Second):
		t.Fatal("legacy handler not called")
	}
	select {
	case <-modern:
	case <-time.After(time.Second):
		t.Fatal("payload-free handler not called")
	}
	awaitNotification(t, notifier, "Failed to handle the consul instance change")
	awaitLog(t, logger, "warn", "modern")
}

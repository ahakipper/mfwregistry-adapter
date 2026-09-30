package consul

import (
	"context"
	"errors"
	"testing"

	"github.com/hashicorp/consul/api"

	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
)

// metricsMonitor is a narrow source-boundary double. It lets the tests pin
// the distinction between a healthy empty catalog and source/partial errors
// without requiring a Consul HTTP server.
type metricsMonitor struct {
	services    map[string][]string
	entries     map[string][]*api.ServiceEntry
	servicesErr error
	entriesErr  error
}

func (m *metricsMonitor) Start(context.Context) error { return nil }

func (m *metricsMonitor) GetServices() (map[string][]string, error) {
	if m.servicesErr != nil {
		return nil, m.servicesErr
	}
	return m.services, nil
}

func (m *metricsMonitor) GetServiceEntries(name string, _ *api.QueryOptions) ([]*api.ServiceEntry, error) {
	if m.entriesErr != nil {
		return nil, m.entriesErr
	}
	return m.entries[name], nil
}

func (m *metricsMonitor) AppendServiceHandler(ServiceHandler)   {}
func (m *metricsMonitor) AppendInstanceHandler(InstanceHandler) {}

func newMetricsProvider(m Monitor, recorder *fakes.FakeMetricsRecorder) *consul {
	return &consul{
		providerName:    "consul",
		sourceCluster:   "catalog-a",
		monitor:         m,
		logger:          ports.NopLogger{},
		metricsRecorder: recorder,
	}
}

func TestConsulSourceMetricsDistinguishReadOutcomes(t *testing.T) {
	readErr := errors.New("catalog unavailable")
	recorder := fakes.NewFakeMetricsRecorder()
	c := newMetricsProvider(&metricsMonitor{services: map[string][]string{}}, recorder)
	if got := c.GetAll(); got == nil || len(got) != 0 {
		t.Fatalf("healthy empty GetAll() = %#v, want non-nil empty snapshot", got)
	}

	c.monitor = &metricsMonitor{servicesErr: readErr}
	if got := c.GetAll(); got != nil {
		t.Fatalf("source-error GetAll() = %#v, want nil", got)
	}

	c.monitor = &metricsMonitor{
		services:   map[string][]string{"orders": {"microservice"}},
		entriesErr: errors.New("service read failed"),
	}
	if got := c.GetAll(); got != nil {
		t.Fatalf("partial GetAll() = %#v, want nil", got)
	}

	reads := recorder.ConsulCatalogReadObservations()
	if len(reads) != 3 {
		t.Fatalf("catalog read observations = %#v, want 3", reads)
	}
	if reads[0].Source != "catalog-a" || reads[0].Outcome != consulMetricOutcomeHealthyEmpty {
		t.Fatalf("healthy-empty read = %#v, want source/outcome catalog-a/healthy_empty", reads[0])
	}
	if reads[1].Outcome != consulMetricOutcomeSourceError || reads[2].Outcome != consulMetricOutcomePartial {
		t.Fatalf("error read outcomes = %#v, want source_error then partial", reads)
	}
	errorsSeen := recorder.ConsulSourceErrorObservations()
	if len(errorsSeen) != 2 || errorsSeen[0].Outcome != consulMetricOutcomeSourceError || errorsSeen[1].Outcome != consulMetricOutcomePartial {
		t.Fatalf("source error observations = %#v, want bounded outcomes", errorsSeen)
	}
}

func TestConsulSourceMetricsCountConversionSkips(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	c := newMetricsProvider(&metricsMonitor{
		services: map[string][]string{"orders": {"microservice"}},
		entries:  map[string][]*api.ServiceEntry{"orders": {nil, nil}},
	}, recorder)
	if got := c.GetAll(); got != nil {
		t.Fatalf("conversion-partial GetAll() = %#v, want nil", got)
	}
	skips := recorder.ConsulConversionSkipsObservations()
	if len(skips) != 1 || skips[0].Source != "catalog-a" || skips[0].Count != 2 {
		t.Fatalf("conversion skips = %#v, want one source-scoped count of 2", skips)
	}
	reads := recorder.ConsulCatalogReadObservations()
	if len(reads) != 1 || reads[0].Outcome != consulMetricOutcomePartial {
		t.Fatalf("conversion read observations = %#v, want partial", reads)
	}
}

func TestConsulSourceMetricsCountHealthyEmptyConfirmations(t *testing.T) {
	recorder := fakes.NewFakeMetricsRecorder()
	c := newMetricsProvider(&metricsMonitor{services: map[string][]string{}}, recorder)
	for i := 0; i < 3; i++ {
		if err := c.syncInstance(); err == nil {
			t.Fatalf("syncInstance() iteration %d error = nil, want confirmation gate error", i+1)
		}
	}
	confirmations := recorder.ConsulHealthyEmptyConfirmationObservations()
	if len(confirmations) != 3 {
		t.Fatalf("healthy-empty confirmations = %#v, want 3", confirmations)
	}
	if confirmations[0].Outcome != consulMetricOutcomePending || confirmations[1].Outcome != consulMetricOutcomePending || confirmations[2].Outcome != consulMetricOutcomeConfirmed {
		t.Fatalf("healthy-empty confirmation outcomes = %#v, want pending,pending,confirmed", confirmations)
	}
}

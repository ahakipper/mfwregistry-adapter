package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// syncAll cost time and syncOnce cost time
var (
	SyncAllDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "sync_all_durations_histogram",
			Help: "",
			ConstLabels: map[string]string{
				"provider": "all",
			},
			Buckets: prometheus.LinearBuckets(0.0, 1000, 10),
		})
	SyncAllK8sDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "sync_all_durations_histogram",
			Help: "",
			ConstLabels: map[string]string{
				"provider": "k8s",
			},
			Buckets: prometheus.LinearBuckets(0.0, 1000, 10),
		})
	SyncAllEcsDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "sync_all_durations_histogram",
			Help: "",
			ConstLabels: map[string]string{
				"provider": "ecs",
			},
			Buckets: prometheus.LinearBuckets(0.0, 1000, 10),
		})
	SyncOnceDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "sync_once_durations_histogram",
			Help:    "",
			Buckets: prometheus.LinearBuckets(0.0, 1000, 10),
		})
	SyncOnceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sync_once_gauge",
			Help: "",
		},
		[]string{"syncgauge"},
	)
	SyncErrorGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sync_error_gauge",
			Help: "",
		},
		[]string{"syncgauge"},
	)
	// EventToStoreE2EDuration is the end-to-end latency series of dsca-2 §3:
	// informer-callback CreateAt -> sink store-visible (2xx), per sink and
	// outcome. Buckets are the load-bearing choice: the legacy series above
	// use LinearBuckets(0, 1000, 10) — 1-second granularity, blind to
	// everything a millisecond-level chain produces — so this series spans
	// 1 ms to 10 s exponential-ish instead.
	EventToStoreE2EDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "event_to_store_e2e_duration_seconds",
			Help: "end-to-end: informer-callback CreateAt -> sink store-visible (2xx), per instance, per sink",
			Buckets: []float64{
				0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
				0.25, 0.5, 1, 2.5, 5, 10,
			},
		},
		[]string{"sink", "outcome"},
	)
	// EventsDroppedTotal is the companion counter of the latency series
	// (dsca-2 §6 row 3, co-owned with dsca-1 DS-1-1 fix item 1): without it
	// a burst that drops a fraction of its events reports beautiful
	// percentiles on the survivors. One counter, one name, one label set —
	// the unified spec both tracks land; the cluster label value is the
	// dropping watcher's kubeconfig path.
	EventsDroppedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_dropped_total",
			Help: "events dropped because the k8s robot queue was full, per cluster",
		},
		[]string{"cluster"},
	)
	// K8sQueueDepthGauge is the queueDepth gauge of dsca-1 DS-1-1 fix item
	// 1: the k8s robot's coalescing event-queue depth (distinct pod keys in
	// flight — one entry per key regardless of how many superseded events
	// coalesced into it). No labels: the robot's queue is shared across
	// every watched cluster (one 4,096-key buffer, k8srobot.go), so a
	// per-cluster split would under-report the shared depth.
	K8sQueueDepthGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "k8s_queue_depth",
			Help: "current depth of the k8s robot's coalescing event queue (distinct pod keys in flight)",
		},
	)
	// Consul source metrics keep the logical source scope separate from the
	// provider-wide synchronization metrics. Only source IDs and bounded
	// outcomes are labels; addresses, ACL tokens, and other credentials never
	// enter a metric label or value.
	ConsulCatalogReadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "consul_catalog_read_duration_seconds",
			Help: "duration of a complete Consul catalog read by source and outcome",
			Buckets: []float64{
				0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
				0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"source", "outcome"},
	)
	ConsulConversionSkipsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consul_conversion_skips_total",
			Help: "Consul catalog endpoints rejected during instance conversion",
		},
		[]string{"source", "outcome"},
	)
	ConsulSourceErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consul_source_errors_total",
			Help: "Consul catalog reads rejected by source or partial-read errors",
		},
		[]string{"source", "outcome"},
	)
	ConsulHealthyEmptyConfirmationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "consul_healthy_empty_confirmations_total",
			Help: "healthy-empty Consul catalog confirmation advancements",
		},
		[]string{"source", "outcome"},
	)
	ConsulWatchToSyncDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "consul_watch_to_sync_duration_seconds",
			Help: "duration from a Consul watch notification to provider synchronization",
			Buckets: []float64{
				0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
				0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"source", "outcome"},
	)
	ConsulLeaderProbeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "consul_leader_probe_duration_seconds",
			Help: "duration of Consul Status().Leader probes by logical source and outcome",
			Buckets: []float64{
				0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
				0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"source", "outcome"},
	)
	ConsulRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "consul_request_duration_seconds",
			Help: "duration of Consul API requests by logical source, operation, and outcome",
			Buckets: []float64{
				0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
				0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
			},
		},
		[]string{"source", "operation", "outcome"},
	)
)

func init() {
	prometheus.MustRegister(SyncAllDurationsHistogram, SyncAllK8sDurationsHistogram, SyncAllEcsDurationsHistogram, SyncOnceDurationsHistogram, SyncOnceGauge, SyncErrorGauge, EventToStoreE2EDuration, EventsDroppedTotal, K8sQueueDepthGauge, ConsulCatalogReadDuration, ConsulConversionSkipsTotal, ConsulSourceErrorsTotal, ConsulHealthyEmptyConfirmationsTotal, ConsulWatchToSyncDuration, ConsulLeaderProbeDuration, ConsulRequestDuration)
}

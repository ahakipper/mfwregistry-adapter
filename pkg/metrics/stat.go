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
)

func init() {
    prometheus.MustRegister(SyncAllDurationsHistogram, SyncAllK8sDurationsHistogram, SyncAllEcsDurationsHistogram, SyncOnceDurationsHistogram, SyncOnceGauge, SyncErrorGauge, EventToStoreE2EDuration, EventsDroppedTotal)
}

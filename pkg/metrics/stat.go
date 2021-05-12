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
)

func init() {
    prometheus.MustRegister(SyncAllDurationsHistogram, SyncAllK8sDurationsHistogram, SyncAllEcsDurationsHistogram, SyncOnceDurationsHistogram, SyncOnceGauge, SyncErrorGauge)
}

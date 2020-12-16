package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// syncAll cost time and syncOnce cost time
var (
	SyncAllDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "sync_all_durations_histogram",
			Help:    "",
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
	prometheus.MustRegister(SyncAllDurationsHistogram, SyncOnceDurationsHistogram, SyncOnceGauge,SyncErrorGauge)
}

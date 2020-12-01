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
			Buckets: prometheus.LinearBuckets(1.0,20.0,10),
		})
	SyncOnceDurationsHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "sync_once_durations_histogram",
			Help: "",
			Buckets: prometheus.LinearBuckets(0.0,1.0,10),
		})
)

func init() {
	prometheus.MustRegister(SyncAllDurationsHistogram,SyncOnceDurationsHistogram)
}

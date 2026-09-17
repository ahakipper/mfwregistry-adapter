//go:build observe

package observe

import "testing"

func TestObserveUnitScaleLadderPercentilesUsePerInstanceSamples(t *testing.T) {
	batches := []ladderSample{
		{Scale: 1000, Operation: "create", LatencySec: 100},
		{Scale: 1000, Operation: "create", LatencySec: 110},
		{Scale: 1000, Operation: "create", LatencySec: 120},
		{Scale: 1000, Operation: "create", LatencySec: 130},
	}
	instances := make([]ladderInstanceSample, 100)
	for i := range instances {
		value := float64(i + 1)
		instances[i] = ladderInstanceSample{
			Scale: 1000, Operation: "create", APIToNacosWatchSec: value,
			ExternalK8sToNacosSec: value, APIToK8sWatchSec: value,
			APIToSpotterEventSec: value, SpotterQueueSec: value,
			SpotterToNacosWatchSec: value,
		}
	}

	got := aggregateLadder(batches, instances)
	if len(got) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(got))
	}
	aggregate := got[0]
	if aggregate.Samples != 100 || aggregate.P90 != 90 || aggregate.P95 != 95 || aggregate.P99 != 99 {
		t.Fatalf("per-instance aggregate = %+v, want n=100 p90=90 p95=95 p99=99", aggregate)
	}
	if aggregate.BatchSamples != 4 || aggregate.BatchExactMin != 100 || aggregate.BatchExactMedian != 115 || aggregate.BatchExactMax != 130 {
		t.Fatalf("batch aggregate = %+v, want explicit 4-run min/average-median/max", aggregate)
	}
}

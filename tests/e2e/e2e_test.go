//go:build e2e
// +build e2e

// Package e2e hosts the end-to-end tier of the test matrix: full pipelines
// assembled from the exported API only, running against the in-process
// testkit mocks (consulmock HTTP server, discoverymock bufconn gRPC server).
// The package is guarded by the e2e build tag so it never runs as part of
// the ordinary unit tiers; `make test-e2e` selects it with -tags=e2e.
package e2e

import (
	"testing"
)

// TestE2EConsulPipeline drives the composition-less discovery pipeline:
//
//	consulmock (loopback HTTP) -> NewConsulProvider -> DefaultWorker -> DiscoveryCenter
//	                                                                    |
//	discoverymock (bufconn gRPC) <------- assert SynInstance calls <-----+
//
// The consul mock is fed a catalog and a healthy "microservice" endpoint,
// the monitor's index advances so the watch loop notices the change, and the
// debounced instance handler pushes the converted instance through the real
// DefaultWorker and DiscoveryCenter into the discovery mock. The test then
// asserts the exact instance fields observed by the mock. It is bounded (the
// poll deadline is 10s), fully offline and race-safe.
func TestE2EConsulPipeline(t *testing.T) {
	testE2EConsulPipeline(t)
}

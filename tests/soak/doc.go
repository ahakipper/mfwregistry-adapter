//go:build soak
// +build soak

// Package soak hosts the local full-stack e2e + soak harness of
// docs/nacos-sink-plan.md §8 (decision D5): a build-tag-guarded Go test
// that owns the dynamic lifecycle — embedded etcd (etcdmock), the Atlas
// stand-in (discoverymock on a TCP listener), the spotter binary as a child
// process, the churn + assertion loop and the edge scenarios — while the
// containerized stack (nacos, consul, k3s) is managed by scripts/soak-up.sh
// / soak-down.sh around the test.
//
// The spotter BINARY is the system under test, not in-process wiring: the
// soak must exercise flags, config resolution, election, dial-retry and
// process restart exactly as shipped (plan §8.1), which is why the test
// execs the binary instead of calling internal.NewServerFromDeps.
//
// Durations are driven by the environment:
//
//	SOAK_DURATION  total window (default 1h; "90s"/"5m"/"1h"; the smoke
//	               shakedown of the harness itself uses minutes)
//	SPOTTER_BIN    path of the built binary (default build/spotter)
//	KUBECONFIG     k3s kubeconfig extracted by soak-up.sh (default
//	               build/soak/kubeconfig)
package soak

//go:build observe
// +build observe

// Package observe is the sustained large-scale consistency harness of
// docs/dsca-4-observation.md §4 (batch Fix-D): the per-tick bidirectional
// observation that proves — or disproves — the user's core requirement
// ("2 hours, 1000+ instances, every observation tick both sides
// consistent").
//
// The tier is distinct from the scenario soak (make test-soak): the soak
// drives edge scenarios with heal bounds; this harness runs ONE standing
// observation loop at a ≤10s tick whose expected side is ALWAYS the live
// cluster state (the dsca-4 §3.3 self-consistency trap removed
// structurally), a churn driver with a mutation ledger (the in-flight
// clock), an in-flight-tolerant bidirectional diff with the single
// OBS_BOUND formula, a per-tick CONSISTENT/DIVERGENT/OBSERR verdict, a
// JSONL record per tick, and a §5.2 acceptance evaluation.
//
// The stack (dsca-4 §4.1, the binding ruling): a THROWAWAY stack the
// harness owns — a throwaway nacos docker container on a scratch port, an
// in-process Atlas stand-in on a scratch port, the embedded etcd, and its
// own spotter child built from the repo, pointed at a kwok cluster. The
// demo stack (its nacos at 18848, consul 18500, k3s 6443 and the demo
// spotter) is never touched: every port this harness binds is outside the
// reserved set.
package observe

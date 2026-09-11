//go:build observe
// +build observe

package observe

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The engine unit tier (the corrected-engine semantics the §6.2 review
// demanded be pinned, the batch4_test.go templates): the max-count union,
// duplicate detection, the k8s-disabled accounting, the source-error →
// OBSERR rule, the in-flight tolerance and its bound, the foreign-delete
// fail-safe, and the continuity tracker's true-age measurement.

// stubLedger builds a ledger-lookup function over static entries.
func stubLedger(entries map[string]ledgerEntry) func(string) (ledgerEntry, bool) {
	return func(id string) (ledgerEntry, bool) {
		entry, ok := entries[id]
		return entry, ok
	}
}

func remote(entries ...remoteEntry) []remoteEntry { return entries }

func entry(id string, enabled bool) remoteEntry {
	return remoteEntry{ID: id, Enabled: enabled, CompositeID: "10.0.0.1#7096#k8s#DEFAULT_GROUP@@svc"}
}

// TestObserveUnitInFlightTolerance pins the §3.4 tolerance: a missing
// expected entry whose source changed at c(e) is tolerated ONLY while
// t − c(e) ≤ OBS_BOUND; at c(e)+B it is DIVERGENT (tolerance, not
// amnesty), and no clock at all means no tolerance.
func TestObserveUnitInFlightTolerance(t *testing.T) {
	bound := 60 * time.Second
	now := time.Now()
	model := &sourceModel{
		Online:    map[string][]string{"obs-app-0": {"pod-a"}},
		Unhealthy: map[string][]string{},
		CreatedAt: map[string]time.Time{"pod-a": now},
	}
	ledger := stubLedger(nil) // the source creationTimestamp is the clock

	cases := []struct {
		name   string
		age    time.Duration // now − clock
		remote []remoteEntry
		want   verdictKind
	}{
		{"young create is tolerated (in-flight)", 30 * time.Second, nil, verdictConsistent},
		{"exactly at the bound is tolerated", 60 * time.Second, nil, verdictConsistent},
		{"one past the bound is DIVERGENT", 61 * time.Second, nil, verdictDivergent},
		{"well past the bound is DIVERGENT", 10 * time.Minute, nil, verdictDivergent},
	}
	for _, tc := range cases {
		clock := now.Add(-tc.age)
		model.CreatedAt["pod-a"] = clock
		diff := compareService("obs-app-0", model, tc.remote, ledger, bound, now)
		got := tickVerdict(diff)
		if got != tc.want {
			t.Fatalf("%s: verdict = %s, want %s (inFlight=%v, divergences=%v)",
				tc.name, got, tc.want, diff.InFlightCount, diff.Divergences)
		}
	}
	// No clock at all: no tolerance (conservative — nothing to wait for).
	model.CreatedAt = map[string]time.Time{}
	diff := compareService("obs-app-0", model, nil, ledger, bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("no-clock missing entry: verdict = %s, want DIVERGENT (no clock means no tolerance)", got)
	}
}

// TestObserveUnitForeignDeleteFailSafe pins the §3.4 fail-safe: a remote
// extra with NO source object and NO ledger entry is DIVERGENT
// immediately (a foreign DELETE leaves no deletionTimestamp — there is
// no clock to wait for); a ledger delete entry inside the bound is
// in-flight.
func TestObserveUnitForeignDeleteFailSafe(t *testing.T) {
	bound := 60 * time.Second
	now := time.Now()
	model := &sourceModel{
		Online:    map[string][]string{"obs-app-0": {}},
		Unhealthy: map[string][]string{},
		CreatedAt: map[string]time.Time{},
	}
	// No ledger entry: immediate divergence.
	diff := compareService("obs-app-0", model, remote(entry("ghost-pod", true)), stubLedger(nil), bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("foreign extra with no ledger: verdict = %s, want DIVERGENT immediately", got)
	}
	if len(diff.Divergences) != 1 || diff.Divergences[0].Kind != divExtra || diff.Divergences[0].InFlight {
		t.Fatalf("foreign extra divergence = %+v, want exactly one extra, not in-flight", diff.Divergences)
	}
	// Ledger delete entry inside the bound: in-flight.
	ledger := stubLedger(map[string]ledgerEntry{
		"churned-pod": {Op: "delete", PodName: "churned-pod", AppCode: "obs-app-0", IssuedAt: now.Add(-30 * time.Second)},
	})
	diff = compareService("obs-app-0", model, remote(entry("churned-pod", true)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictConsistent {
		t.Fatalf("churn delete inside the bound: verdict = %s, want CONSISTENT (in-flight), divergences=%v", got, diff.Divergences)
	}
	// Ledger delete entry past the bound: divergence.
	ledger = stubLedger(map[string]ledgerEntry{
		"churned-pod": {Op: "delete", PodName: "churned-pod", AppCode: "obs-app-0", IssuedAt: now.Add(-2 * time.Minute)},
	})
	diff = compareService("obs-app-0", model, remote(entry("churned-pod", true)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("churn delete past the bound: verdict = %s, want DIVERGENT (no amnesty)", got)
	}
}

// TestObserveUnitDuplicatesNeverTolerated pins plan §8.5's "no duplicate
// composite instance ids per (service, cluster) — ever": a
// within-one-view duplicate is DIVERGENT regardless of any clock.
func TestObserveUnitDuplicatesNeverTolerated(t *testing.T) {
	bound := 60 * time.Second
	now := time.Now()
	model := &sourceModel{
		Online:    map[string][]string{"obs-app-0": {"pod-a"}},
		Unhealthy: map[string][]string{},
		CreatedAt: map[string]time.Time{"pod-a": now},
	}
	// The remote serves pod-a TWICE (within the unioned view).
	diff := compareService("obs-app-0", model, remote(entry("pod-a", true), entry("pod-a", true)), stubLedger(nil), bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("within-view duplicate: verdict = %s, want DIVERGENT (never tolerable)", got)
	}
	found := false
	for _, d := range diff.Divergences {
		if d.Kind == divDup {
			found = true
		}
	}
	if !found {
		t.Fatalf("duplicate divergence not classified as dup: %+v", diff.Divergences)
	}
}

// TestObserveUnitDisabledAccounting pins the two-sided disabled checks
// (the §6.2 engine fix's second defect): a Running-not-containers-ready
// pod is expected-unhealthy — its remote enabled=false is CONSISTENT, its
// absence is a missing divergence with the pod clock, and its remote
// enabled=true is a disabled mismatch; conversely an expected-online pod
// pushed disabled is a divergence; a disabled remote entry with no source
// object is an immediate extra.
func TestObserveUnitDisabledAccounting(t *testing.T) {
	bound := 60 * time.Second
	now := time.Now()
	ledger := stubLedger(nil)

	// Expected-unhealthy pod, remote disabled: CONSISTENT.
	model := &sourceModel{
		Online:    map[string][]string{"obs-app-0": {}},
		Unhealthy: map[string][]string{"obs-app-0": {"pod-u"}},
		CreatedAt: map[string]time.Time{"pod-u": now},
	}
	diff := compareService("obs-app-0", model, remote(entry("pod-u", false)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictConsistent {
		t.Fatalf("expected-unhealthy pushed disabled: verdict = %s, want CONSISTENT, divergences=%v", got, diff.Divergences)
	}
	// Expected-unhealthy pod, remote ENABLED: disabled mismatch — young
	// clock is in-flight, an old clock is divergence.
	diff = compareService("obs-app-0", model, remote(entry("pod-u", true)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictConsistent {
		t.Fatalf("young expected-unhealthy pushed enabled: verdict = %s, want CONSISTENT (in-flight), divergences=%v", got, diff.Divergences)
	}
	model.CreatedAt["pod-u"] = now.Add(-2 * time.Minute)
	diff = compareService("obs-app-0", model, remote(entry("pod-u", true)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("old expected-unhealthy pushed enabled: verdict = %s, want DIVERGENT (the disabled mismatch is real)", got)
	}
	// Expected-online pod, remote disabled: divergence (in-flight via the
	// pod clock while young).
	model = &sourceModel{
		Online:    map[string][]string{"obs-app-0": {"pod-r"}},
		Unhealthy: map[string][]string{},
		CreatedAt: map[string]time.Time{"pod-r": now},
	}
	diff = compareService("obs-app-0", model, remote(entry("pod-r", false)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictConsistent {
		t.Fatalf("young expected-online pushed disabled: verdict = %s, want CONSISTENT (in-flight), divergences=%v", got, diff.Divergences)
	}
	model.CreatedAt["pod-r"] = now.Add(-2 * time.Minute)
	diff = compareService("obs-app-0", model, remote(entry("pod-r", false)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("old expected-online pushed disabled: verdict = %s, want DIVERGENT", got)
	}
	// A remote disabled entry with no source object: immediate extra.
	model = &sourceModel{Online: map[string][]string{"obs-app-0": {}}, Unhealthy: map[string][]string{}}
	diff = compareService("obs-app-0", model, remote(entry("zombie", false)), ledger, bound, now)
	if got := tickVerdict(diff); got != verdictDivergent {
		t.Fatalf("disabled zombie with no source pod: verdict = %s, want DIVERGENT immediately (the k8s disabled-zombie fix)", got)
	}
}

// TestObserveUnitPendingFilter pins the conversion-semantics Pending
// filter: a Pending pod is absent from the expected set entirely (a pod
// pending in k8s is absent in nacos BY DESIGN — not a divergence).
func TestObserveUnitPendingFilter(t *testing.T) {
	pods := []sourcePod{
		{Name: "pod-run", AppCode: "obs-app-0", Phase: "Running", PodIP: "10.0.0.1", ContainersReady: true, CreatedAt: time.Now()},
		{Name: "pod-pend", AppCode: "obs-app-0", Phase: "Pending", CreatedAt: time.Now()},
	}
	model := buildSourceModel(pods, []string{"obs-app-0"})
	if len(model.Online["obs-app-0"]) != 1 || model.Online["obs-app-0"][0] != "pod-run" {
		t.Fatalf("online set = %v, want [pod-run]", model.Online["obs-app-0"])
	}
	if len(model.Unhealthy["obs-app-0"]) != 0 {
		t.Fatalf("pending pod leaked into the unhealthy set: %v", model.Unhealthy["obs-app-0"])
	}
	if model.Count != 2 {
		t.Fatalf("source count = %d, want 2 (Pending pods still count toward the scale criterion)", model.Count)
	}
}

// TestObserveUnitUnionMaxCountSemantics pins the union's per-id max-count
// set semantics against a fake nacos (the §6.2 first defect):
// cross-view overlap collapses (the list and the catalog both serving an
// id counts ONCE), a within-one-view duplicate survives (duplicate
// detection keeps its teeth), and the disabled flag of a hidden id is
// visible through the catalog.
func TestObserveUnitUnionMaxCountSemantics(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("serviceName") != "obs-app-0" {
			t.Errorf("list serviceName = %s, want obs-app-0", r.URL.Query().Get("serviceName"))
		}
		fmt.Fprint(w, `{"count":1,"hosts":[`+
			`{"instanceId":"10.0.0.1#7096#k8s#DEFAULT_GROUP@@obs-app-0","clusterName":"k8s","serviceName":"obs-app-0","enabled":true,"metadata":{"instanceId":"pod-a"}}]}`)
	})
	mux.HandleFunc("/nacos/v1/ns/catalog/instances", func(w http.ResponseWriter, r *http.Request) {
		// The catalog serves the enabled id (overlap — must collapse) + a
		// disabled zombie (only the catalog sees it) + the same id twice
		// (a within-one-view duplicate — must survive).
		fmt.Fprint(w, `{"count":4,"list":[`+
			`{"instanceId":"10.0.0.1#7096#k8s#DEFAULT_GROUP@@obs-app-0","clusterName":"k8s","serviceName":"obs-app-0","enabled":true,"metadata":{"instanceId":"pod-a"}},`+
			`{"instanceId":"10.0.0.9#7096#k8s#DEFAULT_GROUP@@obs-app-0","clusterName":"k8s","serviceName":"obs-app-0","enabled":false,"metadata":{"instanceId":"zombie"}},`+
			`{"instanceId":"10.0.0.2#7096#k8s#DEFAULT_GROUP@@obs-app-0","clusterName":"k8s","serviceName":"obs-app-0","enabled":true,"metadata":{"instanceId":"pod-b"}},`+
			`{"instanceId":"10.0.0.2#7096#k8s#DEFAULT_GROUP@@obs-app-0","clusterName":"k8s","serviceName":"obs-app-0","enabled":true,"metadata":{"instanceId":"pod-b"}}]}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	view := &nacosView{addr: server.URL, http: server.Client()}
	svc, err := view.fullServiceView("obs-app-0")
	if err != nil {
		t.Fatalf("fullServiceView() error = %v", err)
	}
	entries := svc["k8s"]
	ids := map[string]int{}
	enabled := map[string]bool{}
	for _, e := range entries {
		ids[e.ID]++
		enabled[e.ID] = e.Enabled
	}
	if ids["pod-a"] != 1 {
		t.Fatalf("cross-view overlap collapsed wrongly: pod-a appears %d times, want exactly 1 (ids=%v)", ids["pod-a"], ids)
	}
	if ids["pod-b"] != 2 {
		t.Fatalf("within-one-view duplicate did not survive: pod-b appears %d times, want 2 (ids=%v)", ids["pod-b"], ids)
	}
	if ids["zombie"] != 1 {
		t.Fatalf("catalog-only disabled zombie missing from the union: ids=%v", ids)
	}
	if enabled["zombie"] {
		t.Fatalf("zombie must keep enabled=false (the flag the disabled accounting consumes)")
	}
	if !enabled["pod-a"] || !enabled["pod-b"] {
		t.Fatalf("enabled ids must keep enabled=true in the union: %+v", enabled)
	}
}

// TestObserveUnitCatalogNotFoundTolerance pins the not-found-500
// tolerance: a (service, cluster) pair with no catalog entry is the
// fully-pruned steady state (EMPTY, not an error) — and when the LIST
// also serves nothing, the view is a clean empty view.
func TestObserveUnitCatalogNotFoundTolerance(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"count":0,"hosts":[]}`)
	})
	mux.HandleFunc("/nacos/v1/ns/catalog/instances", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "caused: service obs-app-0 is not found!")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	view := &nacosView{addr: server.URL, http: server.Client()}
	svc, err := view.fullServiceView("obs-app-0")
	if err != nil {
		t.Fatalf("fullServiceView() on the pruned steady state: error = %v (the not-found 500 must be tolerated as empty)", err)
	}
	if len(svc["k8s"]) != 0 {
		t.Fatalf("pruned steady state view = %v, want empty", svc["k8s"])
	}
}

// TestObserveUnitReadFailureIsObserr pins the §6.2 third defect's rule:
// a remote read failure is an OBSERR condition, never divergence
// evidence (an emptied side must not fabricate extras). The engine-level
// pin: the view error propagates; the loop maps it to OBSERR (runTick's
// contract, exercised here through the error classification).
func TestObserveUnitReadFailureIsObserr(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "upstream 503")
	})
	mux.HandleFunc("/nacos/v1/ns/catalog/instances", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "upstream 503")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	view := &nacosView{addr: server.URL, http: server.Client()}
	_, err := view.fullServiceView("obs-app-0")
	if err == nil {
		t.Fatalf("both views failing must surface an error (a partial view must never be mistaken for a complete one)")
	}
	if isLeaderlessErr(err) {
		t.Fatalf("a plain 503 must not classify as leaderless: %v", err)
	}
	// The leaderless signature classifies.
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("the read error must carry the status: %v", err)
	}
}

// TestObserveUnitLeaderlessClassification pins the 5xx-body signatures
// the environment classifier keys on.
func TestObserveUnitLeaderlessClassification(t *testing.T) {
	for _, body := range []string{
		"Could not find leader : naming_persistent_service_v2",
		"com.alibaba.nacos.consistency.exception.ConsistencyException",
	} {
		err := classifyNacosAnswer(500, body)
		if !isLeaderlessErr(err) {
			t.Fatalf("classifyNacosAnswer(500, %q) = %v, want the leaderless marker", body, err)
		}
	}
	if isLeaderlessErr(classifyNacosAnswer(500, "Param 'ip' is required")) {
		t.Fatalf("a non-leaderless 500 must not classify as leaderless")
	}
	if isLeaderlessErr(classifyNacosAnswer(404, "Could not find leader")) {
		t.Fatalf("a 4xx must not classify as leaderless (5xx only)")
	}
}

// TestObserveUnitContinuityTracker pins the DS-4-3 fix: divergences are
// keyed and carried across ticks with firstSeen pinning the true age,
// resolution records the true heal time.
func TestObserveUnitContinuityTracker(t *testing.T) {
	tracker := newDivergenceTracker()
	t0 := time.Now()
	d := divergence{AppCode: "obs-app-0", Cluster: "k8s", ID: "pod-x", Kind: divMissing}
	tracker.observeTick(t0, []divergence{d})
	if tracker.openCount() != 1 {
		t.Fatalf("open count after first tick = %d, want 1", tracker.openCount())
	}
	t1 := t0.Add(30 * time.Second)
	tracker.observeTick(t1, []divergence{d}) // still seen
	if tracker.openCount() != 1 {
		t.Fatalf("open count after second tick = %d, want 1", tracker.openCount())
	}
	t2 := t0.Add(70 * time.Second)
	tracker.observeTick(t2, nil) // resolved
	if tracker.openCount() != 0 {
		t.Fatalf("open count after resolution = %d, want 0", tracker.openCount())
	}
	heal := tracker.maxHeal()
	if heal < 70*time.Second {
		t.Fatalf("max heal = %s, want >= 70s (firstSeen pins the true age, not the bound)", heal)
	}
	// A DIFFERENT divergence does not inherit the first's firstSeen.
	d2 := divergence{AppCode: "obs-app-0", Cluster: "k8s", ID: "pod-y", Kind: divExtra}
	tracker.observeTick(t2.Add(time.Second), []divergence{d2})
	if tracker.entries[continuityKey(d2)].FirstSeen.Before(t2) {
		t.Fatalf("a new divergence key must start a NEW firstSeen")
	}
}

// TestObserveUnitObsBoundFormula pins the single §3.4 formula:
// OBS_BOUND = max(SLO bound, push-interval P).
func TestObserveUnitObsBoundFormula(t *testing.T) {
	cfg := observeConfig{}
	got := cfg.obsBound()
	want := 60 * time.Second // max(10s SLO, 60s push interval)
	if got != want {
		t.Fatalf("obsBound() = %s, want %s (max(SLO %ds, P %ds))", got, want, SloBoundSecs, PushIntervalSecs)
	}
	if SloBoundSecs >= PushIntervalSecs {
		t.Fatalf("the pin's premise broke: SLO %ds >= P %ds — update the test", SloBoundSecs, PushIntervalSecs)
	}
}

// TestObserveUnitHistogramPercentiles pins the plain-text histogram
// parsing + percentile derivation against a realistic Fix-A exposition
// body (the p50/p95/p99 the SLO verdicts consume).
func TestObserveUnitHistogramPercentiles(t *testing.T) {
	body := `# HELP event_to_store_e2e_duration_seconds e2e
# TYPE event_to_store_e2e_duration_seconds histogram
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="0.05"} 10
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="0.1"} 50
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="0.25"} 90
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="0.5"} 99
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="1"} 100
event_to_store_e2e_duration_seconds_bucket{sink="nacos",outcome="ok",le="+Inf"} 100
event_to_store_e2e_duration_seconds_bucket{sink="atlas",outcome="ok",le="0.05"} 100
event_to_store_e2e_duration_seconds_bucket{sink="atlas",outcome="ok",le="+Inf"} 100
event_to_store_e2e_duration_seconds_count{sink="nacos",outcome="ok"} 100
sync_error_gauge{syncgauge="nacos"} 3
sync_error_gauge{syncgauge="__total__"} 3
events_dropped_total{cluster="/tmp/kubeconfig"} 0
k8s_queue_depth 7
`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	m := &metricsView{addr: server.URL, http: server.Client()}
	obs, err := m.observe()
	if err != nil {
		t.Fatalf("observe() error = %v", err)
	}
	if obs.Latency == nil || obs.Latency.Count != 100 {
		t.Fatalf("latency count = %+v, want 100 (nacos/ok only, atlas excluded)", obs.Latency)
	}
	if p := obs.Latency.Percentile(0.50); p != 0.1 {
		t.Fatalf("p50 = %v, want 0.1 (bucket 50/100)", p)
	}
	if p := obs.Latency.Percentile(0.95); p != 0.5 {
		t.Fatalf("p95 = %v, want 0.5 (95/100 exceeds the 0.25 bucket's cumulative 90)", p)
	}
	if p := obs.Latency.Percentile(0.99); p != 0.5 {
		t.Fatalf("p99 = %v, want 0.5", p)
	}
	if obs.RetryDepths["nacos"] != 3 {
		t.Fatalf("retry depth nacos = %v, want 3", obs.RetryDepths)
	}
	if obs.QueueDepth != 7 {
		t.Fatalf("robot queue depth = %d, want 7", obs.QueueDepth)
	}
	if obs.DroppedTotal != 0 {
		t.Fatalf("first scrape must baseline the counter (dropped delta 0), got %v", obs.DroppedTotal)
	}
	// Second scrape with the counter incremented: the delta shows.
	obs2, err := m.observe()
	if err != nil {
		t.Fatalf("second observe() error = %v", err)
	}
	if obs2.DroppedTotal != 0 {
		t.Fatalf("dropped delta after an unchanged counter = %v, want 0", obs2.DroppedTotal)
	}
}

// TestObserveUnitVerdictAggregation pins the verdict rule table end to
// end: zero divergence → CONSISTENT; only in-flight mismatches →
// CONSISTENT; any non-in-flight mismatch or any duplicate → DIVERGENT.
func TestObserveUnitVerdictAggregation(t *testing.T) {
	if got := tickVerdict(diffResult{}); got != verdictConsistent {
		t.Fatalf("empty diff: %s, want CONSISTENT", got)
	}
	if got := tickVerdict(diffResult{Divergences: []divergence{{Kind: divMissing, InFlight: true}}}); got != verdictConsistent {
		t.Fatalf("only in-flight: %s, want CONSISTENT", got)
	}
	if got := tickVerdict(diffResult{Divergences: []divergence{{Kind: divMissing, InFlight: true}, {Kind: divExtra, InFlight: false}}}); got != verdictDivergent {
		t.Fatalf("one untolerated mismatch: %s, want DIVERGENT", got)
	}
	if got := tickVerdict(diffResult{Divergences: []divergence{{Kind: divDup, InFlight: true}}}); got != verdictDivergent {
		t.Fatalf("in-flight duplicate: %s, want DIVERGENT (duplicates are never tolerable)", got)
	}
}

// TestObserveUnitBurstSchedule pins the §4.2 OBS_BURSTS marks: the
// 100-in-1s storm early (5%) + the 200-instance batches at 25%/50%/75%.
func TestObserveUnitBurstSchedule(t *testing.T) {
	schedule := burstSchedule(30 * time.Minute)
	if len(schedule) != 4 {
		t.Fatalf("burstSchedule() = %d events, want 4 (storm + 3 batches)", len(schedule))
	}
	if schedule[0].id != "storm-100" || schedule[0].n != 100 {
		t.Fatalf("first burst = %s n=%d, want storm-100 n=100", schedule[0].id, schedule[0].n)
	}
	for i, wantFrac := range []float64{0.25, 0.50, 0.75} {
		got := schedule[i+1].at
		want := time.Duration(wantFrac * float64(30*time.Minute))
		if got != want {
			t.Fatalf("batch %d at %s, want %s (fraction %.2f of 30m)", i+1, got, want, wantFrac)
		}
		if schedule[i+1].n != 200 {
			t.Fatalf("batch %d n=%d, want 200", i+1, schedule[i+1].n)
		}
	}
}

// TestObserveUnitOwnPodFilter pins the foreign-pod guard: the churn victim
// picker only selects the harness's OWN pods (observed app-code + pod-name
// prefix) — the run-2 population drift was foreign dsca-scale-app pods
// being deleted, so the filter is load-bearing.
func TestObserveUnitOwnPodFilter(t *testing.T) {
	r := &observeRun{
		appCodes: []string{"obs-app-0", "obs-app-1"},
		driver:   newChurnDriver("unused", []string{"obs-app-0", "obs-app-1"}, observePodPrefix),
	}
	cases := []struct {
		pod  sourcePod
		want bool
	}{
		{sourcePod{Name: "obs-pod-42", AppCode: "obs-app-0", Phase: "Running"}, true},
		{sourcePod{Name: "dsca-scale-pod-3000", AppCode: "dsca-scale-app", Phase: "Running"}, false},
		{sourcePod{Name: "obs-pod-99", AppCode: "dsca-scale-app", Phase: "Running"}, false},
		{sourcePod{Name: "dsca-scale-pod-3001", AppCode: "obs-app-0", Phase: "Running"}, false},
	}
	for _, tc := range cases {
		if got := r.isOwnPod(tc.pod); got != tc.want {
			t.Fatalf("isOwnPod(%+v) = %v, want %v", tc.pod, got, tc.want)
		}
	}
}

// TestObserveUnitBurstConvergenceAccounting pins the burst folding: a
// burst pod absent from a tick's divergence list converges that leg; one
// present blocks convergence until it clears.
func TestObserveUnitBurstConvergenceAccounting(t *testing.T) {
	r := &observeRun{
		cfg:      observeConfig{Tick: 10 * time.Second},
		log:      mustHarnessLog(t),
		appCodes: []string{"obs-app-0"},
	}
	create := time.Now().Add(-15 * time.Second)
	delete := time.Now().Add(-15 * time.Second)
	r.burstEvents = []burstEvent{{
		Name: "storm-100", Size: 2, Pods: []string{"obs-pod-1", "obs-pod-2"},
		CreateIssued: create, DeleteIssued: delete,
	}}
	tickTS := time.Now()
	// Both pods clean: both legs converge.
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictConsistent), TS: formatTime(tickTS), Divergence: nil,
	})
	ev := r.burstEvents[0]
	if ev.ConvergedUp.IsZero() || ev.ConvergedDown.IsZero() {
		t.Fatalf("clean tick must converge both legs: %+v", ev)
	}
	// A divergent pod blocks convergence on a fresh burst.
	r.burstEvents = []burstEvent{{
		Name: "batch-200@25%", Size: 1, Pods: []string{"obs-pod-3"},
		CreateIssued: time.Now().Add(-20 * time.Second),
	}}
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictDivergent), TS: formatTime(time.Now()),
		Divergence: []divergence{{ID: "obs-pod-3", Kind: divMissing}},
	})
	if !r.burstEvents[0].ConvergedUp.IsZero() {
		t.Fatalf("a divergent burst pod must block up-convergence")
	}
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictConsistent), TS: formatTime(time.Now()), Divergence: nil,
	})
	if r.burstEvents[0].ConvergedUp.IsZero() {
		t.Fatalf("the clean tick after the divergence must converge the leg")
	}
}

// mustHarnessLog builds a scratch harness log for unit tests.
func mustHarnessLog(t *testing.T) *harnessLog {
	t.Helper()
	log, err := newHarnessLog(filepath.Join(t.TempDir(), "observe-unit.log"))
	if err != nil {
		t.Fatalf("harness log: %v", err)
	}
	t.Cleanup(func() { _ = log.close() })
	return log
}

// arithmetic: 30 base at 5%/min with a 20s cadence is 0.5 pod per
// cadence — two cadences must accumulate to one churn (the fraction
// accumulator never rounds the rate to zero).
func TestObserveUnitChurnFractionAccumulation(t *testing.T) {
	r := &observeRun{
		cfg: observeConfig{
			BaseInstances:      30,
			ChurnRatePctPerMin: 5,
			ChurnEvery:         20 * time.Second,
		},
	}
	exact := r.churnPerTickExact()
	if exact <= 0 || exact >= 1 {
		t.Fatalf("churnPerTickExact() = %v, want a fraction in (0,1) for the 30-base, 5%%/min, 20s-cadence shape", exact)
	}
	// 2 cadences accumulate to >= 1 pod.
	acc := 2 * exact
	if acc < 1 {
		t.Fatalf("2 cadences accumulate %v, want >= 1 pod (the 5%%/min rate must not round to zero)", acc)
	}
	// The 1000 default is integral per cadence: 1000 * 5% / min * (20s/60s) = 16.67.
	def := &observeRun{cfg: observeConfig{BaseInstances: 1000, ChurnRatePctPerMin: 5, ChurnEvery: 20 * time.Second}}
	if got := def.churnPerTickExact(); got < 16 || got > 17 {
		t.Fatalf("default churn per cadence = %v, want ~16.67 (50/min at 20s cadence)", got)
	}
}

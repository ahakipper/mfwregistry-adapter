//go:build soak
// +build soak

package soak

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/discoverymock"
)

// Unit-tier proofs of the batch-4 observation additions (the full soak run
// is NOT required — these pin the assertion logic itself against a fake
// child / fake nacos):
//
//   - D-5: the metrics text parsing, and the drain bound tripping on a
//     stale non-zero depth after churn quiescence (mutation (c)'s standing
//     guard — the full soak would catch F7 live, this proves the bound's
//     logic fires).
//   - D-4: fullView unioning the instance-list view with the catalog view
//     (a disabled zombie in the catalog becomes a divergence).
//   - D-11: the Atlas payload parity comparison against the model.

// TestSoakUnitParseSyncErrorGauge pins the plain-text parsing of the
// Prometheus exposition: per-sink labeled lines, the __total__ label of
// batch 3, the label-less degenerate form, comments/help lines and garbage
// samples are all handled.
func TestSoakUnitParseSyncErrorGauge(t *testing.T) {
	body := strings.Join([]string{
		"# HELP sync_error_gauge some help",
		"# TYPE sync_error_gauge gauge",
		`sync_error_gauge{syncgauge="atlas"} 0`,
		`sync_error_gauge{syncgauge="nacos"} 130`,
		`sync_error_gauge{syncgauge="__total__"} 130`,
		`sync_error_gauge 7`,
		`sync_once_gauge{syncgauge="sync_once_gauge"} 1`,
		`sync_all_durations_histogram_bucket{provider="all",le="0"} 0`,
		`sync_error_gauge{syncgauge="bad"} not-a-number`,
		`sync_error_gauge{unrelated="label"} 1`,
	}, "\n")

	depths := parseSyncErrorGauge(body)
	want := map[string]int{"atlas": 0, "nacos": 130, "__total__": 130, "": 7}
	for sink, depth := range want {
		if got := depths[sink]; got != depth {
			t.Errorf("parseSyncErrorGauge(%q)[%q] = %d, want %d", body, sink, got, depth)
		}
	}
	if len(depths) != len(want) {
		t.Errorf("parseSyncErrorGauge parsed %d series (%v), want exactly %d", len(depths), depths, len(want))
	}
}

// TestSoakUnitQueueDepthsScrape proves the observer's scrape path against a
// stand-in metrics endpoint: 200 + a gauge body parses; a non-200 answer is
// an error (never a fake zero-depth map — a metrics outage must not look
// like a drained queue).
func TestSoakUnitQueueDepthsScrape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`sync_error_gauge{syncgauge="nacos"} 4` + "\n"))
	}))
	defer server.Close()

	observer := &metricsObserver{addr: server.URL, http: server.Client()}
	depths, err := observer.queueDepths()
	if err != nil {
		t.Fatalf("queueDepths() error = %v", err)
	}
	if got := depths["nacos"]; got != 4 {
		t.Fatalf("depths[nacos] = %d, want 4", got)
	}

	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer broken.Close()
	brokenObserver := &metricsObserver{addr: broken.URL, http: broken.Client()}
	if _, err := brokenObserver.queueDepths(); err == nil {
		t.Fatal("queueDepths() on a 500 metrics endpoint = nil error, want an error (an outage is not a drained queue)")
	}
}

// newUnitTestHarness builds a harness with the standing-assertion helpers
// and a scratch soak log (the drain bound writes its trip marker there).
func newUnitTestHarness(t *testing.T) *soakHarness {
	t.Helper()
	log, err := newSoakLog(t.TempDir() + "/soak-unit.log")
	if err != nil {
		t.Fatalf("newSoakLog() error = %v", err)
	}
	return &soakHarness{
		schedule: windowSchedule{FullPushBound: 120 * time.Second},
		log:      log,
	}
}

// TestSoakUnitDrainBoundTripsOnStaleNonZeroDepth is mutation (c)'s standing
// guard: a queue that still holds entries after churn quiescence (last
// churn + one full-push bound) must trip the drain violation — F7's exact
// signature (the live incident held 130 pending entries while the soak
// printed nothing). The trip requires the depth to persist across TWO
// consecutive observations: the worker publishes the gauge at the START of
// each 5s retry tick, so a single nonzero observation can be a stale
// publication from before a mid-tick heal (see
// TestSoakUnitDrainBoundToleratesPublicationLag for that half).
func TestSoakUnitDrainBoundTripsOnStaleNonZeroDepth(t *testing.T) {
	h := newUnitTestHarness(t)
	defer func() { _ = h.log.close() }()

	// No churn yet: nothing can have queued, the check stays silent.
	h.checkQueueDrain(map[string]int{"nacos": 5}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain bound tripped before any churn: %v", h.drainBreach)
	}

	// Fresh churn, entries still held: inside the legitimate heal window —
	// still silent.
	h.lastSourceChurn = time.Now()
	h.checkQueueDrain(map[string]int{"nacos": 5}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain bound tripped inside the heal window: %v", h.drainBreach)
	}

	// Quiesced, FIRST nonzero observation: held as pending — one
	// observation is not a breach (publication lag must be tolerated).
	h.lastSourceChurn = time.Now().Add(-2 * h.schedule.FullPushBound)
	h.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 5, "__total__": 5}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain bound tripped on a single nonzero observation (a stale gauge from before a mid-tick heal must be tolerated): %v", h.drainBreach)
	}

	// Quiesced and STILL holding on the next observation: the bound trips,
	// the violation names the sink and its depth.
	h.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 5, "__total__": 5}, false)
	if h.drainBreach == nil {
		t.Fatal("drain bound did not trip on a depth persisting across two observations after quiescence (the F7 signature would pass silently)")
	}
	message := h.drainBreach.String()
	if !strings.Contains(message, "nacos") || !strings.Contains(message, "nacos=5") {
		t.Fatalf("drain violation %q does not name the held sink and depth", message)
	}

	// The final pass's strict mode: the same persistence rule — one strict
	// nonzero observation is pending, the pair trips.
	h2 := newUnitTestHarness(t)
	defer func() { _ = h2.log.close() }()
	h2.lastSourceChurn = time.Now()
	h2.checkQueueDrain(map[string]int{"nacos": 1}, true)
	if h2.drainBreach != nil {
		t.Fatalf("strict drain check tripped on a single observation (publication lag must be tolerated in strict mode too): %v", h2.drainBreach)
	}
	h2.checkQueueDrain(map[string]int{"nacos": 1}, true)
	if h2.drainBreach == nil {
		t.Fatal("strict drain check did not trip on a persistently held queue")
	}
}

// TestSoakUnitDrainBoundToleratesPublicationLag pins the persistence rule's
// other half: the worker publishes the queue depths at the START of each 5s
// retry tick (before that tick's pushes), so an entry that heals mid-tick
// leaves the published gauge stale-nonzero for up to one tick. A transient
// nonzero that clears on the next observation must NOT trip the breach — and
// a recorded breach clears once the queue actually drains (F7's signature is
// a queue that never drains, not one that heals one publication late).
func TestSoakUnitDrainBoundToleratesPublicationLag(t *testing.T) {
	h := newUnitTestHarness(t)
	defer func() { _ = h.log.close() }()
	h.lastSourceChurn = time.Now().Add(-2 * h.schedule.FullPushBound)

	// The stale-lag transient: nonzero once (the pre-heal publication),
	// all-zero on the next observation — no breach.
	h.checkQueueDrain(map[string]int{"nacos": 3, "__total__": 3}, false)
	h.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 0, "__total__": 0}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain bound tripped on a stale-lag transient that cleared on the second publication: %v", h.drainBreach)
	}

	// A depth that persists across two observations trips; a LATER all-zero
	// observation clears it (the queue eventually drained).
	h.checkQueueDrain(map[string]int{"nacos": 2, "__total__": 2}, false)
	h.checkQueueDrain(map[string]int{"nacos": 2, "__total__": 2}, false)
	if h.drainBreach == nil {
		t.Fatal("drain bound did not trip on a persisting depth")
	}
	h.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 0, "__total__": 0}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain breach did not clear after the queue drained: %v", h.drainBreach)
	}

	// The strict mode's shape: the final gate scrapes, finds nonzero, waits
	// one retry tick and re-scrapes (the two calls below are those two
	// scrapes) — a healed second publication means no breach.
	h2 := newUnitTestHarness(t)
	defer func() { _ = h2.log.close() }()
	h2.lastSourceChurn = time.Now()
	h2.checkQueueDrain(map[string]int{"nacos": 1, "__total__": 1}, true)
	h2.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 0, "__total__": 0}, true)
	if h2.drainBreach != nil {
		t.Fatalf("strict drain check tripped on a publication-lag transient: %v", h2.drainBreach)
	}
}

// TestSoakUnitDrainBoundPassesOnDrainedQueue: the healthy shape — quiesced
// and every depth zero — never trips.
func TestSoakUnitDrainBoundPassesOnDrainedQueue(t *testing.T) {
	h := newUnitTestHarness(t)
	defer func() { _ = h.log.close() }()
	h.lastSourceChurn = time.Now().Add(-2 * h.schedule.FullPushBound)
	h.checkQueueDrain(map[string]int{"atlas": 0, "nacos": 0, "__total__": 0}, false)
	if h.drainBreach != nil {
		t.Fatalf("drain bound tripped on a fully drained queue: %v", h.drainBreach)
	}
}

// TestSoakUnitCatalogUnionSurfacesDisabledZombie (D-4): the fullView unions
// the instance-list view (which hides enabled=false instances — the exact
// state spotter's own unhealthy pushes write) with the catalog view; a
// zombie the list hides must appear in the union, so the standing
// comparison flags it instead of printing "PASS" while the catalog holds
// drift.
func TestSoakUnitCatalogUnionSurfacesDisabledZombie(t *testing.T) {
	// The fake nacos: instance/list serves the enabled instance only; the
	// catalog serves BOTH (the F8 fidelity). The zombie carries no
	// metadata instanceId, so its composite id is its domain id — the
	// comparison must see it as an extra id under the ecs cluster.
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"count":1,"hosts":[{"instanceId":"127.0.0.1#20001#ecs#DEFAULT_GROUP@@soak-app","clusterName":"ecs","serviceName":"soak-app","metadata":{"instanceId":"soak-app-inst-1"}}]}`)
	})
	mux.HandleFunc("/nacos/v1/ns/catalog/instances", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("clusterName") != "ecs" {
			// The k8s-cluster catalog read: the fully-pruned steady state.
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "caused: service soak-app is not found!")
			return
		}
		fmt.Fprint(w, `{"count":2,"list":[{"instanceId":"127.0.0.1#20001#ecs#DEFAULT_GROUP@@soak-app","clusterName":"ecs","serviceName":"soak-app","metadata":{"instanceId":"soak-app-inst-1"}},{"instanceId":"127.0.0.9#9999#ecs#DEFAULT_GROUP@@soak-app","clusterName":"ecs","enabled":false,"serviceName":"soak-app"}]}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	observer := &nacosObserver{addr: server.URL, http: server.Client()}
	view, err := observer.fullView([]string{"soak-app"})
	if err != nil {
		t.Fatalf("fullView() error = %v (the 500 not-found catalog answer must be tolerated as empty)", err)
	}

	ecs := view["soak-app"]["ecs"]
	if len(ecs) != 2 {
		t.Fatalf("union view ecs cluster = %v, want 2 ids (the live id — served by BOTH views, counted once — + the catalog's disabled zombie)", ecs)
	}
	joined := strings.Join(ecs, ",")
	if got := strings.Count(joined, "soak-app-inst-1"); got != 1 {
		t.Fatalf("live instance id appears %d times in the union, want exactly 1 (the catalog is a superset of the list view; plain concatenation would double-count every healthy instance): %v", got, ecs)
	}
	if !strings.Contains(joined, "127.0.0.9#9999#ecs#DEFAULT_GROUP@@soak-app") {
		t.Fatalf("union view missing the disabled zombie (D-4: the catalog view must surface it): %v", ecs)
	}

	// The comparison flags the zombie: the model wants only inst-1.
	expected := newExpectedState()
	expected.setConsul("soak-app", []string{"soak-app-inst-1"})
	_, divergences, err := observer.checkStateScoped(expected, []string{"soak-app"})
	if err != nil {
		t.Fatalf("checkStateScoped() error = %v", err)
	}
	if len(divergences) == 0 {
		t.Fatal("the disabled zombie produced no divergence (D-4 failed: the zombie would silently pass)")
	}
	found := false
	for _, d := range divergences {
		if d.cluster == "ecs" && containsString(d.extra, "127.0.0.9#9999#ecs#DEFAULT_GROUP@@soak-app") {
			found = true
		}
	}
	if !found {
		t.Fatalf("the zombie is not reported as an ecs extra; divergences = %v", divergences)
	}
}

// TestSoakUnitUnionClusterViewSetSemantics pins the union's per-id SET
// semantics directly: the catalog view is a superset of the list view, so
// an id both views serve counts ONCE (plain concatenation would
// double-count every healthy instance and turn a converged stack into an
// extra+duplicate divergence on every assert cycle), while a genuine
// within-one-view duplicate survives (the comparison's duplicate
// detection must keep its teeth).
func TestSoakUnitUnionClusterViewSetSemantics(t *testing.T) {
	cases := []struct {
		name  string
		base  map[string][]string
		extra map[string][]string
		want  map[string][]string
	}{
		{
			name:  "cross-view overlap collapses",
			base:  map[string][]string{"ecs": {"a", "b"}},
			extra: map[string][]string{"ecs": {"a", "c"}},
			want:  map[string][]string{"ecs": {"a", "b", "c"}},
		},
		{
			name:  "within-one-view duplicate on the list side survives",
			base:  map[string][]string{"ecs": {"a", "a"}},
			extra: map[string][]string{"ecs": {"a"}},
			want:  map[string][]string{"ecs": {"a", "a"}},
		},
		{
			name:  "within-one-view duplicate on the catalog side survives",
			base:  map[string][]string{"ecs": {"a"}},
			extra: map[string][]string{"ecs": {"a", "a", "b"}},
			want:  map[string][]string{"ecs": {"a", "a", "b"}},
		},
		{
			name:  "distinct clusters stay distinct",
			base:  map[string][]string{"k8s": {"p1"}},
			extra: map[string][]string{"ecs": {"c1"}},
			want:  map[string][]string{"k8s": {"p1"}, "ecs": {"c1"}},
		},
	}
	for _, tc := range cases {
		got := unionClusterView(tc.base, tc.extra)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: union = %v, want %v", tc.name, got, tc.want)
		}
		for cluster, wantIDs := range tc.want {
			gotIDs, ok := got[cluster]
			if !ok {
				t.Fatalf("%s: union missing cluster %q: %v", tc.name, cluster, got)
			}
			if strings.Join(gotIDs, ",") != strings.Join(wantIDs, ",") {
				t.Fatalf("%s: union[%q] = %v, want %v", tc.name, cluster, gotIDs, wantIDs)
			}
		}
	}
}

// TestSoakUnitAtlasParityDetectsPayloadDivergence (D-11): a stand-in whose
// latest payloads disagree with the model is a divergence — the id set and
// the per-cluster placement are both compared.
func TestSoakUnitAtlasParityDetectsPayloadDivergence(t *testing.T) {
	expected := newExpectedState()
	expected.setK8s("spike-app", []string{"pod-1", "pod-2"})
	expected.setConsul("soak-stable-1", []string{"soak-stable-1-inst-1"})

	latest := map[string]map[string]*atlasInstanceRecord{
		"spike-app": {
			"pod-1": {AppCode: "spike-app", InstanceId: "pod-1", Provider: "k8s", Status: 1},
			"pod-2": {AppCode: "spike-app", InstanceId: "pod-2", Provider: "k8s", Status: 3}, // deregistered: absent
			"pod-3": {AppCode: "spike-app", InstanceId: "pod-3", Provider: "k8s", Status: 1}, // never modeled
		},
		"soak-stable-1": {
			"soak-stable-1-inst-1": {AppCode: "soak-stable-1", InstanceId: "soak-stable-1-inst-1", Provider: "ecs", Status: 1},
		},
	}

	divergences := atlasCheck(latest, expected, nil)
	if len(divergences) != 1 {
		t.Fatalf("atlasCheck divergences = %v, want exactly one (spike-app: pod-2 missing, pod-3 extra)", divergences)
	}
	d := divergences[0]
	if d.appCode != "spike-app" || d.cluster != "k8s" {
		t.Fatalf("divergence = %s, want spike-app/k8s", d.String())
	}
	if !containsString(d.missing, "pod-2") || !containsString(d.extra, "pod-3") {
		t.Fatalf("divergence = %s, want missing pod-2 and extra pod-3", d.String())
	}

	// The matching shape: zero divergence.
	fixed := map[string]map[string]*atlasInstanceRecord{
		"spike-app": {
			"pod-1": {AppCode: "spike-app", InstanceId: "pod-1", Provider: "k8s", Status: 1},
			"pod-2": {AppCode: "spike-app", InstanceId: "pod-2", Provider: "k8s", Status: 1},
		},
		"soak-stable-1": {
			"soak-stable-1-inst-1": {AppCode: "soak-stable-1", InstanceId: "soak-stable-1-inst-1", Provider: "ecs", Status: 1},
		},
	}
	if diffs := atlasCheck(fixed, expected, nil); len(diffs) != 0 {
		t.Fatalf("atlasCheck on the matching shape = %v, want zero divergence", diffs)
	}

	// The latest-record-wins reduction: an instance pushed online then
	// offline ends absent, a stale online record superseded by a newer
	// offline one must not survive the view.
	calls := []discoverymock.Call{
		{Method: "SynInstance", Instances: instanceRecords(
			&record{appCode: "spike-app", id: "pod-1", provider: "k8s", status: 1})},
		{Method: "SynInstance", Instances: instanceRecords(
			&record{appCode: "spike-app", id: "pod-1", provider: "k8s", status: 3})},
	}
	view := atlasView(atlasSnapshots(calls))
	if len(view["spike-app"]) != 1 {
		t.Fatalf("atlasView kept %d records, want 1 (the offline record replaced the online one)", len(view["spike-app"]))
	}
}

// record is a small builder helper for the discoverymock.Call payloads.
type record struct {
	appCode  string
	id       string
	provider string
	status   int32
}

// instanceRecords builds the domain-shaped instance slice one Call carries.
func instanceRecords(records ...*record) []*instance.Instance {
	out := make([]*instance.Instance, 0, len(records))
	for _, r := range records {
		out = append(out, &instance.Instance{
			AppCode:    r.appCode,
			InstanceId: r.id,
			Provider:   r.provider,
			Status:     r.status,
		})
	}
	return out
}

// containsString reports whether the slice holds the id (test-local).
func containsString(ids []string, wanted string) bool {
	for _, id := range ids {
		if id == wanted {
			return true
		}
	}
	return false
}

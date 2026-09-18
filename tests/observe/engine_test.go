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

	"spotter/internal/domain/instance"
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

func TestObserveUnitSpotterProjectionRequiresCompleteCanonicalPayload(t *testing.T) {
	ins := &instance.Instance{SourceKey: "default/pod-a", SourceCluster: "cluster-a", InstanceId: "pod-a", AppCode: "obs-app-0", Provider: "k8s", EnvType: "test", Ip: "10.0.0.7", Reversion: 42, Status: 1, Enabled: true, State: "running", Label: map[string]string{"team": "payments"}}
	pod := sourcePod{Name: "pod-a", AppCode: "obs-app-0", Phase: "Running", PodIP: "10.0.0.7", ContainersReady: true, Instance: ins}
	canonical := []string{instance.CanonicalPayload(ins)}
	if !spotterProjectionMatches([]sourcePod{pod}, []string{"obs-app-0"}, []*instance.Instance{ins}, canonical) {
		t.Fatal("matching Spotter projection rejected")
	}
	mutated := *ins
	mutated.Reversion = 43
	if spotterProjectionMatches([]sourcePod{pod}, []string{"obs-app-0"}, []*instance.Instance{&mutated}, []string{instance.CanonicalPayload(&mutated)}) {
		t.Fatal("Reversion drift was accepted by Spotter projection matcher")
	}
	if spotterProjectionMatches([]sourcePod{pod, pod}, []string{"obs-app-0"}, []*instance.Instance{ins}, canonical) {
		t.Fatal("duplicate source identity was accepted by Spotter projection matcher")
	}
}

func remote(entries ...remoteEntry) []remoteEntry { return entries }

func entry(id string, enabled bool) remoteEntry {
	return remoteEntry{ID: id, Enabled: enabled, CompositeID: "10.0.0.1#7096#k8s#DEFAULT_GROUP@@svc"}
}

// TestObserveUnitExactEntryFieldsAreCompared pins the data-plane equality
// contract: a matching domain id is insufficient when Nacos has the wrong
// endpoint, scope, lifecycle mode, enabled state, or Spotter ownership.
func TestObserveUnitExactEntryFieldsAreCompared(t *testing.T) {
	now := time.Now()
	model := buildSourceModel([]sourcePod{{
		Name: "pod-a", AppCode: "obs-app-0", Phase: "Running", PodIP: "10.0.0.7",
		ContainersReady: true, CreatedAt: now,
	}}, []string{"obs-app-0"})
	expected := model.Entries["obs-app-0"]["pod-a"]
	remoteFor := func(mutate func(*remoteEntry)) []remoteEntry {
		got := remoteEntry{
			ID: "pod-a", IP: expected.IP, Port: expected.Port,
			ClusterName: expected.ClusterName, ServiceName: expected.ServiceName,
			Healthy: expected.Healthy, Enabled: expected.Enabled, Ephemeral: expected.Ephemeral,
			Metadata:    copyStringMap(expected.Metadata),
			CompositeID: "10.0.0.7#7096#k8s#DEFAULT_GROUP@@obs-app-0",
		}
		mutate(&got)
		return []remoteEntry{got}
	}
	if diff := compareService("obs-app-0", model, remoteFor(func(*remoteEntry) {}), stubLedger(nil), time.Minute, now); len(diff.Divergences) != 0 {
		t.Fatalf("exact projected entry diff = %+v, want no divergence", diff.Divergences)
	}
	checks := []struct {
		name   string
		mutate func(*remoteEntry)
	}{
		{"ip mismatch", func(e *remoteEntry) { e.IP = "10.0.0.8" }},
		{"port mismatch", func(e *remoteEntry) { e.Port = 8080 }},
		{"cluster mismatch", func(e *remoteEntry) { e.ClusterName = "ecs" }},
		{"service mismatch", func(e *remoteEntry) { e.ServiceName = "other-service" }},
		{"ephemeral mismatch", func(e *remoteEntry) { e.Ephemeral = true }},
		{"enabled mismatch", func(e *remoteEntry) { e.Enabled = false }},
		{"status metadata mismatch", func(e *remoteEntry) { e.Metadata["status"] = "2" }},
		{"ownership metadata mismatch", func(e *remoteEntry) { e.Metadata["spotterOwner"] = "other-writer" }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			diff := compareService("obs-app-0", model, remoteFor(check.mutate), stubLedger(nil), time.Minute, now)
			if len(diff.Divergences) != 1 || diff.Divergences[0].Kind != divField {
				t.Fatalf("field mismatch diff = %+v, want one %q divergence", diff.Divergences, divField)
			}
		})
	}
}

func TestObserveUnitNacosOwnedHealthDriftDoesNotBreakSpotterEquality(t *testing.T) {
	now := time.Now()
	model := buildSourceModel([]sourcePod{{
		Name: "pod-a", AppCode: "obs-app-0", Phase: "Running", PodIP: "10.0.0.7",
		ContainersReady: true, CreatedAt: now,
	}}, []string{"obs-app-0"})
	expected := model.Entries["obs-app-0"]["pod-a"]
	got := remoteEntry{
		ID: expected.ID, IP: expected.IP, Port: expected.Port,
		ClusterName: expected.ClusterName, ServiceName: expected.ServiceName,
		Healthy: false, Enabled: expected.Enabled, Ephemeral: expected.Ephemeral,
		Metadata: copyStringMap(expected.Metadata), CompositeID: expected.CompositeID,
	}
	if diff := compareService("obs-app-0", model, []remoteEntry{got}, stubLedger(nil), time.Minute, now); len(diff.Divergences) != 0 {
		t.Fatalf("Nacos-owned healthy drift = %+v, want no Spotter data divergence", diff.Divergences)
	}
}

func TestObserveUnitUnhealthySDKVisibilityPreservesCanonicalEnabled(t *testing.T) {
	ins := &instance.Instance{SourceKey: "default/pod-a", SourceCluster: "cluster-a", InstanceId: "pod-a", AppCode: "obs-app-0", Provider: "k8s", EnvType: "test", Ip: "10.0.0.7", Reversion: 42, Status: 2, Enabled: false, State: "crash"}
	pod := sourcePod{Name: "pod-a", AppCode: "obs-app-0", Phase: "Running", PodIP: ins.Ip, ContainersReady: false, Instance: ins}
	model := buildSourceModel([]sourcePod{pod}, []string{"obs-app-0"})
	expected := model.Entries["obs-app-0"]["pod-a"]
	remote := remoteEntry{ID: "pod-a", IP: expected.IP, Port: expected.Port, ClusterName: expected.ClusterName, ServiceName: expected.ServiceName, CompositeID: expected.CompositeID, Enabled: true, Healthy: false, Ephemeral: false, Metadata: copyStringMap(expected.Metadata)}
	if diff := compareService("obs-app-0", model, []remoteEntry{remote}, stubLedger(nil), time.Minute, time.Now()); len(diff.Divergences) != 0 {
		t.Fatalf("query-visible unhealthy wire shape diverged: %+v", diff.Divergences)
	}
	remote.Metadata["status"] = "1"
	if diff := compareService("obs-app-0", model, []remoteEntry{remote}, stubLedger(nil), time.Minute, time.Now()); len(diff.Divergences) == 0 {
		t.Fatal("status/canonical drift was accepted by unhealthy wire exception")
	}
}

func TestObserveUnitFullPayloadDetectsReversionAndLabelDrift(t *testing.T) {
	now := time.Now()
	source := &instance.Instance{
		InstanceId: "pod-a", AppCode: "obs-app-0", Provider: "k8s", Ip: "10.0.0.7",
		Ports: []*instance.PortInfo{{Port: 7096}}, Enabled: true, State: "running",
		EnvType: "test", EnvGroup: "blue", Label: map[string]string{"app": "pay-user", "custom": "kept"},
		Image: map[string]string{"application": "repo/app:v7"}, Reversion: 42, Status: 1,
	}
	model := buildSourceModel([]sourcePod{{Name: "pod-a", AppCode: "obs-app-0", Phase: "Running", PodIP: source.Ip, ContainersReady: true, CreatedAt: now, Instance: source}}, []string{"obs-app-0"})
	expected := model.Entries["obs-app-0"]["pod-a"]
	remoteFor := func(payload string) []remoteEntry {
		metadata := copyStringMap(expected.Metadata)
		metadata["spotter.instance"] = payload
		return []remoteEntry{{ID: expected.ID, IP: expected.IP, Port: expected.Port, ClusterName: expected.ClusterName, ServiceName: expected.ServiceName, Enabled: expected.Enabled, Ephemeral: false, Metadata: metadata, CompositeID: expected.CompositeID}}
	}
	if diff := compareService("obs-app-0", model, remoteFor(expected.Metadata["spotter.instance"]), stubLedger(nil), time.Minute, now); len(diff.Divergences) != 0 {
		t.Fatalf("matching full payload diff = %+v, want none", diff.Divergences)
	}
	mutated, err := instance.DecodeCompressedCanonicalPayload(expected.Metadata["spotter.instance"])
	if err != nil {
		t.Fatalf("DecodeCanonicalPayload() error = %v", err)
	}
	mutated.Reversion++
	if diff := compareService("obs-app-0", model, remoteFor(instance.CompressedCanonicalPayload(mutated)), stubLedger(nil), time.Minute, now); len(diff.Divergences) != 1 || diff.Divergences[0].Kind != divField {
		t.Fatalf("reversion drift diff = %+v, want one field divergence", diff.Divergences)
	}
	mutated.Reversion = source.Reversion
	mutated.Label["custom"] = "changed"
	if diff := compareService("obs-app-0", model, remoteFor(instance.CompressedCanonicalPayload(mutated)), stubLedger(nil), time.Minute, now); len(diff.Divergences) != 1 || diff.Divergences[0].Kind != divField {
		t.Fatalf("label drift diff = %+v, want one field divergence", diff.Divergences)
	}
}

func TestObserveUnitRemoteFingerprintIncludesMetadataAndWireFields(t *testing.T) {
	base := map[string][]remoteEntry{"obs-app-0": {{ID: "pod-a", IP: "10.0.0.7", Port: 7096, ClusterName: "k8s", ServiceName: "obs-app-0", Healthy: true, Enabled: true, Ephemeral: false, Metadata: map[string]string{"spotter.instance": "payload-a"}}}}
	first := remoteViewFingerprint(base)
	base["obs-app-0"][0].Metadata["spotter.instance"] = "payload-b"
	if second := remoteViewFingerprint(base); first == second {
		t.Fatal("remote fingerprint did not change when canonical metadata changed")
	}
	base["obs-app-0"][0].Metadata["spotter.instance"] = "payload-a"
	base["obs-app-0"][0].Healthy = false
	if third := remoteViewFingerprint(base); first == third {
		t.Fatal("remote fingerprint did not retain Nacos-owned healthy observation")
	}
}

func TestObserveUnitSnapshotRetryRequiresBoundedMutationEvidence(t *testing.T) {
	if !snapshotRetryable(tickRecord{SnapshotUnstable: true}) {
		t.Fatal("an unstable three-plane cut must request a fresh snapshot")
	}
	if !snapshotRetryable(tickRecord{Verdict: string(verdictConsistent), ExactEqual: false, MutationObserved: true, InFlight: 1, Divergence: []divergence{{Kind: divMissing, InFlight: true}}}) {
		t.Fatal("mutation-backed in-flight mismatch should request a fresh snapshot")
	}
	if snapshotRetryable(tickRecord{Verdict: string(verdictConsistent), ExactEqual: false, MutationObserved: false, InFlight: 1, Divergence: []divergence{{Kind: divMissing, InFlight: true}}}) {
		t.Fatal("unattributed mismatch must not be retried as a harmless snapshot race")
	}
	if snapshotRetryable(tickRecord{Verdict: string(verdictObsErr), ExactEqual: false, MutationObserved: true, InFlight: 1, Divergence: []divergence{{Kind: divMissing, InFlight: true}}}) {
		t.Fatal("observation errors must not be hidden by snapshot retries")
	}
}

func TestObserveUnitMutationJournalRetainsEveryOperation(t *testing.T) {
	driver := newChurnDriver("unused", []string{"obs-app-0"}, observePodPrefix)
	start := time.Now()
	driver.mu.Lock()
	driver.mutationJournal = append(driver.mutationJournal,
		ledgerEntry{Op: "create", PodName: "pod-a", AppCode: "obs-app-0", IssuedAt: start},
		ledgerEntry{Op: "delete", PodName: "pod-a", AppCode: "obs-app-0", IssuedAt: start.Add(time.Second)},
	)
	driver.mu.Unlock()
	journal := driver.mutationJournalSince(start)
	if len(journal) != 2 || journal[0].Op != "create" || journal[1].Op != "delete" {
		t.Fatalf("mutation journal = %+v, want append-only create/delete history", journal)
	}
}

func TestObserveUnitDuplicateDeleteDoesNotInventMutation(t *testing.T) {
	driver := newChurnDriver("unused", []string{"obs-app-0"}, observePodPrefix)
	deleted, err := driver.deletePods([]string{"obs-pod-already-gone"}, time.Now())
	if err != nil {
		t.Fatalf("duplicate delete should be a no-op: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("duplicate delete count = %d, want 0", deleted)
	}
	if journal := driver.mutationJournalSince(time.Time{}); len(journal) != 0 {
		t.Fatalf("duplicate delete journal = %+v, want no invented mutation", journal)
	}
	if driver.applied != 0 {
		t.Fatalf("applied count = %d, want 0 after duplicate delete", driver.applied)
	}
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

func TestObserveUnitStableTickRequiresExactEquality(t *testing.T) {
	diff := diffResult{Divergences: []divergence{{Kind: divMissing, InFlight: true}}, InFlightCount: 1}
	if got := strictTickVerdict(diff, true); got != verdictConsistent {
		t.Fatalf("mutation-overlap verdict = %s, want CONSISTENT transitional", got)
	}
	if got := strictTickVerdict(diff, false); got != verdictDivergent {
		t.Fatalf("stable young mismatch verdict = %s, want DIVERGENT", got)
	}
	if got := strictTickVerdict(diffResult{}, false); got != verdictConsistent {
		t.Fatalf("stable exact verdict = %s, want CONSISTENT", got)
	}
}

func TestObserveUnitSpotterMismatchRetriesOnlyWhenMutationAttributed(t *testing.T) {
	if !snapshotRetryable(tickRecord{Verdict: string(verdictDivergent), SpotterRetryable: true}) {
		t.Fatal("mutation-attributed Spotter mismatch was not retryable")
	}
	if snapshotRetryable(tickRecord{Verdict: string(verdictDivergent), SpotterObserved: true, SpotterEqual: false}) {
		t.Fatal("unattributed Spotter mismatch was incorrectly retryable")
	}
}

func TestObserveUnitTransitionalClassificationExcludesObservationErrors(t *testing.T) {
	transitional := tickRecord{
		Verdict:          string(verdictConsistent),
		ExactEqual:       false,
		MutationObserved: true,
		InFlight:         1,
		Divergence:       []divergence{{Kind: divMissing, InFlight: true}},
	}
	if !tickIsTransitional(transitional) {
		t.Fatal("in-flight consistent tick was not classified as transitional")
	}
	if tickIsTransitional(tickRecord{
		Verdict:          string(verdictConsistent),
		MutationObserved: false,
		InFlight:         1,
		Divergence:       []divergence{{Kind: divMissing, InFlight: true}},
	}) {
		t.Fatal("stable tick with a young stale mismatch was incorrectly classified as transitional")
	}
	if tickIsTransitional(tickRecord{
		Verdict:    string(verdictObsErr),
		InFlight:   0,
		Divergence: nil,
	}) {
		t.Fatal("OBSERR tick was incorrectly classified as transitional")
	}
	if tickIsTransitional(tickRecord{
		Verdict:    string(verdictConsistent),
		ExactEqual: true,
		InFlight:   0,
	}) {
		t.Fatal("exact tick was incorrectly classified as transitional")
	}
}

func TestObserveUnitMutationBoundaryTracksBetweenAndOverlappingTicks(t *testing.T) {
	driver := newChurnDriver("unused", []string{"obs-app-0"}, observePodPrefix)
	if seq, observed := finishTickMutationState(driver, 0, 0, false); seq != 0 || observed {
		t.Fatalf("stable mutation state = (%d,%t), want (0,false)", seq, observed)
	}
	driver.mu.Lock()
	driver.mutationSeq = 1
	driver.mutationActive = 1
	driver.mu.Unlock()
	if seq, observed := finishTickMutationState(driver, 0, 0, false); seq != 1 || !observed {
		t.Fatalf("between-tick mutation state = (%d,%t), want (1,true)", seq, observed)
	}
	if seq, observed := finishTickMutationState(driver, 1, 1, true); seq != 1 || !observed {
		t.Fatalf("overlapping mutation state = (%d,%t), want (1,true)", seq, observed)
	}
	driver.finishMutation()
	if seq, observed := finishTickMutationState(driver, 1, 1, false); seq != 1 || observed {
		t.Fatalf("post-mutation stable state = (%d,%t), want (1,false)", seq, observed)
	}
}

func TestObserveUnitExactAndTransitionalCountersExcludeOBSERR(t *testing.T) {
	run := &observeRun{tracker: newDivergenceTracker()}
	run.recordTick(t, tickRecord{Verdict: string(verdictConsistent), ExactEqual: true})
	run.recordTick(t, tickRecord{
		Verdict:          string(verdictConsistent),
		MutationObserved: true,
		InFlight:         1,
		Divergence:       []divergence{{Kind: divMissing, InFlight: true}},
	})
	run.recordTick(t, tickRecord{Verdict: string(verdictObsErr)})
	if run.tickCount != 3 || run.exactEqualTicks != 1 || run.transitionalTicks != 1 || run.obsErr != 1 {
		t.Fatalf("tick counters = total:%d exact:%d transitional:%d obserr:%d, want 3/1/1/1",
			run.tickCount, run.exactEqualTicks, run.transitionalTicks, run.obsErr)
	}
}

func TestObserveUnitChurnFailureFailsAcceptance(t *testing.T) {
	run := &observeRun{cfg: observeConfig{}, tracker: newDivergenceTracker()}
	pass, failures := run.evaluateAcceptance(runSummary{ChurnErrors: 1, DrainedAtEnd: true})
	if pass {
		t.Fatal("acceptance passed with a source mutation failure")
	}
	if got := strings.Join(failures, "; "); !strings.Contains(got, "churn: 1 source mutation operations failed") {
		t.Fatalf("acceptance failures = %q, want churn failure", got)
	}
}

func TestObserveUnitFinalPopulationDriftFailsAcceptance(t *testing.T) {
	run := &observeRun{cfg: observeConfig{BaseInstances: 1000}, tracker: newDivergenceTracker()}
	pass, failures := run.evaluateAcceptance(runSummary{Ticks: 1, Scale: 1000, FinalSourceCount: 1066, DrainedAtEnd: true})
	if pass {
		t.Fatal("acceptance passed with net-neutral population drift")
	}
	if got := strings.Join(failures, "; "); !strings.Contains(got, "final source count 1066 != base 1000") {
		t.Fatalf("acceptance failures = %q, want final population drift", got)
	}
}

func TestObserveUnitFinalSnapshotDoesNotDiluteWindowRatios(t *testing.T) {
	run := &observeRun{
		tickCount:    19,
		consistent:   18,
		obsErr:       1,
		sourceGEBase: 18,
		steadyTicks:  19,
	}
	run.captureFinalSnapshot(tickRecord{
		Verdict:        string(verdictConsistent),
		SnapshotStable: true,
		ExactEqual:     true,
		SpotterEqual:   true,
		Source:         sideCount{Count: 1000},
	})
	if run.tickCount != 19 || run.consistent != 18 || run.obsErr != 1 || run.sourceGEBase != 18 || run.steadyTicks != 19 {
		t.Fatalf("final proof changed window counters: ticks=%d consistent=%d obsErr=%d sourceGEBase=%d steady=%d",
			run.tickCount, run.consistent, run.obsErr, run.sourceGEBase, run.steadyTicks)
	}
	if run.finalSourceCount != 1000 || !run.finalSnapshotExact {
		t.Fatalf("final proof not captured: count=%d exact=%t", run.finalSourceCount, run.finalSnapshotExact)
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

func TestObserveUnitChurnExcludesBurstAndCrashOwnedPods(t *testing.T) {
	r := &observeRun{
		appCodes:    []string{"obs-app-0"},
		driver:      newChurnDriver("unused", []string{"obs-app-0"}, observePodPrefix),
		burstEvents: []burstEvent{{Pods: []string{"obs-pod-burst"}}},
		crashPod:    "obs-pod-crash",
	}
	if r.isChurnCandidate(sourcePod{Name: "obs-pod-burst", AppCode: "obs-app-0", Phase: "Running"}) {
		t.Fatal("burst-owned Pod was eligible for ordinary churn")
	}
	if r.isChurnCandidate(sourcePod{Name: "obs-pod-crash", AppCode: "obs-app-0", Phase: "Running"}) {
		t.Fatal("crash-owned Pod was eligible for ordinary churn")
	}
	if !r.isChurnCandidate(sourcePod{Name: "obs-pod-free", AppCode: "obs-app-0", Phase: "Running"}) {
		t.Fatal("ordinary running harness Pod was not eligible for churn")
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
	r.burstEvents = []burstEvent{{
		Name: "storm-100", Size: 2, Pods: []string{"obs-pod-1", "obs-pod-2"},
		CreateIssued: create,
	}}
	tickTS := time.Now()
	// Both pods are present on all three planes: only the UP leg converges.
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictConsistent), TS: formatTime(tickTS), Divergence: nil,
		SnapshotStable: true, ExactEqual: true, SpotterEqual: true,
		SourceIDs: []string{"obs-pod-1", "obs-pod-2"}, SpotterIDs: []string{"obs-pod-1", "obs-pod-2"}, RemoteIDs: []string{"obs-pod-1", "obs-pod-2"},
	})
	ev := r.burstEvents[0]
	if ev.ConvergedUp.IsZero() || !ev.ConvergedDown.IsZero() {
		t.Fatalf("present tick must converge only UP: %+v", ev)
	}
	// Once delete is issued, all three planes must omit every target before
	// the DOWN leg converges.
	r.burstEvents[0].DeleteIssued = time.Now().Add(-15 * time.Second)
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictConsistent), TS: formatTime(time.Now()), SnapshotStable: true, ExactEqual: true, SpotterEqual: true,
	})
	if r.burstEvents[0].ConvergedDown.IsZero() {
		t.Fatalf("absent tick must converge DOWN: %+v", r.burstEvents[0])
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
	// A source/Nacos-clean tick with a Spotter-only mismatch is not a
	// three-plane convergence point.
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictDivergent), TS: formatTime(time.Now()), SnapshotStable: true,
		ExactEqual: false, SpotterEqual: false,
	})
	if !r.burstEvents[0].ConvergedUp.IsZero() {
		t.Fatal("Spotter-only mismatch incorrectly marked burst converged")
	}
	r.checkBurstConvergence(tickRecord{
		Verdict: string(verdictConsistent), TS: formatTime(time.Now()), Divergence: nil,
		SnapshotStable: true, ExactEqual: true, SpotterEqual: true,
		SourceIDs: []string{"obs-pod-3"}, SpotterIDs: []string{"obs-pod-3"}, RemoteIDs: []string{"obs-pod-3"},
	})
	if r.burstEvents[0].ConvergedUp.IsZero() {
		t.Fatalf("the clean tick after the divergence must converge the leg")
	}
}

func TestObserveUnitBurstWatchResultUsesFinalStableBoundaryAndChunkClocks(t *testing.T) {
	base := time.Now().Add(-time.Minute)
	observation := func(at time.Duration, present bool, status int32, key string) planeWatchObservation {
		return planeWatchObservation{
			At: base.Add(at), Present: present, Status: status, SourceKey: key,
			Operation: "Sync", Origin: "event-cache-applied", Reversion: 7,
			CanonicalPayload: "canonical",
		}
	}
	timeline := &watchTimeline{
		sourceHistory: map[string][]planeWatchObservation{
			"pod-a": {observation(2*time.Second, false, 3, "uid-a"), observation(5*time.Second, true, 1, "uid-a"), observation(20*time.Second, false, 3, "uid-a"), observation(8*time.Second, true, 1, "uid-a")},
			"pod-b": {observation(30*time.Second, false, 3, "uid-b")},
		},
		spotterHistory: map[string][]planeWatchObservation{
			"pod-a": {observation(3*time.Second, false, 3, "cluster/uid-a"), observation(6*time.Second, true, 1, "cluster/uid-a"), observation(21*time.Second, false, 3, "cluster/uid-a"), observation(9*time.Second, true, 1, "cluster/uid-a")},
			"pod-b": {observation(31*time.Second, false, 3, "cluster/uid-b")},
		},
		nacosHistory: map[string][]planeWatchObservation{
			"pod-a": {observation(4*time.Second, false, 3, "cluster/uid-a"), observation(7*time.Second, true, 1, "cluster/uid-a"), observation(22*time.Second, false, 3, "cluster/uid-a"), observation(10*time.Second, true, 1, "cluster/uid-a")},
			"pod-b": {observation(40*time.Second, false, 3, "cluster/uid-b")},
		},
	}
	driver := newChurnDriver("unused", []string{"obs-app-0"}, observePodPrefix)
	driver.mutationJournal = []ledgerEntry{
		{Op: "delete", PodName: "pod-a", AppCode: "obs-app-0", IssuedAt: base},
		{Op: "delete", PodName: "pod-b", AppCode: "obs-app-0", IssuedAt: base.Add(10 * time.Second)},
	}
	run := &observeRun{driver: driver, timeline: timeline, windowStart: base.Add(-time.Second)}
	result, ok := run.burstWatchResult([]string{"pod-a", "pod-b"}, "delete", base.Add(50*time.Second))
	if !ok {
		t.Fatal("final stable burst boundary was not found")
	}
	if !result.FirstIssued.Equal(base) || !result.ConvergedAt.Equal(base.Add(40*time.Second)) {
		t.Fatalf("burst result = %+v, want first=%s converged=%s", result, base, base.Add(40*time.Second))
	}
	if result.MaxPerItem != 30*time.Second {
		t.Fatalf("max per-item = %s, want 30s from pod-b's own chunk clock", result.MaxPerItem)
	}
	if _, ok := run.burstWatchResult([]string{"pod-a", "pod-missing"}, "delete", base.Add(50*time.Second)); ok {
		t.Fatal("burst result passed with one target missing its exact boundary")
	}
	run.burstEvents = []burstEvent{{Name: "batch", Pods: []string{"pod-a", "pod-b"}, DeleteIssued: base}}
	run.refreshBurstWatchConvergence(base.Add(50 * time.Second))
	if !run.burstEvents[0].ConvergedDown.IsZero() {
		t.Fatal("watch-only result bypassed the stable-cut membership proof")
	}
	run.burstEvents[0].DownMembershipProven = true
	run.refreshBurstWatchConvergence(base.Add(50 * time.Second))
	if !run.burstEvents[0].ConvergedDown.Equal(base.Add(40 * time.Second)) {
		t.Fatalf("membership-proven convergence = %s, want %s", run.burstEvents[0].ConvergedDown, base.Add(40*time.Second))
	}
	// Repeated desired-state updates near the horizon must not move create
	// convergence from the first exact boundary to the last healthy refresh.
	timeline.sourceHistory["pod-c"] = []planeWatchObservation{
		observation(40*time.Second, true, 1, "uid-c"), observation(time.Second, true, 1, "uid-c"),
	}
	timeline.spotterHistory["pod-c"] = []planeWatchObservation{
		observation(41*time.Second, true, 1, "cluster/uid-c"), observation(2*time.Second, true, 1, "cluster/uid-c"),
	}
	timeline.nacosHistory["pod-c"] = []planeWatchObservation{
		observation(42*time.Second, true, 1, "cluster/uid-c"), observation(3*time.Second, true, 1, "cluster/uid-c"),
	}
	driver.mutationJournal = append(driver.mutationJournal, ledgerEntry{Op: "create", PodName: "pod-c", AppCode: "obs-app-0", IssuedAt: base})
	create, ok := run.burstWatchResult([]string{"pod-c"}, "create", base.Add(50*time.Second))
	if !ok || !create.ConvergedAt.Equal(base.Add(3*time.Second)) {
		t.Fatalf("repeated desired update result=%+v ok=%t, want first exact boundary at +3s", create, ok)
	}
}

func TestObserveUnitBurstBoundUsesUnroundedDuration(t *testing.T) {
	run := &observeRun{cfg: observeConfig{Duration: 20 * time.Minute, Bursts: true}, tracker: newDivergenceTracker()}
	bursts := make([]burstSummary, len(burstSchedule(run.cfg.Duration)))
	for i := range bursts {
		bursts[i] = burstSummary{
			Name: "burst", CreateIssued: "issued", ConvergedUp: "seen", UpConvergence: time.Minute.String(),
			DeleteIssued: "issued", ConvergedDown: "seen", DownConvergence: time.Minute.String(),
		}
	}
	bursts[0].DownConvergence = (time.Minute + 400*time.Millisecond).String()
	_, failures := run.evaluateAcceptance(runSummary{Bursts: bursts, DrainedAtEnd: true})
	if got := strings.Join(failures, "; "); !strings.Contains(got, "DOWN convergence 1m0.4s exceeds OBS_BOUND 1m0s") {
		t.Fatalf("failures=%q, want the unrounded 60.4s burst rejection", got)
	}
	bursts[0].DownConvergence = time.Minute.String()
	_, failures = run.evaluateAcceptance(runSummary{Bursts: bursts, DrainedAtEnd: true})
	if got := strings.Join(failures, "; "); strings.Contains(got, "burst burst: DOWN convergence") {
		t.Fatalf("exactly-60s burst was rejected: %q", got)
	}
}

func TestObserveUnitBurstSummaryPreservesNanosecondTimestamps(t *testing.T) {
	base := time.Date(2026, 9, 19, 3, 30, 35, 603755000, time.FixedZone("SGT", 8*60*60))
	summary := burstSummaryFromEvent(burstEvent{
		Name: "batch", Size: 200,
		CreateIssued: base, ConvergedUp: base.Add(4*time.Second + 123*time.Microsecond),
		DeleteIssued:  base.Add(time.Minute + 7*time.Nanosecond),
		ConvergedDown: base.Add(time.Minute + 51*time.Second + 47293000*time.Nanosecond),
	})
	for name, value := range map[string]string{
		"createIssued": summary.CreateIssued, "convergedUp": summary.ConvergedUp,
		"deleteIssued": summary.DeleteIssued, "convergedDown": summary.ConvergedDown,
	} {
		if !strings.Contains(value, ".") {
			t.Fatalf("%s=%q lost its fractional timestamp", name, value)
		}
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			t.Fatalf("%s=%q is not RFC3339Nano: %v", name, value, err)
		}
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

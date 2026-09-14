//go:build observe
// +build observe

package observe

import (
	"fmt"
	"sort"
	"time"
)

// The comparison engine (dsca-4 §4.3 steps 3-5 / §6.2's corrected
// semantics, pinned by engine_test.go):
//
//   - the expected side is the LIVE source (per tick, never cached);
//   - the remote side is the list∪catalog union with max-count set
//     semantics (duplicate detection keeps its teeth);
//   - the in-flight tolerance: an expected-side entry whose source state
//     changed at c(e) is tolerated ONLY while t − c(e) ≤ OBS_BOUND —
//     tolerance, not amnesty: at c(e)+B the mismatch flips the tick to
//     DIVERGENT;
//   - the foreign-delete fail-safe: a remote extra with NO source object
//     and NO ledger entry is DIVERGENT immediately (no source object =
//     no deletionTimestamp = no clock to wait for);
//   - duplicates are never tolerable;
//   - a disabled-mismatch (expected-online but remote disabled, or a
//     remote disabled entry with no source object) is a divergence;
//   - any source/remote read failure is OBSERR (retried, never CONSISTENT,
//     never divergence evidence on an emptied side).

// verdictKind is the per-tick verdict.
type verdictKind string

const (
	verdictConsistent verdictKind = "CONSISTENT"
	verdictDivergent  verdictKind = "DIVERGENT"
	verdictObsErr     verdictKind = "OBSERR"
)

// divergenceKind classifies one mismatch.
type divergenceKind string

const (
	divMissing  divergenceKind = "missing"  // expected-online, not remote
	divExtra    divergenceKind = "extra"    // remote, no source object
	divDup      divergenceKind = "dup"      // duplicate id within one view
	divDisabled divergenceKind = "disabled" // enabled-flag mismatch
	divField    divergenceKind = "field"    // projected field or metadata mismatch
)

// divergence is one mismatch occurrence on one tick.
type divergence struct {
	AppCode   string         `json:"appCode"`
	Cluster   string         `json:"cluster"`
	ID        string         `json:"id"`        // the pod name (domain id)
	Composite string         `json:"composite"` // the nacos composite instanceId when known
	Kind      divergenceKind `json:"kind"`
	InFlight  bool           `json:"inFlight"` // tolerated: inside OBS_BOUND of its clock
	Clock     time.Time      `json:"clock"`    // the c(e) the tolerance consumed
}

// sourceModel is the expected side: per app-code the online ids
// (Running+ready+IP) and the unhealthy ids (Running-not-ready: the
// converter pushes them disabled). Pending pods are filtered entirely
// (InitInstanceFilters drops InstanceStatePending — the batch-D contract).
type sourceModel struct {
	Online    map[string][]string               // appCode -> pod names
	Unhealthy map[string][]string               // appCode -> pod names
	Entries   map[string]map[string]sourceEntry // appCode -> pod name -> projected wire fields
	CreatedAt map[string]time.Time
	// Count is the raw source population (all phases — the scale
	// criterion's source count).
	Count int
}

// sourceEntry contains the fields that the K8s converter deterministically
// projects into the Nacos registration. The Observe comparison intentionally
// checks only this stable projection; source fields such as resourceVersion
// and sourceKey are unavailable in the one-call Pod snapshot.
type sourceEntry struct {
	ID          string
	IP          string
	Port        int
	ClusterName string
	ServiceName string
	CompositeID string
	Enabled     bool
	Healthy     bool
	Ephemeral   bool
	Status      string
	Metadata    map[string]string
}

// buildSourceModel derives the expected model from the live pods (the
// conversion semantics incl. the Pending filter).
func buildSourceModel(pods []sourcePod, appCodes []string) *sourceModel {
	model := &sourceModel{
		Online:    map[string][]string{},
		Unhealthy: map[string][]string{},
		Entries:   map[string]map[string]sourceEntry{},
		CreatedAt: map[string]time.Time{},
		Count:     0,
	}
	for _, pod := range pods {
		if !isObservedAppCode(pod.AppCode, appCodes) {
			continue // a foreign app-code is not this run's model (recorded via Count diff)
		}
		model.Count++
		if model.Entries[pod.AppCode] == nil {
			model.Entries[pod.AppCode] = map[string]sourceEntry{}
		}
		switch {
		case pod.Phase == "Running" && pod.PodIP != "" && pod.ContainersReady:
			model.Online[pod.AppCode] = append(model.Online[pod.AppCode], pod.Name)
			model.Entries[pod.AppCode][pod.Name] = expectedSourceEntry(pod, true, "1")
		case pod.Phase == "Running" && pod.PodIP != "":
			// Running but not containers-ready: expected-unhealthy (the
			// converter pushes it enabled=false — visible in the catalog).
			model.Unhealthy[pod.AppCode] = append(model.Unhealthy[pod.AppCode], pod.Name)
			model.Entries[pod.AppCode][pod.Name] = expectedSourceEntry(pod, false, "2")
		case pod.Phase == "Running":
			// Running without an IP yet: not representable (the converter's
			// Online path needs the IP); treated as pending-in-flight.
			model.Unhealthy[pod.AppCode] = append(model.Unhealthy[pod.AppCode], pod.Name)
			model.Entries[pod.AppCode][pod.Name] = expectedSourceEntry(pod, false, "2")
		default:
			// Pending / other phases: filtered (the Pending filter). The
			// pod's creationTimestamp is still the in-flight clock — a pod
			// that flips Running becomes expected at that moment; nacos has
			// no entry for it yet, and the tolerance consumes its clock.
		}
		if !pod.CreatedAt.IsZero() {
			model.CreatedAt[pod.Name] = pod.CreatedAt
		}
	}
	for _, ids := range model.Online {
		sort.Strings(ids)
	}
	for _, ids := range model.Unhealthy {
		sort.Strings(ids)
	}
	return model
}

func expectedSourceEntry(pod sourcePod, enabled bool, status string) sourceEntry {
	const port = 7096 // formatAppPort places the compatibility port first.
	const cluster = "k8s"
	const group = "DEFAULT_GROUP"
	return sourceEntry{
		ID: pod.Name, IP: pod.PodIP, Port: port, ClusterName: cluster,
		ServiceName: pod.AppCode,
		CompositeID: fmt.Sprintf("%s#%d#%s#%s@@%s", pod.PodIP, port, cluster, group, pod.AppCode),
		Enabled:     enabled, Healthy: enabled, Ephemeral: false, Status: status,
		Metadata: map[string]string{"spotterOwner": "spotter", "instanceId": pod.Name, "status": status},
	}
}

// isObservedAppCode reports whether an app-code is one of this run's
// observed services.
func isObservedAppCode(appCode string, appCodes []string) bool {
	for _, code := range appCodes {
		if code == appCode {
			return true
		}
	}
	return false
}

// clockSource resolves the in-flight clock of one id (the ledger entry
// first — the driver's own mutation; the source creationTimestamp second
// — organic changes; zero when neither exists).
func clockSource(id string, ledger func(string) (ledgerEntry, bool), model *sourceModel) time.Time {
	if entry, ok := ledger(id); ok {
		return entry.IssuedAt
	}
	if created, ok := model.CreatedAt[id]; ok {
		return created
	}
	return time.Time{}
}

// diffResult is one tick's comparison outcome (before the verdict).
type diffResult struct {
	Divergences []divergence
	// InFlightCount is the number of mismatches tolerated by the bound.
	InFlightCount int
}

// compareService runs the bidirectional diff of one (appCode, cluster)
// pair. ledger resolves the mutation clock of an id; bound is OBS_BOUND;
// now is the tick time. The remote view is the unioned multiset with
// enabled flags.
func compareService(appCode string, model *sourceModel, remote []remoteEntry,
	ledger func(string) (ledgerEntry, bool), bound time.Duration, now time.Time) diffResult {

	result := diffResult{}
	expectedOnline := map[string]int{}
	for _, id := range model.Online[appCode] {
		expectedOnline[id]++
	}
	expectedUnhealthy := map[string]int{}
	for _, id := range model.Unhealthy[appCode] {
		expectedUnhealthy[id]++
	}
	remoteCount := map[string]int{}
	remoteEntries := map[string][]remoteEntry{}
	for _, entry := range remote {
		remoteCount[entry.ID]++
		remoteEntries[entry.ID] = append(remoteEntries[entry.ID], entry)
	}

	record := func(kind divergenceKind, id, composite string, clock time.Time) {
		inFlight := false
		if !clock.IsZero() && now.Sub(clock) <= bound {
			inFlight = true
		}
		result.Divergences = append(result.Divergences, divergence{
			AppCode:   appCode,
			Cluster:   "k8s",
			ID:        id,
			Composite: composite,
			Kind:      kind,
			InFlight:  inFlight,
			Clock:     clock,
		})
		if inFlight {
			result.InFlightCount++
		}
	}

	// Missing: expected-online, not remote (or remote-disabled where
	// online is expected). The clock is the ledger/creationTimestamp of
	// the pod (a just-created pod legitimately not yet in nacos).
	for id := range expectedOnline {
		count, seen := remoteCount[id]
		switch {
		case !seen:
			record(divMissing, id, "", clockSource(id, ledger, model))
		case count > expectedOnline[id]:
			record(divDup, id, compositeOf(remote, id), time.Time{})
		case !remoteEntryMatches(model.Entries[appCode][id], remoteEntries[id][0]):
			record(divField, id, remoteEntries[id][0].CompositeID, clockSource(id, ledger, model))
		case !remoteEntries[id][0].Enabled:
			record(divDisabled, id, compositeOf(remote, id), clockSource(id, ledger, model))
		}
	}
	// Expected-unhealthy entries: the converter pushes them enabled=false;
	// they must be PRESENT (as disabled) — their absence is a missing
	// divergence with the pod clock; their presence as ENABLED is a
	// disabled mismatch.
	for id := range expectedUnhealthy {
		count, seen := remoteCount[id]
		if !seen {
			record(divMissing, id, "", clockSource(id, ledger, model))
			continue
		}
		if count > expectedUnhealthy[id] {
			record(divDup, id, compositeOf(remote, id), time.Time{})
		}
		if !remoteEntryMatches(model.Entries[appCode][id], remoteEntries[id][0]) {
			record(divField, id, remoteEntries[id][0].CompositeID, clockSource(id, ledger, model))
		} else if remoteEntries[id][0].Enabled {
			record(divDisabled, id, compositeOf(remote, id), clockSource(id, ledger, model))
		}
	}
	// Extra: remote, no source object. A ledger delete entry inside the
	// bound is in-flight (the deregister is propagating); anything older
	// is divergence; NO clock at all (no source object, no ledger) is
	// DIVERGENT IMMEDIATELY (the §3.4 fail-safe).
	for id, count := range remoteCount {
		if expectedOnline[id] > 0 || expectedUnhealthy[id] > 0 {
			if count > expectedOnline[id]+expectedUnhealthy[id] {
				record(divDup, id, compositeOf(remote, id), time.Time{})
			}
			continue
		}
		record(divExtra, id, compositeOf(remote, id), clockSource(id, ledger, model))
	}
	sort.Slice(result.Divergences, func(i, j int) bool {
		if result.Divergences[i].AppCode != result.Divergences[j].AppCode {
			return result.Divergences[i].AppCode < result.Divergences[j].AppCode
		}
		if result.Divergences[i].ID != result.Divergences[j].ID {
			return result.Divergences[i].ID < result.Divergences[j].ID
		}
		return result.Divergences[i].Kind < result.Divergences[j].Kind
	})
	return result
}

// remoteEntryMatches compares the deterministic K8s-to-Nacos projection. A
// manually constructed fixture model has no Entries entry, so the historical
// ID/enabled assertions remain available to the HTTP fixture tests.
func remoteEntryMatches(expected sourceEntry, got remoteEntry) bool {
	if expected.ID == "" {
		return true
	}
	if expected.ID != got.ID || expected.IP != got.IP || expected.Port != got.Port ||
		expected.ClusterName != got.ClusterName || !serviceNameMatches(expected.ServiceName, got.ServiceName) ||
		expected.CompositeID != got.CompositeID ||
		expected.Healthy != got.Healthy || expected.Ephemeral != got.Ephemeral {
		return false
	}
	for key, want := range expected.Metadata {
		if got.Metadata[key] != want {
			return false
		}
	}
	return true
}

func copyStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func serviceNameMatches(expected, got string) bool {
	return got == expected || got == "DEFAULT_GROUP@@"+expected
}

// compositeOf finds the composite id of an entry id in the remote view.
func compositeOf(remote []remoteEntry, id string) string {
	for _, entry := range remote {
		if entry.ID == id {
			return entry.CompositeID
		}
	}
	return ""
}

// tickVerdict derives the verdict from a diff: any mismatch NOT in-flight
// (and every duplicate, always) makes the tick DIVERGENT.
func tickVerdict(diff diffResult) verdictKind {
	if diff.Divergences == nil {
		return verdictConsistent
	}
	for _, d := range diff.Divergences {
		switch d.Kind {
		case divDup:
			return verdictDivergent // never tolerable
		case divMissing, divExtra, divDisabled, divField:
			if !d.InFlight {
				return verdictDivergent
			}
		}
	}
	// All mismatches in-flight: CONSISTENT (bounded staleness accounted).
	return verdictConsistent
}

// strictTickVerdict requires zero data differences whenever no source
// mutation occurred since the prior observation boundary. A tick that overlaps
// a known mutation may carry only in-flight mismatches; their age is still
// bounded by tickVerdict and the continuity tracker.
func strictTickVerdict(diff diffResult, mutationObserved bool) verdictKind {
	verdict := tickVerdict(diff)
	if verdict == verdictConsistent && len(diff.Divergences) > 0 && !mutationObserved {
		return verdictDivergent
	}
	return verdict
}

// tickIsTransitional identifies a successful comparison that still contains
// only in-flight mismatches. OBSERR records are never transitional: they did
// not produce a complete source/remote comparison and must remain visible as
// observation errors.
func tickIsTransitional(record tickRecord) bool {
	return record.Verdict == string(verdictConsistent) &&
		record.MutationObserved &&
		!record.ExactEqual && len(record.Divergence) > 0 &&
		record.InFlight == len(record.Divergence)
}

// continuityKey keys a divergence across ticks (the §4.3 step-7 ledger:
// firstSeenTick pins the true age — DS-4-3's fix).
func continuityKey(d divergence) string {
	return fmt.Sprintf("%s/%s/%s/%s", d.AppCode, d.Cluster, d.ID, d.Kind)
}

// divergenceTracker carries divergences across ticks until resolved,
// recording firstSeen and resolved times (the true heal measurement).
type divergenceTracker struct {
	entries map[string]*trackedDivergence
}

type trackedDivergence struct {
	Key        string
	Divergence divergence
	FirstSeen  time.Time
	Resolved   time.Time
	LastSeen   time.Time
}

func newDivergenceTracker() *divergenceTracker {
	return &divergenceTracker{entries: map[string]*trackedDivergence{}}
}

// observeTick updates the tracker with one tick's divergences: new keys
// start tracking (firstSeen = now), existing keys refresh lastSeen, and
// keys absent from this tick resolve (resolved = now).
func (t *divergenceTracker) observeTick(now time.Time, divergences []divergence) {
	seen := map[string]bool{}
	for _, d := range divergences {
		key := continuityKey(d)
		seen[key] = true
		if existing, ok := t.entries[key]; ok {
			existing.LastSeen = now
			existing.Divergence = d
		} else {
			t.entries[key] = &trackedDivergence{
				Key: key, Divergence: d, FirstSeen: now, LastSeen: now,
			}
		}
	}
	for key, existing := range t.entries {
		if !existing.Resolved.IsZero() {
			continue
		}
		if !seen[key] {
			existing.Resolved = now
		}
	}
}

// openCount returns the currently-open (unresolved) divergences.
func (t *divergenceTracker) openCount() int {
	n := 0
	for _, existing := range t.entries {
		if existing.Resolved.IsZero() {
			n++
		}
	}
	return n
}

// maxHeal returns the longest firstSeen->resolved span of the resolved
// entries (the true max heal time; 0 when none resolved).
func (t *divergenceTracker) maxHeal() time.Duration {
	var max time.Duration
	for _, existing := range t.entries {
		if existing.Resolved.IsZero() {
			continue
		}
		if heal := existing.Resolved.Sub(existing.FirstSeen); heal > max {
			max = heal
		}
	}
	return max
}

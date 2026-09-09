//go:build soak
// +build soak

package soak

import (
	"fmt"
	"sort"
	"strings"

	"spotter/internal/domain/instance"
	"spotter/internal/testkit/discoverymock"
)

// atlasCallSnapshot is one recorded stand-in call, flattened to the fields
// the payload comparison needs (an adapter over discoverymock.Call so the
// comparison logic below stays mock-shape-independent).
type atlasCallSnapshot struct {
	Method    string
	Instances []*atlasInstanceRecord
}

// atlasInstanceRecord is one instance as the stand-in received it.
type atlasInstanceRecord struct {
	AppCode    string
	InstanceId string
	Provider   string
	Status     int32
}

// atlasSnapshots flattens the stand-in's call log into the comparison's
// input shape.
func atlasSnapshots(calls []discoverymock.Call) []atlasCallSnapshot {
	snapshots := make([]atlasCallSnapshot, 0, len(calls))
	for _, call := range calls {
		snapshot := atlasCallSnapshot{Method: call.Method, Instances: []*atlasInstanceRecord{}}
		for _, ins := range call.Instances {
			if ins == nil {
				continue
			}
			snapshot.Instances = append(snapshot.Instances, &atlasInstanceRecord{
				AppCode:    ins.AppCode,
				InstanceId: ins.InstanceId,
				Provider:   ins.Provider,
				Status:     ins.Status,
			})
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots
}

// The Atlas payload parity assertion of AUDIT-D-11: the soak's Atlas
// stand-in (discoverymock StartTCP) records every SynInstance call's full
// payload, but the standing assertions only ever counted calls — a fanout
// corrupting Atlas payloads while pushing Nacos correctly would pass. This
// file compares the stand-in's recorded instance sets against the SAME
// expected model the nacos view compares to (expectedState): per app-code
// and provider-cluster, the ids Atlas last saw must match the model.
//
// The comparison runs at each assert tick (assertOnce) and is summarized by
// finalConvergence/atlasParityLabel. Semantics:
//
//   - the LATEST push per (appCode, instanceId) wins: the stand-in's call
//     log is a stream of incremental pushes, and an instance pushed online
//     then offline legitimately ends with the offline record — the model's
//     current set is the comparison target, and the latest record is the
//     stand-in's current knowledge of that instance;
//   - an instance whose latest record is Status 3 (offline) is absent
//     (it was a deregistration push — the same convention the nacos view's
//     model applies: offline entries are not part of the live set);
//   - unknown clusters (anything but k8s/ecs) hold only "extra" ids, the
//     same rule the nacos comparison applies.

// atlasDivergence describes the Atlas payload parity violation (D-11).
type atlasDivergence struct {
	appCode string
	cluster string
	missing []string // modeled, not in Atlas's latest payloads
	extra   []string // in Atlas's latest payloads, not modeled
}

func (d atlasDivergence) String() string {
	parts := []string{fmt.Sprintf("%s/%s:", d.appCode, d.cluster)}
	if len(d.missing) > 0 {
		parts = append(parts, fmt.Sprintf("missing %v", d.missing))
	}
	if len(d.extra) > 0 {
		parts = append(parts, fmt.Sprintf("extra %v", d.extra))
	}
	return strings.Join(parts, " ")
}

// atlasView reduces the stand-in's recorded SynInstance/SynAllInstance
// calls to the LATEST record per (appCode, instanceId): the stand-in's
// current knowledge. Calls are time-ordered by arrival (the mock appends
// under its lock), so a later record replaces an earlier one.
func atlasView(calls []atlasCallSnapshot) map[string]map[string]*atlasInstanceRecord {
	latest := map[string]map[string]*atlasInstanceRecord{}
	for _, call := range calls {
		for _, record := range call.Instances {
			if record == nil {
				continue
			}
			if latest[record.AppCode] == nil {
				latest[record.AppCode] = map[string]*atlasInstanceRecord{}
			}
			latest[record.AppCode][record.InstanceId] = record
		}
	}
	return latest
}

// atlasCheck compares the Atlas stand-in's latest instance records against
// the expected model: per app-code, per cluster (k8s vs live pods, ecs vs
// live consul ids). Returns the divergences (empty when the payloads match).
//
// scope restricts the judgment to the given app-codes; nil judges every
// app-code the model tracks.
func atlasCheck(latest map[string]map[string]*atlasInstanceRecord, expected *expectedState, scope []string) []atlasDivergence {
	appCodes := expected.appCodes()
	if len(scope) > 0 {
		appCodes = scope
	}
	divergences := []atlasDivergence{}
	for _, appCode := range appCodes {
		perCluster := map[string][]string{} // cluster -> live ids per the latest records
		for instanceID, record := range latest[appCode] {
			if record.Status == atlasStatusOffline {
				continue // the latest push deregistered it
			}
			cluster := record.Provider
			if cluster == "" {
				cluster = "DEFAULT"
			}
			perCluster[cluster] = append(perCluster[cluster], instanceID)
		}
		for _, cluster := range atlasClusters(perCluster) {
			var want []string
			if cluster == "k8s" {
				want = expected.k8sFor(appCode)
			} else if cluster == "ecs" {
				want = expected.consulFor(appCode)
			}
			missing, extra, _ := diffMultisets(want, perCluster[cluster])
			if len(missing) > 0 || len(extra) > 0 {
				divergences = append(divergences, atlasDivergence{
					appCode: appCode, cluster: cluster, missing: missing, extra: extra,
				})
			}
		}
	}
	sort.Slice(divergences, func(i, j int) bool {
		if divergences[i].appCode != divergences[j].appCode {
			return divergences[i].appCode < divergences[j].appCode
		}
		return divergences[i].cluster < divergences[j].cluster
	})
	return divergences
}

// atlasClusters returns the distinct clusters of a per-cluster id map,
// sorted (deterministic iteration).
func atlasClusters(perCluster map[string][]string) []string {
	clusters := make([]string, 0, len(perCluster))
	for cluster := range perCluster {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)
	return clusters
}

// atlasStatusOffline is the domain offline status (3): the latest record
// with this status is a deregistration, so the instance is not live.
const atlasStatusOffline = int32(instance.InstanceStatusOffline)

// assertAtlasParity runs one Atlas payload parity pass (D-11) against the
// current model and reports the divergences: nil when the stand-in's latest
// payloads match the expected live sets.
func (h *soakHarness) assertAtlasParity(scope []string) []atlasDivergence {
	if h.atlas == nil {
		return nil
	}
	latest := atlasView(atlasSnapshots(h.atlas.Calls()))
	return atlasCheck(latest, h.expected, scope)
}

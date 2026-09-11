//go:build observe
// +build observe

package observe

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// The remote read side (dsca-4 §4.3 step 2 / §6.2's corrected engine): the
// nacos v1 instance/list view UNIONED with the admin catalog view, per
// service, max-count set semantics (the assert.go unionClusterView
// discipline: cross-view overlap collapses, a within-one-view duplicate
// survives so duplicate detection keeps its teeth). The union keeps the
// ENABLED flag per id: an enabled=false entry the list hides becomes
// visible through the catalog (the exact state spotter's own unhealthy
// pushes write — a disabled zombie must be an observable divergence).
//
// Read failures are OBSERR material, never divergence evidence: a 5xx
// whose body carries the Raft-leaderless signature classifies as
// env-leaderless (retryable environment state, the 20260909-0000
// lesson); a "not found" 500 from the catalog endpoint is the
// fully-pruned steady state of a (service, cluster) pair and counts as
// EMPTY (the batch-1 tolerance, mirrored from assert.go).

// nacosView is the observe package's nacos reader.
type nacosView struct {
	addr string
	http *http.Client
}

func newNacosView(addr string) *nacosView {
	return &nacosView{addr: "http://" + addr, http: sharedHTTP}
}

// nacosHost mirrors the v2.1.0 host shape (pkg/nacos client + assert.go).
type nacosHost struct {
	InstanceID  string            `json:"instanceId"`
	IP          string            `json:"ip"`
	Port        int               `json:"port"`
	Enabled     bool              `json:"enabled"`
	ClusterName string            `json:"clusterName"`
	ServiceName string            `json:"serviceName"`
	Metadata    map[string]string `json:"metadata"`
}

// domainIDOf derives the comparison key of a remote host: the metadata
// instanceId (the DOMAIN instance id the sink round-trips — the pod name
// for k8s instances), falling back to the composite id when the metadata
// is absent (a foreign/zombie writer).
func domainIDOf(host nacosHost) string {
	if id := host.Metadata["instanceId"]; id != "" {
		return id
	}
	return host.InstanceID
}

// remoteEntry is one id in the unioned view with its enabled flag and the
// composite id (the forensic record's nacos-side identity).
type remoteEntry struct {
	ID          string
	Enabled     bool
	CompositeID string
}

// serviceView is one service's unioned view: cluster -> id multiset (with
// per-id enabled flags).
type serviceView map[string][]remoteEntry

// viewError carries the failed read plus its environment classification.
type viewError struct {
	err    error
	leader bool
}

func (e *viewError) Error() string { return e.err.Error() }

// errNacosLeaderless marks a leaderless-classified read failure.
var errNacosLeaderless = fmt.Errorf("nacos naming service Raft group unavailable (leaderless)")

// isLeaderlessErr reports whether a view error is leaderless-classified.
func isLeaderlessErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), errNacosLeaderless.Error())
}

// fullServiceView reads one service's unioned view: instance/list once +
// catalog/instances for the k8s cluster (the only cluster this stack
// pushes), paginated, unioned by (id, composite) with max-count
// semantics. The enabled flag of a unioned id: the max view wins on
// presence; the flag is taken from whichever side reports the id (the
// list only serves enabled=true ids, so an id visible in the list is
// enabled; the catalog's flag is authoritative for hidden ids).
func (v *nacosView) fullServiceView(service string) (serviceView, error) {
	listIDs, listErr := v.listView(service)
	catalogIDs, catalogErr := v.catalogView(service, "k8s")
	if listErr != nil && catalogErr != nil {
		return nil, listErr
	}
	if listErr != nil && len(catalogIDs) == 0 {
		// Both legs failed OR the catalog leg failed with nothing served:
		// the list error stands when the catalog also cannot serve.
		if catalogErr != nil {
			return nil, listErr
		}
		// List failed, catalog empty-and-clean: the list view error still
		// poisons the union's completeness (the union must never report a
		// partial view as complete) — surface it.
		return nil, listErr
	}
	if catalogErr != nil && len(listIDs) == 0 {
		// The catalog leg failed; the list is authoritative for enabled
		// instances but hides disabled ones — a partial view. Surface it.
		return nil, catalogErr
	}
	// Union (assert.go's unionClusterView max-count semantics): per id,
	// count = max(listCount, catalogCount) — cross-view overlap collapses
	// (an id both views serve counts ONCE), a within-one-view duplicate
	// survives at its max count. The enabled flag: list-visible ids are
	// enabled; a catalog-only id carries the catalog's flag (the disabled
	// zombie stays visible with enabled=false).
	listCounts := map[string]int{}
	catalogCounts := map[string]int{}
	for _, entry := range listIDs {
		listCounts[entry.ID]++
	}
	for _, entry := range catalogIDs {
		catalogCounts[entry.ID]++
	}
	union := map[string]remoteEntry{}
	for _, entry := range listIDs {
		union[entry.ID] = entry
	}
	for _, entry := range catalogIDs {
		if existing, ok := union[entry.ID]; !ok {
			union[entry.ID] = entry
		} else if !existing.Enabled && entry.Enabled && catalogCounts[entry.ID] <= listCounts[entry.ID] {
			// The flag of the max side wins: the catalog's enabled=true for
			// an id the list also serves (at least as many times) is the
			// same enabled id — take the enabled flag.
			union[entry.ID] = entry
		}
	}
	view := serviceView{"k8s": make([]remoteEntry, 0, len(union))}
	for id, count := range listCounts {
		if catalogCounts[id] > count {
			count = catalogCounts[id]
		}
		for i := 0; i < count; i++ {
			view["k8s"] = append(view["k8s"], union[id])
		}
	}
	for id, count := range catalogCounts {
		if listCounts[id] > 0 {
			continue // already unioned above
		}
		for i := 0; i < count; i++ {
			view["k8s"] = append(view["k8s"], union[id])
		}
	}
	sort.Slice(view["k8s"], func(i, j int) bool { return view["k8s"][i].ID < view["k8s"][j].ID })
	return view, nil
}

// listView reads the v1 instance list of one service (enabled instances
// only — the hiding view).
func (v *nacosView) listView(service string) ([]remoteEntry, error) {
	target := v.addr + "/nacos/v1/ns/instance/list?" + url.Values{
		"serviceName": {service},
		"groupName":   {nacosGroup},
		"namespaceId": {nacosNamespace},
	}.Encode()
	var body struct {
		Count int         `json:"count"`
		Hosts []nacosHost `json:"hosts"`
	}
	if err := v.getJSON(target, &body); err != nil {
		return nil, fmt.Errorf("nacos list %s: %w", service, err)
	}
	entries := make([]remoteEntry, 0, len(body.Hosts))
	for _, host := range body.Hosts {
		entries = append(entries, remoteEntry{
			ID:          domainIDOf(host),
			Enabled:     host.Enabled,
			CompositeID: host.InstanceID,
		})
	}
	return entries, nil
}

// catalogView reads the admin catalog view of one (service, cluster),
// paginated (page size 100, the production prune's own walk), tolerating
// the not-found 500 as the fully-pruned steady state.
func (v *nacosView) catalogView(service, cluster string) ([]remoteEntry, error) {
	entries := []remoteEntry{}
	for page := 1; ; page++ {
		target := v.addr + "/nacos/v1/ns/catalog/instances?" + url.Values{
			"serviceName": {service},
			"clusterName": {cluster},
			"groupName":   {nacosGroup},
			"namespaceId": {nacosNamespace},
			"pageSize":    {"100"},
			"pageNo":      {strconv.Itoa(page)},
			"hasIpCount":  {"false"},
		}.Encode()
		var body struct {
			Count int         `json:"count"`
			List  []nacosHost `json:"list"`
		}
		if err := v.getJSON(target, &body); err != nil {
			if isCatalogNotFound(err) {
				return entries, nil // no catalog entry: the fully-pruned steady state
			}
			return nil, fmt.Errorf("nacos catalog %s/%s: %w", service, cluster, err)
		}
		for _, host := range body.List {
			entries = append(entries, remoteEntry{
				ID:          domainIDOf(host),
				Enabled:     host.Enabled,
				CompositeID: host.InstanceID,
			})
		}
		if len(body.List) < 100 {
			break
		}
	}
	return entries, nil
}

// getJSON issues one GET and decodes the JSON body, classifying non-2xx
// answers (leaderless 500s wrap the leaderless marker).
func (v *nacosView) getJSON(target string, out interface{}) error {
	response, err := v.http.Get(target) //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<10))
		return classifyNacosAnswer(response.StatusCode, string(body))
	}
	return jsonDecodeBody(response.Body, out)
}

// classifyNacosAnswer renders a non-2xx answer as an error, wrapping the
// leaderless marker when the status is 5xx AND the body carries the
// Raft-unavailable signature (assert.go's classification verbatim).
func classifyNacosAnswer(statusCode int, body string) error {
	answer := fmt.Errorf("answered %d: %s", statusCode, truncateBody(body))
	if statusCode >= http.StatusInternalServerError && nacosLeaderlessBody(body) {
		return fmt.Errorf("%w: %v", errNacosLeaderless, answer)
	}
	return answer
}

// nacosLeaderlessBody matches the Raft-unavailable body signatures.
func nacosLeaderlessBody(body string) bool {
	for _, marker := range []string{"Could not find leader", "naming_persistent_service_v2", "ConsistencyException"} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// isCatalogNotFound reports whether an error carries the real-server's
// absent-pair 500 signature.
func isCatalogNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "answered 500") && strings.Contains(msg, "is not found")
}

// truncateBody bounds an answer body to a readable length.
func truncateBody(body string) string {
	const limit = 256
	if len(body) > limit {
		return body[:limit] + "..."
	}
	return body
}

// writeProbe proves nacos accepts WRITES (the leaderless window is
// read-silent — only an actual write detects it; the §4.5 environment
// classifier's periodic probe, the soak-env-probe pattern: a dedicated
// service invisible to the model).
const probeService = "obs-env-probe"

func (v *nacosView) writeProbe() error {
	target := v.addr + "/nacos/v1/ns/instance?" + url.Values{
		"serviceName": {probeService},
		"ip":          {"127.0.0.1"},
		"port":        {"39998"},
		"clusterName": {"probe"},
		"groupName":   {nacosGroup},
		"namespaceId": {nacosNamespace},
		"ephemeral":   {"false"},
		"enabled":     {"true"},
	}.Encode()
	request, err := http.NewRequest(http.MethodPost, target, nil)
	if err != nil {
		return err
	}
	response, err := v.http.Do(request) //nolint:gosec // fixed loopback URL
	if err != nil {
		return fmt.Errorf("nacos write probe: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<10))
	return classifyNacosAnswer(response.StatusCode, string(body))
}

// probeRemove deletes the probe instance (harness teardown hygiene).
func (v *nacosView) probeRemove() error {
	target := v.addr + "/nacos/v1/ns/instance?" + url.Values{
		"serviceName": {probeService},
		"ip":          {"127.0.0.1"},
		"port":        {"39998"},
		"clusterName": {"probe"},
		"groupName":   {nacosGroup},
		"namespaceId": {nacosNamespace},
		"ephemeral":   {"false"},
	}.Encode()
	request, err := http.NewRequest(http.MethodDelete, target, nil)
	if err != nil {
		return err
	}
	response, err := v.http.Do(request) //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	return nil // best-effort
}

// jsonDecodeBody decodes one JSON document from a reader.
func jsonDecodeBody(r io.Reader, out interface{}) error {
	return jsonNewDecoder(r).Decode(out)
}

const (
	nacosGroup     = "DEFAULT_GROUP"
	nacosNamespace = "public"
)

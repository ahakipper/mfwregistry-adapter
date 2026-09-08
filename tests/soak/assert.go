//go:build soak
// +build soak

package soak

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// expectedState is the churn driver's model of what Nacos must converge to:
// per app-code, the live pod names (k8s cluster) and the live consul
// service ids (ecs cluster). The standing assertion loop compares this
// model against the v1 API view every AssertEvery seconds (plan §8.5).
type expectedState struct {
	mu sync.RWMutex
	// k8s maps app-code -> live pod names (the pod name IS the instance id
	// in the k8s converter).
	k8s map[string][]string
	// consul maps app-code -> live consul service ids (the meta
	// instanceId).
	consul map[string][]string
}

func newExpectedState() *expectedState {
	return &expectedState{
		k8s:    map[string][]string{},
		consul: map[string][]string{},
	}
}

// setK8s records the live pod set of one app-code (nil value deletes).
func (e *expectedState) setK8s(appCode string, pods []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if pods == nil {
		delete(e.k8s, appCode)
		return
	}
	e.k8s[appCode] = pods
}

// setConsul records the live consul service-id set of one app-code.
func (e *expectedState) setConsul(appCode string, ids []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ids == nil {
		delete(e.consul, appCode)
		return
	}
	e.consul[appCode] = ids
}

// appCodes returns every app-code seen so far (plan §8.5: "for every
// app-code seen so far").
func (e *expectedState) appCodes() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	codes := map[string]struct{}{}
	for code := range e.k8s {
		codes[code] = struct{}{}
	}
	for code := range e.consul {
		codes[code] = struct{}{}
	}
	sorted := make([]string, 0, len(codes))
	for code := range codes {
		sorted = append(sorted, code)
	}
	sort.Strings(sorted)
	return sorted
}

// snapshot returns copies of both maps under one lock.
func (e *expectedState) snapshot() (k8s, consul map[string][]string) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	k8s = map[string][]string{}
	for code, pods := range e.k8s {
		k8s[code] = append([]string(nil), pods...)
	}
	consul = map[string][]string{}
	for code, ids := range e.consul {
		consul[code] = append([]string(nil), ids...)
	}
	return k8s, consul
}

// nacosObserver queries the Nacos v1 OpenAPI instance list of one service
// (the standing assertion's read side, plan §8.5) and derives the
// per-cluster instance-id multisets the comparison runs on.
type nacosObserver struct {
	addr string
	http *http.Client
}

func newNacosObserver(addr string) *nacosObserver {
	return &nacosObserver{
		addr: "http://" + addr,
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// nacosHost mirrors the v2.1.0 instance-list host shape (pkg/nacos client).
// Two identity fields matter to the harness: the composite instanceId
// (ip#port#cluster#group@@service — Nacos's own identity, the
// no-duplicates key) and metadata["instanceId"] (the DOMAIN instance id
// the sink round-trips: pod name for k8s, the consul meta instanceId for
// ecs — the comparison key against the expected sets).
type nacosHost struct {
	InstanceID  string            `json:"instanceId"`
	ClusterName string            `json:"clusterName"`
	ServiceName string            `json:"serviceName"`
	Metadata    map[string]string `json:"metadata"`
}

// clusterView returns the instance ids of one service grouped by cluster.
func (o *nacosObserver) clusterView(serviceName string) (map[string][]string, error) {
	target := o.addr + "/nacos/v1/ns/instance/list?" + url.Values{
		"serviceName": {serviceName},
		"groupName":   {defaultNacosGroup},
		"namespaceId": {defaultNacosNamespace},
	}.Encode()
	response, err := o.http.Get(target) //nolint:gosec // fixed loopback URL
	if err != nil {
		return nil, fmt.Errorf("nacos list %s: %w", serviceName, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		// Read and classify the answer: an HTTP 500 whose body carries the
		// naming service's Raft-unavailable signature is retryable
		// environment state, not divergence (run 20260909-0000 observed the
		// list endpoint serving while writes 500'd — the classification
		// matters for the flavor where the read itself takes the 500).
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<10))
		return nil, classifyNacosAnswer(fmt.Sprintf("nacos list %s", serviceName),
			response.StatusCode, string(body))
	}
	var body struct {
		Count int         `json:"count"`
		Hosts []nacosHost `json:"hosts"`
	}
	if err := jsonDecode(response.Body, &body); err != nil {
		return nil, fmt.Errorf("nacos list %s decode: %w", serviceName, err)
	}
	view := map[string][]string{}
	composites := map[string][]string{} // cluster -> composite ids (dup check)
	for _, host := range body.Hosts {
		domainID := host.Metadata["instanceId"]
		if domainID == "" {
			domainID = host.InstanceID // no metadata: fall back to the composite id
		}
		view[host.ClusterName] = append(view[host.ClusterName], domainID)
		composites[host.ClusterName] = append(composites[host.ClusterName], host.InstanceID)
	}
	for cluster := range view {
		sort.Strings(view[cluster])
		sort.Strings(composites[cluster])
	}
	return view, nil
}

// readiness checks the nacos console readiness endpoint.
func (o *nacosObserver) readiness() error {
	response, err := o.http.Get(o.addr + "/nacos/v1/console/health/readiness") //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("nacos readiness answered %d", response.StatusCode)
	}
	return nil
}

// nsAPIReady reports whether the v1 naming API is serving its persisted data:
// the console readiness endpoint turns 200 while the naming service is still
// rebuilding (observed on the ARM/colima stack: instance/list serves an empty
// view for minutes after a restart), so this is the stronger gate the restart
// scenario's heal clock starts from. "Serving" means the service list answers
// 200 AND reports a non-empty catalog — the derby store's data is visible
// again, so a convergence measurement from this moment measures the pipeline,
// not the JVM's rebuild.
func (o *nacosObserver) nsAPIReady() error {
	target := o.addr + "/nacos/v1/ns/service/list?" + url.Values{
		"pageNo":      {"1"},
		"pageSize":    {"1"},
		"groupName":   {defaultNacosGroup},
		"namespaceId": {defaultNacosNamespace},
	}.Encode()
	response, err := o.http.Get(target) //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("nacos ns service list answered %d", response.StatusCode)
	}
	var page struct {
		Count int      `json:"count"`
		Doms  []string `json:"doms"`
	}
	if err := jsonDecode(response.Body, &page); err != nil {
		return fmt.Errorf("nacos ns service list decode: %w", err)
	}
	if page.Count == 0 && len(page.Doms) == 0 {
		return fmt.Errorf("nacos ns service list is empty (naming service still rebuilding)")
	}
	return nil
}

// errNacosLeaderless marks a nacos answer whose body shows the naming
// service's Raft group (naming_persistent_service_v2) cannot serve
// writes. The state is read-silent: the console readiness endpoint and
// the v1 service list answer normally while every register/deregister
// returns HTTP 500 "Could not find leader : naming_persistent_service_v2"
// (run 20260909-0000: 20,962 such 500s from 00:40 onward while reads
// kept serving). The waits treat this class as retryable environment
// state — the outage window must not consume a heal bound, exactly the
// reset the read-side view errors already get.
var errNacosLeaderless = errors.New("nacos naming service Raft group unavailable (leaderless)")

// isNacosLeaderless reports whether err (or anything it wraps) carries the
// leaderless marker.
func isNacosLeaderless(err error) bool {
	return errors.Is(err, errNacosLeaderless)
}

// classifyNacosAnswer renders one non-2xx nacos answer as an error, wrapping
// errNacosLeaderless when the status is 5xx AND the body carries the
// Raft-unavailable signature — the marker the retryable handling keys on.
func classifyNacosAnswer(context string, statusCode int, body string) error {
	answer := fmt.Errorf("%s answered %d: %s", context, statusCode, truncateNacosBody(body))
	if statusCode >= http.StatusInternalServerError && nacosLeaderlessBody(body) {
		return fmt.Errorf("%w: %v", errNacosLeaderless, answer)
	}
	return answer
}

// nacosLeaderlessBody matches the body signature of the naming service's
// Raft group failing to serve writes: the explicit "Could not find leader"
// message, the Raft group's own name, or the ConsistencyException class
// (the same failure's "operation failure" flavor — the outage's first
// answer in run 20260909-0000, 00:40:10, before the could-not-find-leader
// form appears).
func nacosLeaderlessBody(body string) bool {
	for _, marker := range []string{"Could not find leader", "naming_persistent_service_v2", "ConsistencyException"} {
		if strings.Contains(body, marker) {
			return true
		}
	}
	return false
}

// truncateNacosBody bounds an answer body to a readable length.
func truncateNacosBody(body string) string {
	const limit = 512
	if len(body) > limit {
		return body[:limit] + "..."
	}
	return body
}

// probeService is the write probe's dedicated service: never part of the
// expected model, so its instance is invisible to every assertion (the
// standing checks and the scenario waits compare only modeled
// app-codes). The probe instance's composite id is stable, so repeated
// probe writes upsert one entry.
const probeService = "soak-env-probe"

// writeProbe proves nacos accepts WRITES: it registers (upserts) one probe
// instance through the same v1 endpoint and query convention the product
// sink uses (params on the query string, "ok" on success). Readiness and
// the service list answer while the Raft group is leaderless — only an
// actual write detects that state.
func (o *nacosObserver) writeProbe() error {
	return o.doProbeWrite(http.MethodPost)
}

// probeRemove deletes the probe instance: best-effort cleanup (the probe
// is invisible to the model-scoped assertions, but the harness does not
// leave litter in nacos).
func (o *nacosObserver) probeRemove() error {
	return o.doProbeWrite(http.MethodDelete)
}

// doProbeWrite issues one probe register (POST) or deregister (DELETE),
// mirroring pkg/nacos's wire form for the v1 instance endpoint.
func (o *nacosObserver) doProbeWrite(method string) error {
	target := o.addr + "/nacos/v1/ns/instance?" + probeValues().Encode()
	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		return fmt.Errorf("nacos write probe: %w", err)
	}
	response, err := o.http.Do(request) //nolint:gosec // fixed loopback URL
	if err != nil {
		return fmt.Errorf("nacos write probe: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<10))
	if err != nil {
		return fmt.Errorf("nacos write probe: read body: %w", err)
	}
	if response.StatusCode == http.StatusOK {
		return nil
	}
	return classifyNacosAnswer("nacos write probe", response.StatusCode, string(body))
}

// probeValues renders the probe instance's wire form: the same shape the
// product sink sends (service, ip, port, cluster, group, namespace,
// persistent).
func probeValues() url.Values {
	return url.Values{
		"serviceName": {probeService},
		"ip":          {"127.0.0.1"},
		"port":        {"39999"},
		"clusterName": {"probe"},
		"groupName":   {defaultNacosGroup},
		"namespaceId": {defaultNacosNamespace},
		"ephemeral":   {"false"},
		"enabled":     {"true"},
	}
}

// checkResult is one standing assertion cycle's outcome.
type checkResult struct {
	Time        time.Time
	Services    int
	Divergences int
	Detail      string
}

// divergentService is one app-code whose remote view differs from the model.
type divergentService struct {
	appCode string
	cluster string
	missing []string // expected, not remote
	extra   []string // remote, not expected
	dups    []string // duplicate composite ids (must never happen)
}

func (d divergentService) String() string {
	parts := []string{fmt.Sprintf("%s/%s:", d.appCode, d.cluster)}
	if len(d.missing) > 0 {
		parts = append(parts, fmt.Sprintf("missing %v", d.missing))
	}
	if len(d.extra) > 0 {
		parts = append(parts, fmt.Sprintf("extra %v", d.extra))
	}
	if len(d.dups) > 0 {
		parts = append(parts, fmt.Sprintf("DUPLICATES %v", d.dups))
	}
	return strings.Join(parts, " ")
}

// checkState runs one full comparison pass: for every app-code seen so far,
// fetch the Nacos view and compare the per-cluster id multisets (k8s
// cluster vs live pods, ecs cluster vs live consul entries). Duplicate
// composite ids are flagged on every pass they occur (plan §8.5: "no
// duplicate composite instance ids per (service, cluster) — ever").
//
// The comparison covers BOTH directions: clusters present in the remote
// view (remote ids vs the expected set) AND expected clusters entirely
// absent from the remote view (an expected non-empty set with no remote
// instances is a divergence — the service itself has not been pushed, or
// was pushed then wrongly removed).
func (o *nacosObserver) checkState(expected *expectedState) (int, []divergentService, error) {
	return o.checkStateScoped(expected, nil)
}

// checkStateScoped is checkState restricted to the given app-codes; a nil
// scope judges every app-code the model tracks.
func (o *nacosObserver) checkStateScoped(expected *expectedState, scope []string) (int, []divergentService, error) {
	appCodes := expected.appCodes()
	if len(scope) > 0 {
		appCodes = scope
	}
	view, err := o.fullView(appCodes)
	if err != nil {
		return 0, nil, err
	}
	divergences := []divergentService{}
	seen := map[string]map[string]bool{} // appCode -> cluster seen in the remote view
	for appCode, clusters := range view {
		if seen[appCode] == nil {
			seen[appCode] = map[string]bool{}
		}
		for cluster, remoteIDs := range clusters {
			seen[appCode][cluster] = true
			var want []string
			if cluster == "k8s" {
				want = expected.k8sFor(appCode)
			} else if cluster == "ecs" {
				want = expected.consulFor(appCode)
			} else {
				// Unknown cluster: everything in it is extra (nothing in
				// the model maps there).
			}
			missing, extra, dups := diffMultisets(want, remoteIDs)
			if len(missing) > 0 || len(extra) > 0 || len(dups) > 0 {
				divergences = append(divergences, divergentService{
					appCode: appCode, cluster: cluster, missing: missing, extra: extra, dups: dups,
				})
			}
		}
	}
	// Expected-but-absent clusters: the model wants instances where nacos
	// holds none at all.
	for _, appCode := range appCodes {
		if seen[appCode] == nil {
			seen[appCode] = map[string]bool{}
		}
		if pods := expected.k8sFor(appCode); len(pods) > 0 && !seen[appCode]["k8s"] {
			divergences = append(divergences, divergentService{
				appCode: appCode, cluster: "k8s", missing: append([]string(nil), pods...),
			})
		}
		if ids := expected.consulFor(appCode); len(ids) > 0 && !seen[appCode]["ecs"] {
			divergences = append(divergences, divergentService{
				appCode: appCode, cluster: "ecs", missing: append([]string(nil), ids...),
			})
		}
	}
	sort.Slice(divergences, func(i, j int) bool {
		if divergences[i].appCode != divergences[j].appCode {
			return divergences[i].appCode < divergences[j].appCode
		}
		return divergences[i].cluster < divergences[j].cluster
	})
	return len(view), divergences, nil
}

// k8sFor returns the expected k8s pod ids of an app-code.
func (e *expectedState) k8sFor(appCode string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.k8s[appCode]
}

// consulFor returns the expected consul service ids of an app-code.
func (e *expectedState) consulFor(appCode string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.consul[appCode]
}

// fullView fetches every service's cluster view in one pass.
func (o *nacosObserver) fullView(appCodes []string) (map[string]map[string][]string, error) {
	view := map[string]map[string][]string{}
	for _, appCode := range appCodes {
		clusters, err := o.clusterView(appCode)
		if err != nil {
			return nil, err
		}
		if len(clusters) > 0 {
			view[appCode] = clusters
		}
	}
	return view, nil
}

// diffMultisets compares the wanted and remote id multisets: missing =
// wanted-but-not-remote (count-aware), extra = remote-but-not-wanted,
// dups = ids the remote side reports more than once.
func diffMultisets(want, remote []string) (missing, extra, dups []string) {
	wantCount := map[string]int{}
	for _, id := range want {
		wantCount[id]++
	}
	remoteCount := map[string]int{}
	for _, id := range remote {
		remoteCount[id]++
	}
	for id, count := range wantCount {
		switch {
		case remoteCount[id] == 0:
			missing = append(missing, id)
		case remoteCount[id] < count:
			missing = append(missing, id)
		case remoteCount[id] > count:
			extra = append(extra, id)
		}
	}
	for id, count := range remoteCount {
		if wantCount[id] == 0 {
			extra = append(extra, id)
		}
		if count > 1 {
			dups = append(dups, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(dups)
	return missing, extra, dups
}

// soakLog is the rolling timestamped soak log (plan §8.5: every check
// writes one line — time, checked services, divergence count, worst
// divergence age).
type soakLog struct {
	mu    sync.Mutex
	file  *soakFile
	worst time.Duration
}

// newSoakLog opens (appends to) the log file; the harness owns its path.
func newSoakLog(path string) (*soakLog, error) {
	file, err := openAppend(path)
	if err != nil {
		return nil, err
	}
	return &soakLog{file: file}, nil
}

// record writes one check line and updates the worst divergence age.
func (l *soakLog) record(result checkResult, worstAge time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if worstAge > l.worst {
		l.worst = worstAge
	}
	detail := ""
	if result.Detail != "" {
		detail = " " + result.Detail
	}
	_, _ = fmt.Fprintf(l.file, "%s services=%d divergences=%d worst_age=%s%s\n",
		result.Time.Format("15:04:05"), result.Services, result.Divergences, worstAge, detail)
}

// event writes one timestamped event line (scenario markers, heals).
func (l *soakLog) event(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = fmt.Fprintf(l.file, "%s EVENT %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// close flushes and closes the log.
func (l *soakLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

const (
	defaultNacosGroup     = "DEFAULT_GROUP"
	defaultNacosNamespace = "public"
)

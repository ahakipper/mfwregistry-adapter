//go:build observe
// +build observe

package observe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
	"spotter/pkg/k8srobot"
	k8sprovider "spotter/pkg/providers/k8s"
)

const observeCommandTimeout = 30 * time.Second

// The churn vehicle (dsca-4 §4.2, the driver contract with dsca-1's kwok
// design): bare Pods applied to the kwok cluster in batched List
// manifests, labeled app-code + K8S_CLUSTER_TYPE=test, the dsca-1 pod
// spec (application container, http port, small limits, kwok
// nodeSelector/toleration) — kwok's pod-ready stage writes phase Running +
// Ready + podIP, so every applied pod is convertible by the pipeline.
//
// THE LEDGER: every applied/deleted pod is recorded with its timestamp —
// the in-flight clock the observation tolerance consumes (c(e) =
// issuedAt). Foreign changes (pods the driver did not touch) fall back to
// the pod's creationTimestamp; a remote extra with no source object AND
// no ledger entry is DIVERGENT immediately (the §3.4 fail-safe — there is
// no clock to wait for).

// churnDriver applies and deletes kwok pods, keeping the ledger.
type churnDriver struct {
	kubeconfig string
	appCodes   []string
	prefix     string

	mu              sync.Mutex
	ledger          map[string]ledgerEntry // podName -> latest mutation
	mutationJournal []ledgerEntry          // append-only per-operation evidence
	live            map[string]bool        // driver-owned objects confirmed live
	deleting        map[string]bool        // atomically claimed by one delete actor
	latestIssuedAt  time.Time              // latest per-chunk API issue clock
	mutationSeq     uint64                 // monotonic source-mutation epoch
	mutationActive  int                    // overlapping source mutation operations
	counter         int                    // monotonically increasing pod-name suffix
	applied         int                    // live pod count the driver tracks
}

// ledgerEntry is one mutation's record (the in-flight clock source).
type ledgerEntry struct {
	Op       string // "create" | "delete" | "crash" | "recover"
	PodName  string
	AppCode  string
	IssuedAt time.Time
}

// patchCrash marks a running pod as CrashLoopBackOff through the Kubernetes
// status subresource. The mutation is recorded before the API call so an
// overlapping observation tick has the same bounded in-flight clock as
// create/delete operations. Kwok preserves the rest of the pod status while
// replacing the container readiness/state fields used by conversion.go.
func (d *churnDriver) patchCrash(name string, issuedAt time.Time) error {
	return d.patchStatus(name, issuedAt, "crash", `{"status":{"phase":"Running","containerStatuses":[{"name":"application","ready":false,"state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}`)
}

// patchRecovered restores a pod to a ready Running container after a crash
// transition, again publishing the mutation clock before the status request.
func (d *churnDriver) patchRecovered(name string, issuedAt time.Time) error {
	return d.patchStatus(name, issuedAt, "recover", `{"status":{"phase":"Running","containerStatuses":[{"name":"application","ready":true,"state":{"running":{}}}]}}`)
}

func (d *churnDriver) patchStatus(name string, issuedAt time.Time, op, patch string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("patch %s requires a pod name", op)
	}
	d.mu.Lock()
	entry := d.ledger[name]
	if entry.AppCode == "" {
		entry.AppCode = d.appCodeOf(0)
	}
	d.ledger[name] = ledgerEntry{Op: op, PodName: name, AppCode: entry.AppCode, IssuedAt: issuedAt}
	d.latestIssuedAt = issuedAt
	d.mutationSeq++
	d.mutationActive++
	d.mu.Unlock()
	defer d.finishMutation()
	if _, err := d.kubectlStdin(patch, "patch", "pod", name, "--subresource=status", "--type=merge", "-p", patch); err != nil {
		return fmt.Errorf("patch pod %s status (%s): %w", name, op, err)
	}
	d.mu.Lock()
	d.mutationJournal = append(d.mutationJournal, d.ledger[name])
	d.mu.Unlock()
	return nil
}

// newChurnDriver builds the driver. appCodes is the observed service set;
// prefix namespaces this harness's pods (the run tears down by prefix).
func newChurnDriver(kubeconfig string, appCodes []string, prefix string) *churnDriver {
	codes := append([]string(nil), appCodes...)
	sort.Strings(codes)
	return &churnDriver{
		kubeconfig: kubeconfig,
		appCodes:   codes,
		prefix:     prefix,
		ledger:     map[string]ledgerEntry{},
		live:       map[string]bool{},
		deleting:   map[string]bool{},
	}
}

// kubectl runs kubectl with the kwok kubeconfig, feeding stdin.
func (d *churnDriver) kubectlStdin(stdin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), observeCommandTimeout)
	defer cancel()
	return d.kubectlStdinContext(ctx, stdin, args...)
}

func (d *churnDriver) kubectlStdinContext(ctx context.Context, stdin string, args ...string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...) //nolint:gosec // the churn tool, the soak driver's pattern
	cmd.Env = append(os.Environ(), "KUBECONFIG="+d.kubeconfig)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return out.String(), fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), ctx.Err())
		}
		return out.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, errOut.String())
	}
	return out.String(), nil
}

// observePodPrefix is the default pod/app-code prefix of this harness.
const observePodPrefix = "obs"

// appCodeOf derives app-code N's name.
func (d *churnDriver) appCodeOf(idx int) string {
	return fmt.Sprintf("%s-app-%d", d.prefix, idx)
}

// serviceNameOf derives the nacos service name of a kwok app-code (the
// converter's AppCode is the app-code label verbatim; the nacos service
// name equals it).
func serviceNameOf(appCode string) string {
	return appCode
}

// podManifest renders one kwok Pod (the dsca-1 §scale-harness spec
// verbatim: the labels conversion.go requires, the kwok
// nodeSelector/toleration, the application container with an http port,
// 100m limits so thousands schedule on the capacity-patched fake node).
func (d *churnDriver) podManifest(appCode, podName string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %s
  labels:
    app-code: %s
    app: %s
    K8S_CLUSTER_TYPE: test
spec:
  nodeSelector:
    type: kwok
  tolerations:
  - key: kwok.x-k8s.io/node
    operator: Exists
    effect: NoSchedule
  containers:
  - name: application
    image: %s/fake-app:latest
    ports:
    - name: http
      containerPort: 8080
      protocol: TCP
    resources:
      limits:
        cpu: 100m
        memory: 128Mi
`, podName, appCode, appCode, d.prefix)
}

// nextPodName derives a unique pod name.
func (d *churnDriver) nextPodName() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counter++
	return fmt.Sprintf("%s-pod-%d", d.prefix, d.counter)
}

// podListItem re-indents one standalone manifest as a List item:
// "- apiVersion: v1\n  kind: Pod\n  ..." — the shape a List's items array
// requires (a standalone manifest concatenated under items without the
// dash is "no objects passed to apply", the trap the micro-run caught).
func podListItem(manifest string) string {
	lines := strings.Split(strings.TrimRight(manifest, "\n"), "\n")
	var b strings.Builder
	for i, line := range lines {
		if i == 0 {
			b.WriteString("- " + line + "\n")
			continue
		}
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

// applyBatch applies pods in one List manifest (batched at batchSize per
// kubectl apply — dsca-1's E5/E8 measured pacing; batchSize >= n is ONE
// apply, the ~1s storm shape), recording every pod in the ledger and
// returning the applied pod names (the burst accounting's pod set). count
// pods are spread round-robin over the driver's app-codes.
func (d *churnDriver) applyBatch(count, batchSize int, issuedAt time.Time) ([]string, error) {
	if count <= 0 {
		return nil, nil
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("apply batch size must be positive")
	}
	if len(d.appCodes) == 0 {
		return nil, fmt.Errorf("apply batch requires at least one app code")
	}
	// Reserve names + app-codes first, under the lock.
	type pod struct{ name, appCode string }
	var pods []pod
	d.mu.Lock()
	for i := 0; i < count; i++ {
		d.counter++
		name := fmt.Sprintf("%s-pod-%d", d.prefix, d.counter)
		idx := d.counter % len(d.appCodes)
		if idx == 0 {
			idx = len(d.appCodes) // keep the count spread across services
		}
		if idx > len(d.appCodes) {
			idx = len(d.appCodes)
		}
		pods = append(pods, pod{name: name, appCode: d.appCodeOf(idx - 1)})
	}
	d.mutationActive++
	d.mu.Unlock()
	defer d.finishMutation()
	appliedNames := make([]string, 0, len(pods))

	for start := 0; start < len(pods); start += batchSize {
		end := start + batchSize
		if end > len(pods) {
			end = len(pods)
		}
		chunkIssued := time.Now()
		if start == 0 && !issuedAt.IsZero() {
			chunkIssued = issuedAt
		}
		// Publish this chunk's clock immediately before its API call. A large
		// operation is several serial requests; one batch-start timestamp makes
		// later chunks look older than they really are.
		d.mu.Lock()
		for _, p := range pods[start:end] {
			d.ledger[p.name] = ledgerEntry{Op: "create", PodName: p.name, AppCode: p.appCode, IssuedAt: chunkIssued}
		}
		d.latestIssuedAt = chunkIssued
		d.mutationSeq++
		d.mu.Unlock()
		var b strings.Builder
		b.WriteString("apiVersion: v1\nkind: List\nitems:\n")
		for _, p := range pods[start:end] {
			b.WriteString(podListItem(d.podManifest(p.appCode, p.name)))
		}
		if _, err := d.kubectlStdin(b.String(), "apply", "-f", "-"); err != nil {
			d.mu.Lock()
			for _, p := range pods[start:end] {
				delete(d.ledger, p.name)
			}
			d.mu.Unlock()
			sort.Strings(appliedNames)
			return appliedNames, fmt.Errorf("apply batch (%d pods): %w", end-start, err)
		}
		d.mu.Lock()
		for _, p := range pods[start:end] {
			d.live[p.name] = true
			d.applied++
			d.mutationJournal = append(d.mutationJournal, d.ledger[p.name])
			appliedNames = append(appliedNames, p.name)
		}
		d.mu.Unlock()
	}
	sort.Strings(appliedNames)
	return appliedNames, nil
}

// deletePods deletes the named pods, recording each successful source
// mutation in the ledger. The returned count is deliberately separate from
// len(names): a duplicate/no-op request is not a successful mutation and must
// never be reported as one by the harness.
func (d *churnDriver) deletePods(names []string, issuedAt time.Time) (int, error) {
	if len(names) == 0 {
		return 0, nil
	}
	// Ignore duplicate rollback attempts for objects already deleted by another
	// harness actor. `--ignore-not-found` succeeds for them, but no source
	// mutation occurred and the evidence journal must not invent one.
	d.mu.Lock()
	actual := make([]string, 0, len(names))
	for _, name := range names {
		if d.live[name] && !d.deleting[name] {
			actual = append(actual, name)
			d.deleting[name] = true
		}
	}
	if len(actual) == 0 {
		d.mu.Unlock()
		return 0, nil
	}
	d.mutationActive++
	d.mu.Unlock()
	defer d.finishMutation()
	deleted := 0
	for start := 0; start < len(actual); start += 100 {
		end := start + 100
		if end > len(actual) {
			end = len(actual)
		}
		chunkIssued := time.Now()
		if start == 0 && !issuedAt.IsZero() {
			chunkIssued = issuedAt
		}
		d.mu.Lock()
		for _, name := range actual[start:end] {
			appCode := d.ledger[name].AppCode
			if appCode == "" {
				appCode = d.appCodeOf(0)
			}
			d.ledger[name] = ledgerEntry{Op: "delete", PodName: name, AppCode: appCode, IssuedAt: chunkIssued}
		}
		d.latestIssuedAt = chunkIssued
		d.mutationSeq++
		d.mu.Unlock()
		args := append([]string{"delete", "pod", "--ignore-not-found=true", "--wait=false", "--"}, actual[start:end]...)
		if _, err := d.kubectlStdin("", args...); err != nil {
			d.mu.Lock()
			for _, name := range actual[start:] {
				delete(d.deleting, name)
			}
			d.mu.Unlock()
			return deleted, fmt.Errorf("delete pods (%d): %w", end-start, err)
		}
		d.mu.Lock()
		for _, name := range actual[start:end] {
			delete(d.live, name)
			delete(d.deleting, name)
			d.applied--
			d.mutationJournal = append(d.mutationJournal, d.ledger[name])
		}
		d.mu.Unlock()
		deleted += end - start
	}
	return deleted, nil
}

// ledgerLookup returns the latest ledger entry of a pod (ok=false when the
// driver never touched it).
func (d *churnDriver) ledgerLookup(podName string) (ledgerEntry, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry, ok := d.ledger[podName]
	return entry, ok
}

// ledgerSnapshot copies the whole ledger (the summary's convergence
// accounting).
func (d *churnDriver) ledgerSnapshot() map[string]ledgerEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]ledgerEntry, len(d.ledger))
	for name, entry := range d.ledger {
		out[name] = entry
	}
	return out
}

func (d *churnDriver) mutationJournalSince(start time.Time) []ledgerEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]ledgerEntry, 0, len(d.mutationJournal))
	for _, entry := range d.mutationJournal {
		if !entry.IssuedAt.Before(start) {
			out = append(out, entry)
		}
	}
	return out
}

// mutationSequence returns the latest source mutation epoch. It advances
// before an apply/delete API call so a tick overlapping the request is
// treated as transitional even if the API response is still propagating.
func (d *churnDriver) mutationSequence() uint64 {
	seq, _ := d.mutationState()
	return seq
}

func (d *churnDriver) mutationState() (uint64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.mutationSeq, d.mutationActive > 0
}

func (d *churnDriver) latestMutationIssuedAt() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.latestIssuedAt
}

func (d *churnDriver) finishMutation() {
	d.mu.Lock()
	if d.mutationActive > 0 {
		d.mutationActive--
	}
	d.mu.Unlock()
}

// sourcePod is one live pod in the converter's terms.
type sourcePod struct {
	Name            string
	AppCode         string
	CreatedAt       time.Time
	Phase           string
	PodIP           string
	ContainersReady bool
	Instance        *instance.Instance
}

// liveSourcePods reads the kwok cluster's pods in ONE kubectl call (the
// §4.3 step-1 read), converting to the observation's source model: pods
// of the harness's app-codes with their converter-relevant status.
//
// The Pending filter (dsca-4 batch-D contract): a Pending pod is filtered
// from the expected set (InitInstanceFilters drops InstanceStatePending),
// so it is absent from nacos by design until it becomes Running; its
// creationTimestamp is still the in-flight clock once it flips Running.
func (d *churnDriver) liveSourcePods(labelSelector string) ([]sourcePod, error) {
	out, err := d.kubectlStdin("", "get", "pods", "-l", labelSelector, "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []v1.Pod `json:"items"`
	}
	if err := jsonUnmarshalString(out, &list); err != nil {
		return nil, fmt.Errorf("decode kubectl get pods: %w", err)
	}
	// The harness's own app-code set scopes the observation: a foreign
	// pod (none expected — the kwok cluster is dedicated) would surface as
	// an unexpected app-code, recorded in the tick.
	pods := make([]sourcePod, 0, len(list.Items))
	clusterID := d.sourceClusterID()
	for i := range list.Items {
		item := &list.Items[i]
		obj := &k8srobot.QueueObject{
			RType: k8srobot.Pods, Key: item.Namespace + "/" + item.Name,
			ClusterID: clusterID, UID: string(item.UID),
		}
		converted := k8sprovider.ConvertPod(obj, item, nil, ports.NopLogger{})
		appCode := item.Labels["app-code"]
		if converted != nil && converted.AppCode != "" {
			appCode = converted.AppCode
		}
		pods = append(pods, sourcePod{
			Name: item.Name, AppCode: appCode, CreatedAt: item.CreationTimestamp.Time,
			Phase: string(item.Status.Phase), PodIP: item.Status.PodIP,
			ContainersReady: containersReadyOf(item), Instance: converted,
		})
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
	return pods, nil
}

// sourceClusterID mirrors k8srobot's deterministic identity for a configured
// cluster. The Observe source must use the same value in sourceKey/sourceCluster
// labels that the production informer conversion writes.
func (d *churnDriver) sourceClusterID() string {
	path, err := filepath.Abs(d.kubeconfig)
	if err != nil {
		path = filepath.Clean(d.kubeconfig)
	}
	sum := sha256.Sum256([]byte(path))
	return fmt.Sprintf("config-%x", sum[:8])
}

// containersReadyOf derives the converter's containersReady predicate
// (every container ready AND a non-empty ContainerStatuses report).
func containersReadyOf(item *v1.Pod) bool {
	if item == nil || len(item.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, c := range item.Status.ContainerStatuses {
		if !c.Ready || c.State.Running == nil {
			return false
		}
	}
	return true
}

// parseK8sTimestamp parses a k8s RFC3339 creationTimestamp (zero on
// failure — the foreign clock degrades to "unknown", which the diff
// treats conservatively: no clock means no tolerance).
func parseK8sTimestamp(raw string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}
	}
	return ts
}

// parseK8sTimestampOr parses with a fallback format (kubectl emits
// RFC3339 always; kept for robustness).
func parseK8sTimestampOr(raw string, layout string) time.Time {
	if raw == "" {
		return time.Time{}
	}
	if ts, err := time.Parse(layout, raw); err == nil {
		return ts
	}
	return parseK8sTimestamp(raw)
}

// listPodNames returns the harness-prefixed pod names currently on the
// cluster (teardown bookkeeping).
func (d *churnDriver) listPodNames() ([]string, error) {
	out, err := d.kubectlStdin("", "get", "pods", "-l", "app-code", "-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil, err
	}
	names := []string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line != "" {
			names = append(names, line)
		}
	}
	return names, nil
}

// deleteAll removes every harness-prefixed pod (the run's teardown; not
// the ledger-tracked churn).
func (d *churnDriver) deleteAll() error {
	names, err := d.listPodNames()
	if err != nil {
		return err
	}
	filtered := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasPrefix(name, d.prefix+"-pod-") {
			filtered = append(filtered, name)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	// Keep teardown requests bounded just like applyBatch. Passing hundreds of
	// pod names in one kubectl invocation can exceed the apiserver/client
	// deadline on kwok, leaving a partial population that poisons the next
	// observation run. Chunks remain name-filtered so foreign pods are never
	// deleted.
	for start := 0; start < len(filtered); start += 100 {
		end := start + 100
		if end > len(filtered) {
			end = len(filtered)
		}
		args := append([]string{"delete", "pod", "--ignore-not-found=true", "--wait=false", "--"}, filtered[start:end]...)
		if _, err := d.kubectlStdin("", args...); err != nil {
			return fmt.Errorf("delete pods teardown chunk (%d pods): %w", end-start, err)
		}
	}
	return nil
}

// formatCount renders a count for logs.
func formatCount(n int) string {
	return strconv.Itoa(n)
}

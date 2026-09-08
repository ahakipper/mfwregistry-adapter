//go:build soak
// +build soak

package soak

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// k8sDriver drives the k3s side of the churn: deployments with labels
// app-code=<code> and K8S_CLUSTER_TYPE=test (the underscore key the k8s
// converter reads, plan §8.2), which produce convertible instances
// (InstanceId = pod name, AppCode from labels).
type k8sDriver struct {
	kubeconfig string
}

// k8sDeploymentManifest renders a minimal deployment whose pods satisfy the
// converter: labels app-code + K8S_CLUSTER_TYPE (underscore!), a busybox
// container sleeping forever, and readiness so the pods reach Online (the
// converter maps Ready containers to status 1).
const k8sDeploymentManifest = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: default
  labels:
    app-code: %s
spec:
  replicas: %d
  selector:
    matchLabels:
      app-code: %s
  template:
    metadata:
      labels:
        app-code: %s
        K8S_CLUSTER_TYPE: test
    spec:
      containers:
      - name: application
        image: busybox:1.36
        command: ["sh", "-c", "sleep 3600"]
        readinessProbe:
          exec:
            command: ["true"]
          initialDelaySeconds: 1
          periodSeconds: 5
`

// kubectl runs kubectl with the harness kubeconfig.
func (d *k8sDriver) kubectl(args ...string) (string, error) {
	return d.kubectlStdin("", args...)
}

// kubectlStdin runs kubectl feeding stdin (used by apply -f -).
func (d *k8sDriver) kubectlStdin(stdin string, args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...) //nolint:gosec // kubectl is the churn tool of plan §8.1
	cmd.Env = append(os.Environ(), "KUBECONFIG="+d.kubeconfig)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, errOut.String())
	}
	return out.String(), nil
}

// apply creates or scales a deployment to replicas pods.
func (d *k8sDriver) apply(appCode string, replicas int) error {
	manifest := fmt.Sprintf(k8sDeploymentManifest, deploymentName(appCode), appCode, replicas, appCode, appCode)
	if _, err := d.kubectlStdin(manifest, "apply", "-f", "-"); err != nil {
		return fmt.Errorf("apply %s x%d: %w", appCode, replicas, err)
	}
	return nil
}

// scale sets the replica count of a deployment.
func (d *k8sDriver) scale(appCode string, replicas int) error {
	if _, err := d.kubectl("scale", "deployment", deploymentName(appCode), "--replicas", fmt.Sprint(replicas)); err != nil {
		return fmt.Errorf("scale %s x%d: %w", appCode, replicas, err)
	}
	return nil
}

// delete removes the deployment.
func (d *k8sDriver) delete(appCode string) error {
	if _, err := d.kubectl("delete", "deployment", deploymentName(appCode), "--ignore-not-found=true"); err != nil {
		return fmt.Errorf("delete %s: %w", appCode, err)
	}
	return nil
}

// podReport is one row of kubectl get pods -o json.
type podReport struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		Phase string `json:"phase"`
	} `json:"status"`
}

// livePods returns the running pod names of a deployment, once they have IP
// addresses (the converter needs an IP for Online instances; pods without
// one are not yet representable). Bounded wait for pods to appear.
func (d *k8sDriver) livePods(appCode string, bound time.Duration) ([]string, error) {
	deadline := time.Now().Add(bound)
	var lastErr error
	var last []string
	for time.Now().Before(deadline) {
		out, err := d.kubectl("get", "pods", "-l", "app-code="+appCode,
			"-o", "jsonpath={range .items[*]}{.metadata.name} {.status.phase} {.status.podIP}{\"\\n\"}{end}")
		if err == nil {
			names := []string{}
			for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
				fields := strings.Fields(line)
				if len(fields) == 3 && fields[1] == "Running" && fields[2] != "" && fields[2] != "<none>" {
					names = append(names, fields[0])
				}
			}
			sort.Strings(names)
			last = names
			if len(names) > 0 {
				return names, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return last, nil
}

// livePodsStable is livePods with a quiescence requirement: it returns once
// the pod set has been observed unchanged for stableFor and reached
// minCount (the batch scenario needs the FULL replica set, not the first
// pod that appears). Bounded by bound overall.
func (d *k8sDriver) livePodsStable(appCode string, bound, stableFor time.Duration, minCount int) ([]string, error) {
	deadline := time.Now().Add(bound)
	var previous []string
	stableSince := time.Time{}
	for time.Now().Before(deadline) {
		pods, err := d.livePods(appCode, 5*time.Second)
		if err != nil {
			return nil, err
		}
		if len(pods) >= minCount && equalStrings(pods, previous) {
			if stableSince.IsZero() {
				stableSince = time.Now()
			}
			if time.Since(stableSince) >= stableFor {
				return pods, nil
			}
		} else {
			stableSince = time.Time{}
		}
		previous = pods
		time.Sleep(2 * time.Second)
	}
	return previous, nil
}

// equalStrings compares two sorted slices.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// deploymentName derives the deployment name of an app-code. The pod name
// is the instance id, and the deployment name must be a valid DNS label.
func deploymentName(appCode string) string {
	return "soak-" + strings.ReplaceAll(appCode, "_", "-")
}

// consulDriver drives the consul side of the churn over the HTTP API on
// 18500, with the FULL meta schema the converter requires (plan §8.2):
// Node{Node, Address} + Service{ID, Service, Port, Tags: [microservice],
// Meta{appCode, envType, envGroup, instanceId, version, ports-JSON}} —
// otherwise InitInstanceFilters drops the entry and the soak sees nothing.
type consulDriver struct {
	addr string
	http *http.Client
}

func newConsulDriver(addr string) *consulDriver {
	return &consulDriver{
		addr: "http://" + addr,
		http: &http.Client{Timeout: 10 * time.Second},
	}
}

// consulRegistration is the /v1/catalog/register body.
type consulRegistration struct {
	Node    string           `json:"Node"`
	Address string           `json:"Address"`
	Service consulServiceReg `json:"Service"`
	Check   consulCheckReg   `json:"Check"`
}

type consulServiceReg struct {
	ID      string            `json:"ID"`
	Service string            `json:"Service"`
	Port    int               `json:"Port"`
	Tags    []string          `json:"Tags"`
	Meta    map[string]string `json:"Meta"`
}

type consulCheckReg struct {
	Node      string `json:"Node"`
	CheckID   string `json:"CheckID"`
	Name      string `json:"Name"`
	Status    string `json:"Status"`
	ServiceID string `json:"ServiceID"`
}

// consulMeta renders the meta map the converter reads: appCode, envType,
// envGroup, instanceId, version and the ports JSON (plan §8.2, the
// convertion.go meta keys).
func consulMeta(appCode, instanceID, envGroup string, port int) map[string]string {
	portsJSON := fmt.Sprintf(`[{"name":"http","protocol":"http","port":%d,"servicePort":%d}]`, port, port)
	return map[string]string{
		"appCode":    appCode,
		"envType":    "test",
		"envGroup":   envGroup,
		"instanceId": instanceID,
		"version":    "1.0.0",
		"ports":      portsJSON,
	}
}

// register registers one consul service entry with a passing health check
// (the converter reads the check state; without a passing check the entry
// converts to status 2, not 1).
func (d *consulDriver) register(appCode, instanceID string, port int) error {
	node := "soak-node-" + appCode
	body := consulRegistration{
		Node:    node,
		Address: "127.0.0.10",
		Service: consulServiceReg{
			ID:      instanceID,
			Service: appCode,
			Port:    port,
			Tags:    []string{"microservice"},
			Meta:    consulMeta(appCode, instanceID, "main", port),
		},
		Check: consulCheckReg{
			Node:      node,
			CheckID:   "service:" + appCode,
			Name:      "soak-check-" + appCode,
			Status:    "passing",
			ServiceID: instanceID,
		},
	}
	return d.do("PUT", "/v1/catalog/register", body)
}

// deregister removes a node's service entries (deregister by node covers
// every service of the node; the churn uses one service per node).
func (d *consulDriver) deregister(appCode string) error {
	body := map[string]string{
		"Node": "soak-node-" + appCode,
	}
	return d.do("PUT", "/v1/catalog/deregister", body)
}

// deregisterService removes exactly one service id from its node (the
// churn's rotation path: replace instance N with N+1 without dropping the
// node's other entries).
func (d *consulDriver) deregisterService(appCode, serviceID string) error {
	body := map[string]string{
		"Node":      "soak-node-" + appCode,
		"ServiceID": serviceID,
	}
	return d.do("PUT", "/v1/catalog/deregister", body)
}

// liveServiceIDs returns the passing instance IDs of one app-code from the
// health endpoint — the expected-set source for assertions.
func (d *consulDriver) liveServiceIDs(appCode string) ([]string, error) {
	var entries []struct {
		Service struct {
			ID   string            `json:"ID"`
			Meta map[string]string `json:"Meta"`
		} `json:"Service"`
	}
	if err := d.getJSON("/v1/health/service/"+appCode+"?passing=true", &entries); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Service.Meta["appCode"] == appCode {
			ids = append(ids, e.Service.ID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// healthy reports whether the consul agent answers (the leader endpoint
// returns a bare JSON string like "127.0.0.1:8300").
func (d *consulDriver) healthy() error {
	var leader string
	if err := d.getJSON("/v1/status/leader", &leader); err != nil {
		return err
	}
	if leader == "" {
		return fmt.Errorf("consul leader is empty")
	}
	return nil
}

func (d *consulDriver) do(method, path string, body interface{}) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(method, d.addr+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := d.http.Do(request)
	if err != nil {
		return fmt.Errorf("consul %s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return fmt.Errorf("consul %s %s answered %d", method, path, response.StatusCode)
	}
	return nil
}

func (d *consulDriver) getJSON(path string, out interface{}) error {
	response, err := d.http.Get(d.addr + path) //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return fmt.Errorf("consul GET %s answered %d", path, response.StatusCode)
	}
	return json.NewDecoder(response.Body).Decode(out)
}

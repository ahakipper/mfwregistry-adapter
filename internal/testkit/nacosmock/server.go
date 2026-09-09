// Package nacosmock provides a small in-process Nacos v1 OpenAPI server for
// tests, mirroring the consulmock pattern (an httptest server plus
// observation and failure-injection controls).
package nacosmock

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"
)

// instanceIDFormat is the composite instance id format of Nacos 2.1.0:
// ip#port#cluster#group@@service (plan §7.1, F0-verified).
const instanceIDFormat = "%s#%d#%s#%s@@%s"

// defaultGroupName is the group assumed when a request omits groupName, the
// same default the real Nacos applies.
const defaultGroupName = "DEFAULT_GROUP"

// Request is an immutable snapshot of a request received by Server.
type Request struct {
	Method string
	Path   string
	Query  url.Values
}

// Host is one instance as the /nacos/v1/ns/instance/list response serves it
// (the F0-observed v2.1.0 hosts shape). SetInstances accepts these directly.
type Host struct {
	InstanceID  string            `json:"instanceId"`
	IP          string            `json:"ip"`
	Port        int               `json:"port"`
	Weight      float64           `json:"weight"`
	Healthy     bool              `json:"healthy"`
	Enabled     bool              `json:"enabled"`
	Ephemeral   bool              `json:"ephemeral"`
	ClusterName string            `json:"clusterName"`
	ServiceName string            `json:"serviceName"`
	Metadata    map[string]string `json:"metadata"`
}

// Instance is one stored instance, the state form of Host. Metadata is never
// nil after register or SetInstances.
type Instance struct {
	InstanceID  string
	IP          string
	Port        int
	Enabled     bool
	Ephemeral   bool
	ClusterName string
	GroupName   string
	ServiceName string
	Metadata    map[string]string
}

// CompositeID returns the composite instance id of the instance: the exact
// string the list response reports in Host.InstanceID.
func (i *Instance) CompositeID() string {
	return fmt.Sprintf(instanceIDFormat, i.IP, i.Port, i.ClusterName, i.GroupName, i.ServiceName)
}

// host renders the instance in the list-response shape.
func (i *Instance) host() Host {
	return Host{
		InstanceID:  i.InstanceID,
		IP:          i.IP,
		Port:        i.Port,
		Weight:      1.0,
		Healthy:     true,
		Enabled:     i.Enabled,
		Ephemeral:   i.Ephemeral,
		ClusterName: i.ClusterName,
		ServiceName: i.ServiceName,
		Metadata:    cloneStringMap(i.Metadata),
	}
}

// fromHost rebuilds an Instance from a Host-shaped value (SetInstances),
// filling the group and service and deriving the composite id.
func fromHost(group, service string, host Host) *Instance {
	instance := &Instance{
		InstanceID:  host.InstanceID,
		IP:          host.IP,
		Port:        host.Port,
		Enabled:     host.Enabled,
		Ephemeral:   host.Ephemeral,
		ClusterName: host.ClusterName,
		GroupName:   group,
		ServiceName: service,
		Metadata:    cloneStringMap(host.Metadata),
	}
	if instance.ClusterName == "" {
		instance.ClusterName = "DEFAULT"
	}
	if instance.InstanceID == "" {
		instance.InstanceID = instance.CompositeID()
	}
	return instance
}

// Server is a thread-safe, minimal Nacos v1 OpenAPI server for tests.
//
// Endpoints: POST/DELETE /nacos/v1/ns/instance (register/deregister),
// GET /nacos/v1/ns/instance/list (per-service view query, hides disabled
// instances like the real one), GET /nacos/v1/ns/catalog/instances (the
// admin catalog view incl. disabled instances, paginated),
// GET /nacos/v1/ns/service/list (paginated service names) and
// GET /nacos/v1/console/health/readiness. All parameters are form values
// URL-encoded on the query string, exactly like the real v1 API.
type Server struct {
	mu        sync.RWMutex
	server    *httptest.Server
	instances map[string]*Instance // keyed by composite id
	injected  int                  // HTTP status injected on every endpoint; 0 = off
	delay     time.Duration        // injected per-request delay
	requests  []Request
	closeOnce sync.Once
}

// Start starts a Nacos mock on a loopback-only HTTP listener.
func Start() *Server {
	s := &Server{instances: make(map[string]*Instance)}
	s.server = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

// URL returns the server URL in http://127.0.0.1:<port> form.
func (s *Server) URL() string {
	return s.server.URL
}

// Address returns the server URL, the value the Nacos adapter accepts as its
// base address.
func (s *Server) Address() string {
	return s.URL()
}

// SetInstances replaces the state of one service, group and CLUSTER with
// the given hosts: the out-of-band drift control (plan §7.5), seeding remote
// instances the adapter never pushed. The cluster scoping lets a test seed
// several clusters of one service independently (the provider collision
// shape); other services and clusters are untouched.
func (s *Server) SetInstances(hosts []Host, group, service string, cluster string) {
	stored := make(map[string]*Instance, len(hosts))
	for _, host := range hosts {
		host.ClusterName = cluster
		instance := fromHost(group, service, host)
		stored[instance.InstanceID] = instance
	}

	s.mu.Lock()
	for id, instance := range s.instances {
		if instance.GroupName == group && instance.ServiceName == service && instance.ClusterName == cluster {
			delete(s.instances, id)
		}
	}
	for id, instance := range stored {
		s.instances[id] = instance
	}
	s.mu.Unlock()
}

// Instances returns a snapshot of the stored instances of one service and
// cluster (a nil/empty cluster means every cluster of the service), in
// composite-id order for deterministic assertions.
func (s *Server) Instances(service, cluster string) []Instance {
	s.mu.RLock()
	snapshot := make([]Instance, 0, len(s.instances))
	for _, instance := range s.instances {
		if instance.ServiceName != service {
			continue
		}
		if cluster != "" && instance.ClusterName != cluster {
			continue
		}
		snapshot = append(snapshot, *instance)
	}
	s.mu.RUnlock()

	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].InstanceID < snapshot[j].InstanceID
	})
	for i := range snapshot {
		snapshot[i].Metadata = cloneStringMap(snapshot[i].Metadata)
	}
	return snapshot
}

// Requests returns independent snapshots of all received requests.
func (s *Server) Requests() []Request {
	s.mu.RLock()
	requests := cloneRequests(s.requests)
	s.mu.RUnlock()
	return requests
}

// SetStatus injects an HTTP status returned by every endpoint. 0 restores
// normal behavior (plan §7.5 failure injection).
func (s *Server) SetStatus(status int) {
	s.mu.Lock()
	s.injected = status
	s.mu.Unlock()
}

// SetDelay injects a delay before every response, for client timeout tests.
func (s *Server) SetDelay(delay time.Duration) {
	s.mu.Lock()
	s.delay = delay
	s.mu.Unlock()
}

// Close stops the server. It is safe to call more than once.
func (s *Server) Close() {
	s.closeOnce.Do(s.server.Close)
}

func (s *Server) serveHTTP(w http.ResponseWriter, request *http.Request) {
	s.record(request)

	s.mu.RLock()
	injected := s.injected
	delay := s.delay
	s.mu.RUnlock()

	if delay > 0 {
		time.Sleep(delay)
	}
	if injected != 0 {
		writeJSON(w, injected, map[string]string{"error": fmt.Sprintf("injected status %d", injected)})
		return
	}

	switch {
	case request.URL.Path == "/nacos/v1/ns/instance":
		s.serveInstance(w, request)
	case request.URL.Path == "/nacos/v1/ns/instance/list":
		s.serveInstanceList(w, request)
	case request.URL.Path == "/nacos/v1/ns/catalog/instances":
		s.serveCatalogInstances(w, request)
	case request.URL.Path == "/nacos/v1/ns/service/list":
		s.serveServiceList(w, request)
	case request.URL.Path == "/nacos/v1/console/health/readiness":
		writeJSON(w, http.StatusOK, map[string]string{"status": "UP"})
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

// serveInstance handles POST (register) and DELETE (deregister). Register is
// an upsert keyed by the composite id: registering the same id twice leaves
// exactly one entry, so the no-duplicate-ids invariant is assertable.
func (s *Server) serveInstance(w http.ResponseWriter, request *http.Request) {
	values := request.URL.Query()
	if request.Method == http.MethodPost {
		instance, err := parseInstance(values)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		s.mu.Lock()
		s.instances[instance.InstanceID] = instance
		s.mu.Unlock()
		writeOK(w)
		return
	}
	if request.Method == http.MethodDelete {
		id, err := compositeIDFromValues(values)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		s.mu.Lock()
		delete(s.instances, id)
		s.mu.Unlock()
		writeOK(w)
		return
	}
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}

// serveInstanceList handles GET /nacos/v1/ns/instance/list: every stored
// instance of (serviceName, groupName), across clusters. Instances with
// Enabled==false are SKIPPED, mirroring the real Nacos 2.1.0 view filter
// (nacos source: `if (!ip.isEnabled()) continue;`) — the fidelity gap whose
// absence let the F8 prune blind spot hide from every mock-driven test.
func (s *Server) serveInstanceList(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	values := request.URL.Query()
	service := values.Get("serviceName")
	if service == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "serviceName is required"})
		return
	}
	group := values.Get("groupName")
	if group == "" {
		group = defaultGroupName
	}

	s.mu.RLock()
	hosts := make([]Host, 0, len(s.instances))
	for _, instance := range s.instances {
		if instance.ServiceName != service || instance.GroupName != group {
			continue
		}
		if !instance.Enabled {
			continue // real nacos hides disabled instances from this view
		}
		hosts = append(hosts, instance.host())
	}
	s.mu.RUnlock()

	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].InstanceID < hosts[j].InstanceID
	})
	writeJSON(w, http.StatusOK, map[string]interface{}{"count": len(hosts), "hosts": hosts})
}

// serveCatalogInstances handles GET /nacos/v1/ns/catalog/instances: the
// ADMIN catalog view of (serviceName, clusterName, groupName) — the listing
// the F8 prune reads. Unlike instance/list, the catalog serves disabled
// (Enabled==false) instances too, so drift the view filter hides stays
// reachable. pageNo/pageSize slice a composite-id-sorted host list, count
// carries the total, list the page.
//
// A missing serviceName or clusterName answers 400: the real Nacos answers
// 500 in this case (RaftStorage persistence rejects the missing selector),
// but a mock 500 would collide with the failure-injection semantics and
// with the not-found tolerance the prune builds on top of *APIError, so the
// mock mirrors the same rejection with a client-class status.
func (s *Server) serveCatalogInstances(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	values := request.URL.Query()
	service := values.Get("serviceName")
	cluster := values.Get("clusterName")
	if service == "" || cluster == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "serviceName and clusterName are required"})
		return
	}
	pageNo, err := strconv.Atoi(values.Get("pageNo"))
	if err != nil || pageNo < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pageNo is required"})
		return
	}
	pageSize, err := strconv.Atoi(values.Get("pageSize"))
	if err != nil || pageSize < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pageSize is required"})
		return
	}
	group := values.Get("groupName")
	if group == "" {
		group = defaultGroupName
	}

	s.mu.RLock()
	hosts := make([]Host, 0, len(s.instances))
	for _, instance := range s.instances {
		if instance.ServiceName != service || instance.GroupName != group || instance.ClusterName != cluster {
			continue
		}
		hosts = append(hosts, instance.host())
	}
	s.mu.RUnlock()

	sort.Slice(hosts, func(i, j int) bool {
		return hosts[i].InstanceID < hosts[j].InstanceID
	})

	start := (pageNo - 1) * pageSize
	end := start + pageSize
	if start > len(hosts) {
		start = len(hosts)
	}
	if end > len(hosts) {
		end = len(hosts)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count": len(hosts),
		"list":  hosts[start:end],
	})
}

// serveServiceList handles GET /nacos/v1/ns/service/list with real pagination
// semantics: sorted service names of the group, pageNo/pageSize slicing, and
// a short (or empty) page past the end instead of an error.
func (s *Server) serveServiceList(w http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	values := request.URL.Query()
	pageNo, err := strconv.Atoi(values.Get("pageNo"))
	if err != nil || pageNo < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pageNo is required"})
		return
	}
	pageSize, err := strconv.Atoi(values.Get("pageSize"))
	if err != nil || pageSize < 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pageSize is required"})
		return
	}
	group := values.Get("groupName")
	if group == "" {
		group = defaultGroupName
	}

	s.mu.RLock()
	names := make([]string, 0, len(s.instances))
	seen := make(map[string]struct{}, len(s.instances))
	for _, instance := range s.instances {
		if instance.GroupName != group {
			continue
		}
		if _, ok := seen[instance.ServiceName]; ok {
			continue
		}
		seen[instance.ServiceName] = struct{}{}
		names = append(names, instance.ServiceName)
	}
	s.mu.RUnlock()
	sort.Strings(names)

	start := (pageNo - 1) * pageSize
	end := start + pageSize
	if start > len(names) {
		start = len(names)
	}
	if end > len(names) {
		end = len(names)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"count": len(names),
		"doms":  names[start:end],
	})
}

func (s *Server) record(request *http.Request) {
	snapshot := Request{
		Method: request.Method,
		Path:   request.URL.Path,
		Query:  cloneValues(request.URL.Query()),
	}
	s.mu.Lock()
	s.requests = append(s.requests, snapshot)
	s.mu.Unlock()
}

// parseInstance validates the register form and builds the stored instance
// with its composite id.
func parseInstance(values url.Values) (*Instance, error) {
	service := values.Get("serviceName")
	ip := values.Get("ip")
	cluster := values.Get("clusterName")
	if service == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	if ip == "" {
		return nil, fmt.Errorf("ip is required")
	}
	if cluster == "" {
		return nil, fmt.Errorf("clusterName is required")
	}
	port, err := strconv.Atoi(values.Get("port"))
	if err != nil || port < 0 {
		return nil, fmt.Errorf("port %q is not a non-negative integer", values.Get("port"))
	}
	group := values.Get("groupName")
	if group == "" {
		group = defaultGroupName
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	metadata := map[string]string{}
	if raw := values.Get("metadata"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			return nil, fmt.Errorf("metadata is not a JSON map: %v", err)
		}
	}
	ephemeral := values.Get("ephemeral") == "true"
	instance := &Instance{
		InstanceID:  "",
		IP:          ip,
		Port:        port,
		Enabled:     values.Get("enabled") != "false",
		Ephemeral:   ephemeral,
		ClusterName: cluster,
		GroupName:   group,
		ServiceName: service,
		Metadata:    metadata,
	}
	instance.InstanceID = instance.CompositeID()
	return instance, nil
}

// compositeIDFromValues validates the deregister form and returns the
// addressed composite id.
func compositeIDFromValues(values url.Values) (string, error) {
	instance, err := parseInstance(values)
	if err != nil {
		return "", err
	}
	return instance.InstanceID, nil
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func cloneRequests(requests []Request) []Request {
	cloned := make([]Request, len(requests))
	for i, request := range requests {
		cloned[i] = Request{
			Method: request.Method,
			Path:   request.Path,
			Query:  cloneValues(request.Query),
		}
	}
	return cloned
}

func cloneValues(values url.Values) url.Values {
	cloned := make(url.Values, len(values))
	for key, value := range values {
		cloned[key] = append([]string(nil), value...)
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

// instanceListResponse is the /nacos/v1/ns/instance/list response shape.
type instanceListResponse struct {
	Count int    `json:"count"`
	Hosts []Host `json:"hosts"`
}

// catalogInstancesResponse is the /nacos/v1/ns/catalog/instances response
// shape: the page field is `list`, not `hosts` (verified against Nacos
// 2.1.0 — the shape the production client's CatalogPage decodes).
type catalogInstancesResponse struct {
	Count int    `json:"count"`
	List  []Host `json:"list"`
}

// serviceListResponse is the /nacos/v1/ns/service/list response shape.
type serviceListResponse struct {
	Count int      `json:"count"`
	Doms  []string `json:"doms"`
}

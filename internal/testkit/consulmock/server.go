// Package consulmock provides a small in-process Consul HTTP API for tests.
package consulmock

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/hashicorp/consul/api"
)

const defaultLeader = "127.0.0.1:8300"

// Request is an immutable snapshot of a request received by Server.
type Request struct {
	Method string
	Path   string
	Query  url.Values
}

// Server is a thread-safe, minimal Consul HTTP API server for tests.
type Server struct {
	mu          sync.RWMutex
	server      *httptest.Server
	leader      string
	services    []byte
	entries     map[string][]byte
	healthState []byte
	index       uint64
	requests    []Request
	closeOnce   sync.Once
}

// Start starts a Consul mock on a loopback-only HTTP listener.
func Start() *Server {
	server := &Server{
		leader:      defaultLeader,
		services:    []byte("{}"),
		entries:     make(map[string][]byte),
		healthState: []byte("[]"),
		index:       1,
	}
	server.server = httptest.NewServer(http.HandlerFunc(server.serveHTTP))
	return server
}

// URL returns the server URL in http://127.0.0.1:<port> form.
func (s *Server) URL() string {
	return s.server.URL
}

// Address returns the full HTTP URL accepted directly by api.Config.Address.
func (s *Server) Address() string {
	return s.URL()
}

// SetLeader replaces the leader returned by the status endpoint.
func (s *Server) SetLeader(leader string) {
	s.mu.Lock()
	s.leader = leader
	s.mu.Unlock()
}

// SetServices replaces the catalog service response.
func (s *Server) SetServices(services map[string][]string) {
	if services == nil {
		services = map[string][]string{}
	}
	encoded := mustMarshal(services)

	s.mu.Lock()
	s.services = encoded
	s.mu.Unlock()
}

// SetEntries replaces the health service response for name.
func (s *Server) SetEntries(name string, entries []*api.ServiceEntry) {
	if entries == nil {
		entries = []*api.ServiceEntry{}
	}
	encoded := mustMarshal(entries)

	s.mu.Lock()
	s.entries[name] = encoded
	s.mu.Unlock()
}

// SetHealthState replaces the response for the health state endpoint.
func (s *Server) SetHealthState(checks []*api.HealthCheck) {
	if checks == nil {
		checks = []*api.HealthCheck{}
	}
	encoded := mustMarshal(checks)

	s.mu.Lock()
	s.healthState = encoded
	s.mu.Unlock()
}

// AdvanceIndex increments and returns the Consul query index.
func (s *Server) AdvanceIndex() uint64 {
	s.mu.Lock()
	s.index++
	index := s.index
	s.mu.Unlock()
	return index
}

// Index returns the current Consul query index.
func (s *Server) Index() uint64 {
	s.mu.RLock()
	index := s.index
	s.mu.RUnlock()
	return index
}

// Requests returns independent snapshots of all received requests.
func (s *Server) Requests() []Request {
	s.mu.RLock()
	requests := cloneRequests(s.requests)
	s.mu.RUnlock()
	return requests
}

// Close stops the server. It is safe to call more than once.
func (s *Server) Close() {
	s.closeOnce.Do(s.server.Close)
}

func (s *Server) serveHTTP(w http.ResponseWriter, request *http.Request) {
	s.record(request)
	if request.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, 0, []byte(`{"error":"method not allowed"}`))
		return
	}

	switch {
	case request.URL.Path == "/v1/status/leader":
		s.mu.RLock()
		leader := s.leader
		s.mu.RUnlock()
		writeJSON(w, http.StatusOK, 0, mustMarshal(leader))
	case request.URL.Path == "/v1/catalog/services":
		payload, index := s.snapshotServices()
		writeJSON(w, http.StatusOK, index, payload)
	case request.URL.Path == "/v1/health/state/any":
		payload, index := s.snapshotHealthState()
		writeJSON(w, http.StatusOK, index, payload)
	case strings.HasPrefix(request.URL.Path, "/v1/health/service/"):
		name := strings.TrimPrefix(request.URL.Path, "/v1/health/service/")
		payload, index := s.snapshotEntries(name)
		payload = filterEntries(payload, request.URL.Query())
		writeJSON(w, http.StatusOK, index, payload)
	default:
		writeJSON(w, http.StatusNotFound, 0, []byte(`{"error":"not found"}`))
	}
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

func (s *Server) snapshotServices() ([]byte, uint64) {
	s.mu.RLock()
	payload := append([]byte(nil), s.services...)
	index := s.index
	s.mu.RUnlock()
	return payload, index
}

func (s *Server) snapshotHealthState() ([]byte, uint64) {
	s.mu.RLock()
	payload := append([]byte(nil), s.healthState...)
	index := s.index
	s.mu.RUnlock()
	return payload, index
}

func (s *Server) snapshotEntries(name string) ([]byte, uint64) {
	s.mu.RLock()
	payload := append([]byte(nil), s.entries[name]...)
	index := s.index
	s.mu.RUnlock()
	if len(payload) == 0 {
		payload = []byte("[]")
	}
	return payload, index
}

func filterEntries(payload []byte, query url.Values) []byte {
	tags := query["tag"]
	passingOnly := query.Get("passing") == "1" || query.Get("passing") == "true"
	if len(tags) == 0 && !passingOnly {
		return payload
	}

	var entries []*api.ServiceEntry
	if err := json.Unmarshal(payload, &entries); err != nil {
		panic(err)
	}
	filtered := make([]*api.ServiceEntry, 0, len(entries))
	for _, entry := range entries {
		if hasTags(entry, tags) && (!passingOnly || isPassing(entry)) {
			filtered = append(filtered, entry)
		}
	}
	return mustMarshal(filtered)
}

func hasTags(entry *api.ServiceEntry, tags []string) bool {
	if len(tags) == 0 {
		return true
	}
	if entry == nil || entry.Service == nil {
		return false
	}
	for _, wanted := range tags {
		found := false
		for _, tag := range entry.Service.Tags {
			if tag == wanted {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func isPassing(entry *api.ServiceEntry) bool {
	if entry == nil {
		return false
	}
	for _, check := range entry.Checks {
		if check == nil || check.Status != api.HealthPassing {
			return false
		}
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, index uint64, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	if index != 0 {
		w.Header().Set("X-Consul-Index", strconv.FormatUint(index, 10))
	}
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func mustMarshal(value interface{}) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
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

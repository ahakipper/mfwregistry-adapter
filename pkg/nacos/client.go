// Package nacos is the Nacos InstanceSink adapter: a hand-rolled HTTP client
// over the Nacos v1 OpenAPI (decision D4 of docs/nacos-sink-plan.md §7.2)
// and the Sink that implements internal/ports.InstanceSink over it (§7.3).
//
// The client deliberately uses only net/http, net/url and encoding/json —
// no SDK dependency, matching the repo's discoverycenter precedent: the
// needed surface is a handful of endpoints, timeouts stay under our control,
// and an httptest-based mock (internal/testkit/nacosmock) covers the tests.
package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"spotter/internal/ports"
)

// Wire conventions of the Nacos v1 OpenAPI (F0-verified against a v2.1.0
// standalone server, plan §7.1).
const (
	// RequestTimeout bounds every v1 OpenAPI request, matching the
	// discoverycenter client precedent (client.go readTimeout).
	RequestTimeout = 10 * time.Second
	// DefaultGroup is the fixed group of the sink: serviceName alone
	// addresses the app-code namespace (plan §7.3).
	DefaultGroup = "DEFAULT_GROUP"
	// DefaultNamespaceID is the fixed namespace, `public` (non-default
	// namespaces are a non-goal, plan §10).
	DefaultNamespaceID = "public"
	// DefaultCluster is the cluster Nacos itself assumes when a request
	// omits clusterName.
	DefaultCluster = "DEFAULT"
	// maxServiceListPages bounds the service-list pagination loop (a
	// safety valve against a server answering a full page forever).
	maxServiceListPages = 10000
	// catalogPageSize is the page size the catalog listing requests: large
	// enough that one page covers a normal service, small enough to keep
	// single responses bounded (the plan's default loop size).
	catalogPageSize = 100
	// healthCheckerNone is the healthChecker JSON UpdateCluster sends: the
	// NONE checker, the cluster configuration that switches Nacos's own
	// server-side health check off (see UpdateCluster).
	healthCheckerNone = `{"type":"NONE"}`
)

// Paths of the v1 OpenAPI endpoints (and the console readiness probe).
const (
	pathInstance    = "/nacos/v1/ns/instance"
	pathInstanceLis = "/nacos/v1/ns/instance/list"
	pathServiceList = "/nacos/v1/ns/service/list"
	pathReadiness   = "/nacos/v1/console/health/readiness"
	pathCatalogList = "/nacos/v1/ns/catalog/instances"
	pathCluster     = "/nacos/v1/ns/cluster"
)

// InstanceParams is the wire form of one instance for register and
// deregister. The composite instance id Nacos derives from it is
// ip#port#clusterName#groupName@@serviceName, so register and deregister
// must build identical params for the same instance (plan §7.3).
type InstanceParams struct {
	ServiceName string
	IP          string
	Port        int
	ClusterName string
	GroupName   string
	NamespaceID string
	Enabled     bool
	Ephemeral   bool
	Metadata    map[string]string
}

// Host is one instance as GET /nacos/v1/ns/instance/list serves it
// (the F0-observed v2.1.0 response shape).
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

// Hosts is the instance list response of one service.
type Hosts struct {
	Count int    `json:"count"`
	Hosts []Host `json:"hosts"`
}

// CatalogPage is one page of the catalog instances response. The page field
// is `list`, NOT `hosts` — the catalog endpoint's shape, verified against
// Nacos 2.1.0 (the audit's F8 fix design).
type CatalogPage struct {
	Count int    `json:"count"`
	List  []Host `json:"list"`
}

// ServicePage is one page of the service list response.
type ServicePage struct {
	Count int      `json:"count"`
	Doms  []string `json:"doms"`
}

// Client is a small net/http client for the Nacos v1 OpenAPI.
type Client struct {
	baseURL *url.URL
	http    *http.Client
	logger  ports.Logger
}

// NewClient creates a v1 OpenAPI client bound to addr (the Nacos base
// address, e.g. http://127.0.0.1:18848 or 127.0.0.1:18848 — an address
// without a scheme defaults to http://, matching the flag help and the
// plan §8.4 soak invocation). A nil logger is defaulted.
func NewClient(addr string, logger ports.Logger) (*Client, error) {
	if addr == "" {
		return nil, errors.New("nacos: address is required")
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	parsed, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("nacos: invalid address %q: %w", addr, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("nacos: address %q is not an absolute http(s) URL", addr)
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	return &Client{
		baseURL: parsed,
		http:    &http.Client{Timeout: RequestTimeout},
		logger:  logger,
	}, nil
}

// CheckReadiness polls the console readiness endpoint once; it is the
// startup health gate wired by the server construction. A non-200 status
// or a transport error is an error. Like NewClient, an address without a
// scheme defaults to http://.
func CheckReadiness(addr string, timeout time.Duration) error {
	if addr != "" && !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	client := &http.Client{Timeout: timeout}
	response, err := client.Get(joinURL(addr, pathReadiness))
	if err != nil {
		return fmt.Errorf("nacos: readiness check %s: %w", addr, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("nacos: readiness check %s answered status %d", addr, response.StatusCode)
	}
	return nil
}

// RegisterInstance registers (upserts) one persistent instance.
func (c *Client) RegisterInstance(params InstanceParams) error {
	return c.doForm(http.MethodPost, pathInstance, params.values())
}

// DeregisterInstance deletes one instance by its composite id parameters.
func (c *Client) DeregisterInstance(params InstanceParams) error {
	return c.doForm(http.MethodDelete, pathInstance, params.values())
}

// UpdateCluster disables Nacos's server-side health check for one (service,
// cluster) pair: it PUTs the cluster configuration with healthChecker
// {"type":"NONE"} (PUT /nacos/v1/ns/cluster), the checker under which Nacos
// stops TCP-probing the cluster's persistent instances and takes registration
// as the health authority instead. See Sink.register for why spotter wants
// that.
//
// Wire form (verified live against a v2.1.0 server): the servlet does NOT
// parse a form body on PUT, so every parameter rides the QUERY STRING of a
// body-less request — exactly the doForm shape (which URL-encodes its values
// for every method, POST and DELETE included). checkPort and
// useInstancePort4Check (note the literal "4" in the name) are required by
// the controller but irrelevant under the NONE checker; they are sent as 0
// and false. The controller does not read groupName; it is sent anyway to
// match the client's always-address-the-fixed-group convention.
// namespaceId is likewise sent explicitly (public). Success answers the same
// plain-text "ok" as register, and the call is idempotent: repeating the PUT
// re-writes the same configuration.
//
// Side effect to keep in mind (documented, accepted): the PUT REPLACES the
// cluster's user metadata — the optional metadata param defaults to empty —
// and spotter never sets cluster metadata, so for this sink the replacement
// is a no-op.
func (c *Client) UpdateCluster(serviceName, clusterName string) error {
	values := url.Values{}
	values.Set("serviceName", serviceName)
	values.Set("clusterName", clusterName)
	values.Set("checkPort", "0")
	values.Set("useInstancePort4Check", "false")
	values.Set("healthChecker", healthCheckerNone)
	values.Set("groupName", DefaultGroup)
	values.Set("namespaceId", DefaultNamespaceID)
	return c.doForm(http.MethodPut, pathCluster, values)
}

// ListInstances returns every instance of one service in DefaultGroup.
// A service without instances yields an empty slice.
//
// Fidelity limit (the F8 root cause): the endpoint HIDES instances with
// enabled=false — the exact state spotter's own unhealthy pushes write — so
// this view is unsuitable for the prune. Use ListCatalogInstances for that.
func (c *Client) ListInstances(serviceName string) ([]Host, error) {
	values := url.Values{}
	values.Set("serviceName", serviceName)
	values.Set("groupName", DefaultGroup)
	values.Set("namespaceId", DefaultNamespaceID)

	var hosts Hosts
	if err := c.doJSON(http.MethodGet, pathInstanceLis, values, &hosts); err != nil {
		return nil, err
	}
	if hosts.Hosts == nil {
		hosts.Hosts = []Host{}
	}
	return hosts.Hosts, nil
}

// ListCatalogInstances returns every instance of one service and cluster in
// DefaultGroup through the ADMIN catalog view (GET /nacos/v1/ns/catalog/
// instances). Unlike ListInstances, the catalog lists instances with
// enabled=false too, so the PushAll prune can see — and delete — the
// disabled remote drift the instance list hides (the F8 fix). Pagination
// follows the ListServices discipline — iterate while a page comes back full,
// capped at maxServiceListPages — hardened with a count-based stop: a server
// that clamps the effective page size below the requested pageSize=100
// serves pages that always look SHORT, so the short-page rule alone would
// silently truncate the walk at one clamped page (a partial prune view). The
// response's Count is the server's declared total, so the walk keeps going
// while the accumulated total is below it; the count rule cannot run past
// the end because a count-terminated walk returns exactly Count hosts.
func (c *Client) ListCatalogInstances(serviceName, clusterName string) ([]Host, error) {
	hosts := make([]Host, 0)
	for page := 1; page <= maxServiceListPages; page++ {
		values := url.Values{}
		values.Set("serviceName", serviceName)
		values.Set("clusterName", clusterName)
		values.Set("groupName", DefaultGroup)
		values.Set("namespaceId", DefaultNamespaceID)
		values.Set("pageSize", strconv.Itoa(catalogPageSize))
		values.Set("pageNo", strconv.Itoa(page))
		values.Set("hasIpCount", "false")

		var body CatalogPage
		if err := c.doJSON(http.MethodGet, pathCatalogList, values, &body); err != nil {
			return nil, err
		}
		hosts = append(hosts, body.List...)
		if len(body.List) < catalogPageSize {
			// The count-based stop: a clamped page (shorter than requested
			// only because the server capped the effective page size) with
			// the declared total still uncollected is NOT the end — keep
			// walking. A short page whose Count is absent, zero, or already
			// reached stays the termination for well-behaved servers.
			if body.Count > 0 && len(hosts) < body.Count {
				continue
			}
			return hosts, nil
		}
		// A full-length page whose Count has been collected ends the walk
		// here instead of paging once more for an empty confirmation page.
		if body.Count > 0 && len(hosts) >= body.Count {
			return hosts, nil
		}
	}
	return nil, fmt.Errorf("nacos: catalog instances pagination exceeded %d pages for %s/%s",
		maxServiceListPages, serviceName, clusterName)
}

// ListServices returns every service name of DefaultGroup, paginating the
// service list until a short (or empty) page ends the iteration. pageSize
// is the requested page size (the plan's default loop uses 100).
func (c *Client) ListServices(pageSize int) ([]string, error) {
	if pageSize < 1 {
		pageSize = 100
	}
	names := make([]string, 0)
	for page := 1; page <= maxServiceListPages; page++ {
		values := url.Values{}
		values.Set("pageNo", strconv.Itoa(page))
		values.Set("pageSize", strconv.Itoa(pageSize))
		values.Set("groupName", DefaultGroup)
		values.Set("namespaceId", DefaultNamespaceID)

		var body ServicePage
		if err := c.doJSON(http.MethodGet, pathServiceList, values, &body); err != nil {
			return nil, err
		}
		names = append(names, body.Doms...)
		if len(body.Doms) < pageSize {
			return names, nil
		}
	}
	return nil, fmt.Errorf("nacos: service list pagination exceeded %d pages", maxServiceListPages)
}

// values renders the params as the endpoint query form. The wire carries
// the parameters URL-encoded on the query string (the v1 OpenAPI form
// convention), including the metadata JSON map.
func (p InstanceParams) values() url.Values {
	values := url.Values{}
	values.Set("serviceName", p.ServiceName)
	values.Set("ip", p.IP)
	values.Set("port", strconv.Itoa(p.Port))
	if p.ClusterName != "" {
		values.Set("clusterName", p.ClusterName)
	}
	group := p.GroupName
	if group == "" {
		group = DefaultGroup
	}
	values.Set("groupName", group)
	namespace := p.NamespaceID
	if namespace == "" {
		namespace = DefaultNamespaceID
	}
	values.Set("namespaceId", namespace)
	values.Set("ephemeral", strconv.FormatBool(p.Ephemeral))
	values.Set("enabled", strconv.FormatBool(p.Enabled))
	if len(p.Metadata) > 0 {
		if encoded, err := json.Marshal(p.Metadata); err == nil {
			values.Set("metadata", string(encoded))
		}
	}
	return values
}

// APIError is a non-200 answer from the Nacos v1 OpenAPI: the request that
// was rejected, the HTTP status and the (truncated) response body. It keeps
// the status as a field so callers up the chain can classify the failure
// through Permanent() instead of parsing the message text.
type APIError struct {
	Method string
	Path   string
	Status int
	Body   string
}

// Error renders exactly the message the plain fmt.Errorf site produced, so
// logs and error-text assertions keep their wording.
func (e *APIError) Error() string {
	return fmt.Sprintf("nacos: %s %s answered status %d: %s", e.Method, e.Path, e.Status, e.Body)
}

// Permanent reports whether the request itself is rejected (a 4xx): Nacos
// will answer an identical retry the same way forever, so the retry queue
// drops the entry instead of spinning (the live incident: 9624 futile
// retries of a DELETE without an ip parameter). 5xx answers stay retriable.
func (e *APIError) Permanent() bool {
	return e.Status >= 400 && e.Status < 500
}

// doForm issues one mutating request (register/deregister/cluster update).
// Nacos answers "ok" (plain text) on success; any other status is an error
// carrying the status code and body. Every parameter rides the URL-encoded
// query string — including on PUT, whose form body the v1 servlet does not
// parse (see UpdateCluster).
func (c *Client) doForm(method, path string, values url.Values) error {
	target := joinURL(c.baseURL.String(), path)
	target += "?" + values.Encode()

	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		return fmt.Errorf("nacos: build %s %s: %w", method, path, err)
	}
	ctx, cancel := context.WithTimeout(request.Context(), RequestTimeout)
	defer cancel()
	request = request.WithContext(ctx)

	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("nacos: %s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("nacos: %s %s: read body: %w", method, path, err)
	}
	if response.StatusCode != http.StatusOK {
		return &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
	}
	return nil
}

// doJSON issues one read request and decodes the JSON response body into
// out.
func (c *Client) doJSON(method, path string, values url.Values, out interface{}) error {
	target := joinURL(c.baseURL.String(), path)
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}

	request, err := http.NewRequest(method, target, nil)
	if err != nil {
		return fmt.Errorf("nacos: build %s %s: %w", method, path, err)
	}
	ctx, cancel := context.WithTimeout(request.Context(), RequestTimeout)
	defer cancel()
	request = request.WithContext(ctx)

	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("nacos: %s %s: %w", method, path, err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("nacos: %s %s: read body: %w", method, path, err)
	}
	if response.StatusCode != http.StatusOK {
		return &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("nacos: %s %s: decode response: %w", method, path, err)
	}
	return nil
}

// joinURL concatenates a base address and an endpoint path.
func joinURL(base, path string) string {
	if len(base) > 0 && base[len(base)-1] == '/' {
		return base + path[1:]
	}
	return base + path
}

// truncateBody bounds an error body to a readable length.
func truncateBody(body []byte) string {
	const limit = 512
	if len(body) > limit {
		return string(body[:limit]) + "..."
	}
	return string(body)
}

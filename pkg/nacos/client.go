// Package nacos is the Nacos InstanceSink adapter: a hand-rolled HTTP client
// over the Nacos v1 OpenAPI (decision D4 of docs/nacos-sink-plan.md §7.2)
// and the Sink that implements internal/ports.InstanceSink over it (§7.3).
//
// The client deliberately uses only net/http, net/url and encoding/json —
// no SDK dependency, matching the repo's discoverycenter precedent: the
// needed surface is four endpoints, timeouts stay under our control, and
// an httptest-based mock (internal/testkit/nacosmock) covers the tests.
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
)

// Paths of the four v1 endpoints.
const (
	pathInstance    = "/nacos/v1/ns/instance"
	pathInstanceLis = "/nacos/v1/ns/instance/list"
	pathServiceList = "/nacos/v1/ns/service/list"
	pathReadiness   = "/nacos/v1/console/health/readiness"
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

// ListInstances returns every instance of one service in DefaultGroup.
// A service without instances yields an empty slice.
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

// doForm issues one mutating request (register/deregister). Nacos answers
// "ok" (plain text) on success; any other status is an error carrying the
// status code and body.
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
		return fmt.Errorf("nacos: %s %s answered status %d: %s", method, path, response.StatusCode, truncateBody(body))
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
		return fmt.Errorf("nacos: %s %s answered status %d: %s", method, path, response.StatusCode, truncateBody(body))
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

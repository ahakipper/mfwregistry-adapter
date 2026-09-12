// Package nacos is the transitional Nacos InstanceSink adapter. It currently
// exposes the Nacos v1 OpenAPI compatibility transport used by the migration
// harness; the production default is scheduled to move to the official
// nacos-sdk-go facade under remediation gate B3. The Sink implements
// internal/ports.InstanceSink over that transport (§7.3).
package nacos

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
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
	// DefaultGroup is the group used when no explicit ClientConfig group is
	// supplied (the sink still supports a configured group).
	DefaultGroup = "DEFAULT_GROUP"
	// DefaultNamespaceID is the namespace used when no explicit ClientConfig
	// namespace is supplied.
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

// Client is the compatibility client for the Nacos v1 OpenAPI. It is kept
// behind the Sink boundary so the B3 SDK facade can replace it without
// changing provider/reconcile code.
type Client struct {
	baseURL  *url.URL
	baseURLs []*url.URL
	http     *http.Client
	logger   ports.Logger
	config   ClientConfig
}

// ClientConfig controls the HTTP compatibility transport used by the Nacos
// sink. It is intentionally explicit so authentication and TLS settings are
// never hidden in process globals or logged.
type ClientConfig struct {
	// ServerURL is the legacy single-address setting. When ServerURLs is
	// non-empty it is ignored; list order is the failover order.
	ServerURL string
	// ServerURLs is an ordered list of Nacos addresses. Transport and 5xx
	// failures advance to the next address; a 4xx is returned immediately.
	ServerURLs         []string
	NamespaceID        string
	GroupName          string
	Username           string
	Password           string
	AccessToken        string
	CAFile             string
	ServerName         string
	InsecureSkipVerify bool
	Timeout            time.Duration
	MaxConnsPerHost    int
}

// Transport tuning constants (dsca-2 DS-2-4, fix design: "construct the
// Transport explicitly in NewClient ... with <concurrency>/<cap> tied to
// the DS-2-1 semaphore"). The values are the LOAD-BEARING pair: a transport
// that allows fewer connections than the sink's push concurrency starves
// the worker group (requests queue on the dial), one that allows far more
// wastes connections against the server's accept queue. Both numbers track
// the sink's DefaultPushConcurrency (8): MaxIdleConnsPerHost keeps 8 idle
// keep-alive connections per host (vs the DEFAULT transport's effective 2 —
// the measured churn: 1000 concurrent registers opened 425-624 TCP
// connections, then kept 2), and MaxConnsPerHost caps the in-flight total
// at the same 8 — the circuit breaker that bounds a wedged nacos to 8 held
// workers instead of the ants pool's 100.
const (
	// transportMaxIdleConnsPerHost matches the sink's push concurrency so a
	// full worker group finds a warm connection each.
	transportMaxIdleConnsPerHost = 8
	// transportMaxConnsPerHost is the per-host ceiling: the breaker.
	transportMaxConnsPerHost = 8
	// transportMaxIdleConns is the process-wide idle pool (one host in
	// practice; a small multiple covers the readiness probe's client).
	transportMaxIdleConns = 32
	// transportIdleConnTimeout retires idle keep-alives (the default
	// transport's 90s, kept).
	transportIdleConnTimeout = 90 * time.Second
	// transportDialTimeout bounds connection setup (the contract's 2s).
	transportDialTimeout = 2 * time.Second
	// transportTLSHandshakeTimeout bounds TLS setup (the contract's 5s).
	transportTLSHandshakeTimeout = 5 * time.Second
)

// NewClient creates a v1 OpenAPI client bound to addr (the Nacos base
// address, e.g. http://127.0.0.1:18848 or 127.0.0.1:18848 — an address
// without a scheme defaults to http://, matching the flag help and the
// plan §8.4 soak invocation). A nil logger is defaulted.
//
// The http.Client uses an EXPLICIT tuned Transport (dsca-2 DS-2-4): the
// zero-Transport client inherited http.DefaultTransport, whose effective
// MaxIdleConnsPerHost is 2 — serial pushes reused one connection fine, but
// the moment DS-2-1's bounded worker group runs, every worker past the
// second dials fresh (the measured 425-624 connections for 1000 concurrent
// registers) and only 2 survive afterwards: connection churn on every
// burst, re-paid on the next. The explicit transport sizes the idle pool
// to the push concurrency and caps the total at the same number; the
// RequestTimeout fallback is used only when ClientConfig.Timeout is unset
// (the configured timeout also covers response-body reads).
func NewClient(addr string, logger ports.Logger) (*Client, error) {
	return NewClientWithConfig(ClientConfig{ServerURL: addr}, logger)
}

// NewClientWithConfig creates a configured Nacos client. NewClient remains a
// compatibility wrapper for existing callers.
func NewClientWithConfig(cfg ClientConfig, logger ports.Logger) (*Client, error) {
	addresses := append([]string(nil), cfg.ServerURLs...)
	if len(addresses) == 0 && cfg.ServerURL != "" {
		addresses = []string{cfg.ServerURL}
	}
	if len(addresses) == 0 {
		return nil, errors.New("nacos: address is required")
	}
	parsedURLs := make([]*url.URL, 0, len(addresses))
	for _, address := range addresses {
		addr := address
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
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("nacos: unsupported URL scheme %q", parsed.Scheme)
		}
		if parsed.User != nil {
			return nil, errors.New("nacos: credentials in URL are not allowed; use explicit client config")
		}
		for _, key := range []string{"username", "password", "accessToken"} {
			if parsed.Query().Get(key) != "" {
				return nil, fmt.Errorf("nacos: credential query parameter %q is not allowed", key)
			}
		}
		parsedURLs = append(parsedURLs, parsed)
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	maxConns := cfg.MaxConnsPerHost
	if maxConns < 1 {
		maxConns = transportMaxConnsPerHost
	}
	rootCAs, err := loadRootCAs(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		MaxIdleConns:          transportMaxIdleConns,
		MaxIdleConnsPerHost:   transportMaxIdleConnsPerHost,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       transportIdleConnTimeout,
		DialContext:           (&net.Dialer{Timeout: transportDialTimeout}).DialContext,
		TLSHandshakeTimeout:   transportTLSHandshakeTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       &tls.Config{RootCAs: rootCAs, ServerName: cfg.ServerName, InsecureSkipVerify: cfg.InsecureSkipVerify}, // #nosec G402: explicit operator-controlled compatibility option
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = RequestTimeout
	}
	return &Client{
		baseURL:  parsedURLs[0],
		baseURLs: parsedURLs,
		http:     &http.Client{Timeout: timeout, Transport: transport},
		logger:   logger,
		config:   cfg,
	}, nil
}

func loadRootCAs(path string) (*x509.CertPool, error) {
	if path == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("nacos: read CA file: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("nacos: CA file contains no certificates")
	}
	return pool, nil
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

// CheckReadinessWithConfig performs the read side of the readiness gate with
// the same configured transport and credentials as sink requests.
func CheckReadinessWithConfig(cfg ClientConfig, logger ports.Logger) error {
	c, err := NewClientWithConfig(cfg, logger)
	if err != nil {
		return err
	}
	var lastErr error
	for _, base := range c.baseURLsOrPrimary() {
		request, err := http.NewRequest(http.MethodGet, joinURL(base.String(), pathReadiness), nil)
		if err != nil {
			return fmt.Errorf("nacos: readiness request: %w", err)
		}
		values := request.URL.Query()
		c.addAuth(values)
		request.URL.RawQuery = values.Encode()
		timeout := c.http.Timeout
		if timeout <= 0 {
			timeout = RequestTimeout
		}
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		response, err := c.http.Do(request.WithContext(ctx))
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("nacos: readiness check via %s: %w", base.Host, err)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		cancel()
		if response.StatusCode >= 500 {
			lastErr = fmt.Errorf("nacos: readiness check via %s answered status %d", base.Host, response.StatusCode)
			continue
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("nacos: readiness check answered status %d", response.StatusCode)
		}
		// Pin both canary operations to the address that passed the read
		// probe. Retrying registration/cleanup against another server after a
		// lost response could leave a canary behind on the first server.
		probeClient := *c
		probeClient.baseURL = base
		probeClient.baseURLs = []*url.URL{base}
		canary := InstanceParams{
			ServiceName: fmt.Sprintf("__spotter_readiness_%d", time.Now().UnixNano()),
			IP:          "127.0.0.1",
			Port:        1,
			ClusterName: "spotter-readiness",
			GroupName:   c.config.GroupName,
			NamespaceID: c.config.NamespaceID,
			Enabled:     false,
			Ephemeral:   false,
			Metadata:    map[string]string{"spotterOwner": "spotter", "probe": "readiness"},
		}
		if err := probeClient.RegisterInstance(canary); err != nil {
			c.logger.Errorf("nacos: readiness write probe register failed via %s: %s", base.Host, err)
			return fmt.Errorf("nacos: readiness write probe: %w", err)
		}
		if err := probeClient.DeregisterInstance(canary); err != nil {
			// The probe is persistent. A failed cleanup is both a startup
			// failure and an operator-visible drift warning; never hide it by
			// retrying another server where the canary may not exist.
			c.logger.Warnf("nacos: readiness write probe cleanup failed via %s; persistent canary may remain: %s", base.Host, err)
			return fmt.Errorf("nacos: readiness write probe cleanup: %w", err)
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("nacos: no configured server addresses")
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
// namespaceId is likewise sent explicitly (the configured namespace, or
// public by default). Success answers the same
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

// ListInstances returns every instance of one service in the configured
// group/namespace (DefaultGroup/public when unset).
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
// the configured group/namespace through the ADMIN catalog view (GET /nacos/v1/ns/catalog/
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

// ListServices returns every service name of the configured group/namespace,
// paginating the service list until a short (or empty) page ends the iteration. pageSize
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
	c.addAuth(values)
	var lastErr error
	for _, base := range c.baseURLsOrPrimary() {
		target := joinURL(base.String(), path) + "?" + values.Encode()
		request, err := http.NewRequest(method, target, nil)
		if err != nil {
			return fmt.Errorf("nacos: build %s %s: %w", method, path, err)
		}
		timeout := c.http.Timeout
		if timeout <= 0 {
			timeout = RequestTimeout
		}
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		response, err := c.http.Do(request.WithContext(ctx))
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("nacos: %s %s via %s: %w", method, path, base.Host, err)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		cancel()
		if readErr != nil {
			lastErr = fmt.Errorf("nacos: %s %s: read body: %w", method, path, readErr)
			continue
		}
		if response.StatusCode >= 500 {
			lastErr = &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
			continue
		}
		if response.StatusCode != http.StatusOK {
			return &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("nacos: no configured server addresses")
}

// doJSON issues one read request and decodes the JSON response body into
// out.
func (c *Client) doJSON(method, path string, values url.Values, out interface{}) error {
	c.addAuth(values)
	var lastErr error
	for _, base := range c.baseURLsOrPrimary() {
		target := joinURL(base.String(), path)
		if encoded := values.Encode(); encoded != "" {
			target += "?" + encoded
		}
		request, err := http.NewRequest(method, target, nil)
		if err != nil {
			return fmt.Errorf("nacos: build %s %s: %w", method, path, err)
		}
		timeout := c.http.Timeout
		if timeout <= 0 {
			timeout = RequestTimeout
		}
		ctx, cancel := context.WithTimeout(request.Context(), timeout)
		response, err := c.http.Do(request.WithContext(ctx))
		if err != nil {
			cancel()
			lastErr = fmt.Errorf("nacos: %s %s via %s: %w", method, path, base.Host, err)
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		_ = response.Body.Close()
		cancel()
		if readErr != nil {
			lastErr = fmt.Errorf("nacos: %s %s: read body: %w", method, path, readErr)
			continue
		}
		if response.StatusCode >= 500 {
			lastErr = &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
			continue
		}
		if response.StatusCode != http.StatusOK {
			return &APIError{Method: method, Path: path, Status: response.StatusCode, Body: truncateBody(body)}
		}
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("nacos: %s %s: decode response: %w", method, path, err)
		}
		return nil
	}
	if lastErr != nil {
		return lastErr
	}
	return errors.New("nacos: no configured server addresses")
}

func (c *Client) baseURLsOrPrimary() []*url.URL {
	if len(c.baseURLs) > 0 {
		return c.baseURLs
	}
	if c.baseURL != nil {
		return []*url.URL{c.baseURL}
	}
	return nil
}

func (c *Client) addAuth(values url.Values) {
	if c.config.NamespaceID != "" {
		values.Set("namespaceId", c.config.NamespaceID)
	}
	if c.config.GroupName != "" {
		values.Set("groupName", c.config.GroupName)
	}
	if c.config.AccessToken != "" {
		values.Set("accessToken", c.config.AccessToken)
		return
	}
	if c.config.Username != "" {
		values.Set("username", c.config.Username)
	}
	if c.config.Password != "" {
		values.Set("password", c.config.Password)
	}
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

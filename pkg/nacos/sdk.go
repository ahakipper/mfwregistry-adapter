package nacos

// This file is the seam for the official Nacos Go SDK. Naming operations are
// deliberately kept behind a tiny interface so the sink does not depend on
// SDK concrete types and unit tests can prove operation routing without a
// running Nacos server. The naming SDK's SelectAllInstances and
// GetAllServicesInfo methods are also used for catalog/prune and service-list
// reads; no production operation falls back to a hand-written HTTP request.

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// ErrUnsupportedOperation is returned by the SDK-only path when the pinned
// official naming client does not expose an operation (for example, the
// cluster health-check admin update).  Callers must surface this error as a
// release/configuration failure; they must not silently fall back to a raw
// HTTP request.
var ErrUnsupportedOperation = errors.New("nacos sdk: unsupported operation")

// TransportMode selects the Nacos wire adapter. SDK is the production path;
// HTTPCompat is a temporary migration/rollback path for legacy Admin fixtures
// and tests. An empty mode is resolved to SDK by NewClientWithConfig.
type TransportMode string

const (
	TransportSDK        TransportMode = "sdk"
	TransportHTTPCompat TransportMode = "http-compat"
)

// HTTPCompatibilityException records operations retained solely by the
// explicit migration/test HTTP adapter. Keeping this registry next to the
// facade prevents an accidental, unowned expansion of raw HTTP usage.
type HTTPCompatibilityException struct {
	Operation       string
	Owner           string
	ExpiresOn       string
	RemovalCriteria string
}

// HTTPCompatibilityExceptions is reviewed at every B3 release gate. The
// date is an expiry, not a promise that the exception is production-safe: the
// operation remains a release blocker until its removal criteria are met.
var HTTPCompatibilityExceptions = []HTTPCompatibilityException{
	{Operation: "cluster-health-check-update", Owner: "spotter-maintainers", ExpiresOn: "2026-10-31", RemovalCriteria: "official Nacos Admin/Maintainer SDK equivalent verified against target version"},
}

// SDKUnsupportedOperations is the explicit production gap in the pinned
// official SDK.  The sink reports this operation as a typed error in SDK mode
// and never substitutes a raw HTTP request.  It remains a release blocker
// until an official Admin/Maintainer SDK surface is verified.
var SDKUnsupportedOperations = []string{"cluster-health-check-update"}

func effectiveNamespace(namespace string) string {
	if namespace == "" {
		return DefaultNamespaceID
	}
	return namespace
}

// sdkNamingClient is the subset of the official SDK used by the adapter.
// Keeping this interface local makes every operation auditable and injectable.
type sdkNamingClient interface {
	RegisterInstance(vo.RegisterInstanceParam) (bool, error)
	BatchRegisterInstance(vo.BatchRegisterInstanceParam) (bool, error)
	DeregisterInstance(vo.DeregisterInstanceParam) (bool, error)
	UpdateInstance(vo.UpdateInstanceParam) (bool, error)
	SelectAllInstances(vo.SelectAllInstancesParam) ([]model.Instance, error)
	GetAllServicesInfo(vo.GetAllServiceInfoParam) (model.ServiceList, error)
	Subscribe(*vo.SubscribeParam) error
	Unsubscribe(*vo.SubscribeParam) error
	ServerHealthy() bool
	CloseClient()
}

// nacos3SDKVendor is the Spotter-owned operation seam for the official Nacos
// v3 naming SDK.  Keeping operation names explicit prevents callers from
// reaching the SDK's legacy naming_http delegate for persistent lifecycle
// operations; the concrete v3 adapter is responsible for emitting the
// RegisterInstanceRequest/DeregisterInstanceRequest gRPC payloads.
type nacos3SDKVendor interface {
	RegisterPersistent(InstanceParams) error
	DeregisterPersistent(InstanceParams) error
	SelectAll(service, cluster, group string) ([]Host, error)
	ListServices(page, size int, namespace, group string) ([]string, int, error)
	Subscribe(service, group string, clusters []string, callback func([]Host, error)) error
	Unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error
	Close() error
}

type persistentBatchVendor interface {
	RegisterPersistentBatch([]InstanceParams) error
}

type sdkNamingFacade struct {
	client     sdkNamingClient
	vendor     nacos3SDKVendor
	group      string
	cacheDir   string
	ownedCache bool
}

func (f *sdkNamingFacade) hasPersistentVendor() bool {
	if f == nil {
		return false
	}
	if f.vendor != nil {
		return true
	}
	if c, ok := f.client.(*grpcSDKClient); ok {
		return c.vendor != nil
	}
	return false
}

func (f *sdkNamingFacade) close() {
	if f == nil {
		return
	}
	if f.vendor != nil {
		_ = f.vendor.Close()
	} else if f.client != nil {
		f.client.CloseClient()
	}
	if f.ownedCache && f.cacheDir != "" {
		_ = os.RemoveAll(f.cacheDir)
	}
}

// RegisterPersistent and DeregisterPersistent are the operation-specific
// Nacos 3 seam.  They force Ephemeral=false at the adapter boundary so a
// persistent publication can never be accidentally sent through an
// ephemeral-only path.
func (f *sdkNamingFacade) RegisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	if f.vendor != nil {
		return f.vendor.RegisterPersistent(p)
	}
	return f.register(p)
}

func (f *sdkNamingFacade) RegisterPersistentBatch(items []InstanceParams) error {
	for i := range items {
		items[i].Ephemeral = false
	}
	if batcher, ok := f.vendor.(persistentBatchVendor); ok {
		return batcher.RegisterPersistentBatch(items)
	}
	if batcher, ok := f.client.(persistentBatchVendor); ok {
		return batcher.RegisterPersistentBatch(items)
	}
	for _, item := range items {
		if err := f.RegisterPersistent(item); err != nil {
			return err
		}
	}
	return nil
}

func (f *sdkNamingFacade) DeregisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	if f.vendor != nil {
		return f.vendor.DeregisterPersistent(p)
	}
	return f.deregister(p)
}

func (f *sdkNamingFacade) SelectAll(service, cluster, group string) ([]Host, error) {
	if f.vendor != nil {
		return f.vendor.SelectAll(service, cluster, group)
	}
	if group != "" {
		f.group = effectiveGroup(group)
	}
	return f.list(service, cluster)
}

func (f *sdkNamingFacade) ListServices(page, size int, namespace, group string) ([]string, int, error) {
	if f.vendor != nil {
		return f.vendor.ListServices(page, size, namespace, group)
	}
	if group != "" {
		f.group = effectiveGroup(group)
	}
	return f.services(page, size, namespace)
}

func (f *sdkNamingFacade) Subscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	if f.vendor != nil {
		return f.vendor.Subscribe(service, group, clusters, callback)
	}
	return f.subscribe(service, group, clusters, callback)
}

func (f *sdkNamingFacade) Unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	if f.vendor != nil {
		return f.vendor.Unsubscribe(service, group, clusters, callback)
	}
	return f.unsubscribe(service, group, clusters, callback)
}

func (f *sdkNamingFacade) Close() error {
	if f.vendor != nil {
		return f.vendor.Close()
	}
	f.close()
	return nil
}

// sdkAPIError preserves the retry classification that the official SDK
// otherwise exposes only as text (for example, "request return error code
// 400"). The worker's permanent-error contract must survive the SDK facade;
// a rejected 4xx operation is not allowed to spin in the retry queue.
type sdkAPIError struct {
	err    error
	status int
}

func (e *sdkAPIError) Error() string { return e.err.Error() }

func (e *sdkAPIError) Unwrap() error { return e.err }

func (e *sdkAPIError) Permanent() bool { return e.status >= 400 && e.status < 500 }

func classifySDKError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, marker := range []string{"error code ", "status code: ", "status code "} {
		idx := strings.Index(message, marker)
		if idx < 0 {
			continue
		}
		codeText := message[idx+len(marker):]
		if end := strings.IndexAny(codeText, " \t\r\n,;:"); end >= 0 {
			codeText = codeText[:end]
		}
		status, parseErr := strconv.Atoi(codeText)
		if parseErr == nil && status >= 100 && status <= 599 {
			return &sdkAPIError{err: err, status: status}
		}
	}
	return err
}

func newSDKNamingFacade(cfg ClientConfig) (*sdkNamingFacade, error) {
	if cfg.AccessToken != "" {
		return nil, errors.New("nacos sdk: static accessToken is not supported by the official Go SDK; configure username/password or use the explicit http-compat rollback")
	}
	addresses := cfg.ServerURLs
	if len(addresses) == 0 && cfg.ServerURL != "" {
		addresses = []string{cfg.ServerURL}
	}
	servers := make([]constant.ServerConfig, 0, len(addresses))
	var transportScheme string
	for _, raw := range addresses {
		u, err := normalizeNacosURL(raw)
		if err != nil {
			return nil, err
		}
		port, err := strconv.ParseUint(u.Port(), 10, 64)
		if err != nil || port == 0 {
			return nil, fmt.Errorf("nacos sdk: invalid server port in %q", raw)
		}
		scheme := u.Scheme
		if scheme == "" {
			scheme = "http"
		}
		if transportScheme == "" {
			transportScheme = scheme
		} else if transportScheme != scheme {
			return nil, fmt.Errorf("nacos sdk: mixed server URL schemes %q and %q are not supported by one SDK client", transportScheme, scheme)
		}
		contextPath := u.Path
		if contextPath == "" || contextPath == "/" {
			contextPath = "/nacos"
		}
		servers = append(servers, constant.ServerConfig{Scheme: scheme, ContextPath: contextPath, IpAddr: u.Hostname(), Port: port, GrpcPort: port + 1000})
	}
	if len(servers) == 0 {
		return nil, fmt.Errorf("nacos sdk: at least one server address is required")
	}
	timeoutMs := uint64(cfg.Timeout.Milliseconds())
	if timeoutMs == 0 {
		timeoutMs = uint64(RequestTimeout.Milliseconds())
	}
	ns := cfg.NamespaceID
	if ns == DefaultNamespaceID {
		ns = ""
	}
	cacheDir := cfg.CacheDir
	owned := false
	if cacheDir == "" {
		var err error
		cacheDir, err = os.MkdirTemp("", "spotter-nacos-sdk-")
		if err != nil {
			return nil, fmt.Errorf("nacos sdk: create isolated cache: %w", err)
		}
		owned = true
	}
	clientCfg := &constant.ClientConfig{
		TimeoutMs:            timeoutMs,
		NamespaceId:          ns,
		Username:             cfg.Username,
		Password:             cfg.Password,
		NotLoadCacheAtStart:  true,
		UpdateCacheWhenEmpty: cfg.UpdateCacheWhenEmpty,
		DisableUseSnapShot:   true,
		CacheDir:             cacheDir,
		TLSCfg:               constant.TLSConfig{Appointed: true, Enable: servers[0].Scheme == "https", TrustAll: cfg.InsecureSkipVerify, CaFile: cfg.CAFile, ServerNameOverride: cfg.ServerName},
	}
	grpcVendor, err := newNacos3GRPCVendor(*clientCfg, servers)
	if err != nil {
		if owned {
			_ = os.RemoveAll(cacheDir)
		}
		return nil, fmt.Errorf("nacos sdk: create grpc naming facade: %w", err)
	}
	return &sdkNamingFacade{client: &grpcSDKClient{vendor: grpcVendor}, group: effectiveGroup(cfg.GroupName), cacheDir: cacheDir, ownedCache: owned}, nil
}

func normalizeNacosURL(raw string) (*url.URL, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.Port() == "" {
		return nil, fmt.Errorf("nacos sdk: invalid address %q", raw)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("nacos sdk: unsupported URL scheme %q", u.Scheme)
	}
	if u.User != nil || len(u.RawQuery) > 0 {
		return nil, fmt.Errorf("nacos sdk: credentials/query parameters in server URL are not allowed")
	}
	return u, nil
}

func (f *sdkNamingFacade) register(p InstanceParams) error {
	group := p.GroupName
	if group == "" {
		group = f.group
	}
	healthy := p.Enabled
	if p.Healthy != nil {
		healthy = *p.Healthy
	}
	ok, err := f.client.RegisterInstance(vo.RegisterInstanceParam{Ip: p.IP, Port: uint64(p.Port), Weight: 1, Enable: p.Enabled, Healthy: healthy, Metadata: p.Metadata, ClusterName: p.ClusterName, ServiceName: p.ServiceName, GroupName: effectiveGroup(group), Ephemeral: p.Ephemeral})
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return fmt.Errorf("nacos sdk: register rejected")
	}
	return nil
}

func (f *sdkNamingFacade) deregister(p InstanceParams) error {
	group := p.GroupName
	if group == "" {
		group = f.group
	}
	ok, err := f.client.DeregisterInstance(vo.DeregisterInstanceParam{Ip: p.IP, Port: uint64(p.Port), Cluster: p.ClusterName, ServiceName: p.ServiceName, GroupName: effectiveGroup(group), Ephemeral: p.Ephemeral})
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return fmt.Errorf("nacos sdk: deregister rejected")
	}
	return nil
}

// batchRegister exposes the official SDK batch operation for ephemeral
// instances. The Spotter sink deliberately uses persistent per-instance
// writes, because SDK v2.3.5 rejects persistent batch registration; keeping
// this method in the facade makes that limitation explicit and testable.
func (f *sdkNamingFacade) batchRegister(param vo.BatchRegisterInstanceParam) error {
	ok, err := f.client.BatchRegisterInstance(param)
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return fmt.Errorf("nacos sdk: batch register rejected")
	}
	return nil
}

// BatchRegisterEphemeral exposes the official SDK batch contract explicitly;
// persistent batch registration is rejected by the upstream SDK.
func (c *Client) BatchRegisterEphemeral(param vo.BatchRegisterInstanceParam) error {
	if c == nil || c.sdk == nil {
		return ErrUnsupportedOperation
	}
	for _, ins := range param.Instances {
		if !ins.Ephemeral {
			return fmt.Errorf("nacos sdk: persistent instance in ephemeral batch")
		}
	}
	return c.sdk.batchRegister(param)
}

func (f *sdkNamingFacade) list(service string, cluster string) ([]Host, error) {
	param := vo.SelectAllInstancesParam{ServiceName: service, GroupName: f.group}
	if cluster != "" {
		param.Clusters = []string{cluster}
	}
	items, err := f.client.SelectAllInstances(param)
	if err != nil {
		return nil, classifySDKError(err)
	}
	hosts := make([]Host, 0, len(items))
	for _, item := range items {
		hosts = append(hosts, Host{InstanceID: item.InstanceId, IP: item.Ip, Port: int(item.Port), Weight: item.Weight, Healthy: item.Healthy, Enabled: item.Enable, Ephemeral: item.Ephemeral, ClusterName: item.ClusterName, ServiceName: item.ServiceName, Metadata: item.Metadata})
	}
	return hosts, nil
}

// healthy reports the official SDK connectivity state.  It intentionally
// stays behind the facade so readiness never needs to construct a direct
// net/http request in SDK mode.
func (f *sdkNamingFacade) healthy() bool {
	return f.client.ServerHealthy()
}

// catalog returns the complete naming view for one service/cluster.  The
// official SelectAllInstances contract explicitly includes unhealthy,
// disabled and zero-weight instances, which is the set the sink's prune and
// authoritative compare need.  It is therefore the SDK equivalent of the
// old catalog HTTP endpoint for the production path.
func (f *sdkNamingFacade) catalog(service, cluster string) ([]Host, error) {
	if f.vendor != nil {
		return f.vendor.SelectAll(service, cluster, f.group)
	}
	return f.list(service, cluster)
}

func (f *sdkNamingFacade) services(page, size int, namespace string) ([]string, int, error) {
	result, err := f.client.GetAllServicesInfo(vo.GetAllServiceInfoParam{NameSpace: namespace, GroupName: f.group, PageNo: uint32(page), PageSize: uint32(size)})
	if err != nil {
		return nil, 0, classifySDKError(err)
	}
	return append([]string(nil), result.Doms...), int(result.Count), nil
}

func (f *sdkNamingFacade) subscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	return classifySDKError(f.client.Subscribe(&vo.SubscribeParam{ServiceName: service, GroupName: effectiveGroup(group), Clusters: clusters, SubscribeCallback: func(items []model.Instance, err error) {
		if err != nil {
			callback(nil, err)
			return
		}
		hosts := make([]Host, 0, len(items))
		for _, item := range items {
			hosts = append(hosts, Host{InstanceID: item.InstanceId, IP: item.Ip, Port: int(item.Port), Weight: item.Weight, Healthy: item.Healthy, Enabled: item.Enable, Ephemeral: item.Ephemeral, ClusterName: item.ClusterName, ServiceName: item.ServiceName, Metadata: item.Metadata})
		}
		callback(hosts, nil)
	}}))
}

func (f *sdkNamingFacade) unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	return classifySDKError(f.client.Unsubscribe(&vo.SubscribeParam{ServiceName: service, GroupName: effectiveGroup(group), Clusters: clusters, SubscribeCallback: func(items []model.Instance, err error) {
		if err != nil {
			callback(nil, err)
			return
		}
		hosts := make([]Host, 0, len(items))
		for _, item := range items {
			hosts = append(hosts, Host{InstanceID: item.InstanceId, IP: item.Ip, Port: int(item.Port), Weight: item.Weight, Healthy: item.Healthy, Enabled: item.Enable, Ephemeral: item.Ephemeral, ClusterName: item.ClusterName, ServiceName: item.ServiceName, Metadata: item.Metadata})
		}
		callback(hosts, nil)
	}}))
}

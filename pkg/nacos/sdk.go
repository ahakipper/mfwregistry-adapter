package nacos

// This file is the seam for the official Nacos Go SDK.  Naming operations are
// deliberately kept behind a tiny interface so the sink does not depend on
// SDK concrete types and unit tests can prove operation routing without a
// running Nacos server.  Catalog/admin endpoints are not exposed by the
// official naming client; those calls remain in client.go's explicitly named
// HTTP compatibility adapter until an official catalog SDK is verified.

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// TransportMode selects the Nacos wire adapter. SDK is the production path;
// HTTPCompat is a temporary migration/rollback path for catalog endpoints and
// legacy tests. An empty mode is resolved to SDK by NewClientWithConfig.
type TransportMode string

const (
	TransportSDK        TransportMode = "sdk"
	TransportHTTPCompat TransportMode = "http-compat"
)

// HTTPCompatibilityException records the only operations that are currently
// outside the official naming SDK surface. Keeping this registry next to the
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
	{Operation: "catalog/prune", Owner: "spotter-maintainers", ExpiresOn: "2026-10-31", RemovalCriteria: "official Nacos Admin/Maintainer SDK equivalent verified against target version"},
	{Operation: "cluster-health-check-update", Owner: "spotter-maintainers", ExpiresOn: "2026-10-31", RemovalCriteria: "official Nacos Admin/Maintainer SDK equivalent verified against target version"},
	{Operation: "console-readiness-probe", Owner: "spotter-maintainers", ExpiresOn: "2026-10-31", RemovalCriteria: "official Nacos Admin/Maintainer SDK equivalent verified against target version"},
}

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
	CloseClient()
}

var _ sdkNamingClient = (naming_client.INamingClient)(nil)

type sdkNamingFacade struct {
	client sdkNamingClient
	group  string
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
	clientCfg := &constant.ClientConfig{
		TimeoutMs:   timeoutMs,
		NamespaceId: ns,
		Username:    cfg.Username,
		Password:    cfg.Password,
		TLSCfg:      constant.TLSConfig{Appointed: true, Enable: servers[0].Scheme == "https", TrustAll: cfg.InsecureSkipVerify, CaFile: cfg.CAFile, ServerNameOverride: cfg.ServerName},
	}
	naming, err := clients.NewNamingClient(vo.NacosClientParam{ClientConfig: clientCfg, ServerConfigs: servers})
	if err != nil {
		return nil, fmt.Errorf("nacos sdk: create naming client: %w", err)
	}
	return &sdkNamingFacade{client: naming, group: effectiveGroup(cfg.GroupName)}, nil
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
	ok, err := f.client.RegisterInstance(vo.RegisterInstanceParam{Ip: p.IP, Port: uint64(p.Port), Weight: 1, Enable: p.Enabled, Healthy: p.Enabled, Metadata: p.Metadata, ClusterName: p.ClusterName, ServiceName: p.ServiceName, GroupName: effectiveGroup(group), Ephemeral: p.Ephemeral})
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

func (f *sdkNamingFacade) services(page, size int, namespace string) ([]string, error) {
	result, err := f.client.GetAllServicesInfo(vo.GetAllServiceInfoParam{NameSpace: namespace, GroupName: f.group, PageNo: uint32(page), PageSize: uint32(size)})
	if err != nil {
		return nil, classifySDKError(err)
	}
	return append([]string(nil), result.Doms...), nil
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

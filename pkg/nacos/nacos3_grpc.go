package nacos

// This file contains the concrete Nacos 3 naming gRPC adapter.  The pinned
// SDK's high-level NamingClient intentionally routes persistent instances to
// its legacy HTTP proxy; Spotter bypasses that delegate and owns a
// NamingGrpcProxy directly so every naming operation uses the official gRPC
// request types and method names.

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_grpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/http_agent"
	"github.com/nacos-group/nacos-sdk-go/v3/common/nacos_server"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/codec"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/rpc/rpc_request"
	"github.com/nacos-group/nacos-sdk-go/v3/common/security"
	"github.com/nacos-group/nacos-sdk-go/v3/inner/uuid"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
	namingproto "github.com/nacos-group/nacos-sdk-proto/go/naming"
)

type grpcSubscription struct {
	callback func([]Host, error)
	wrapper  *naming_cache.SubscribeCallbackFuncWrapper
}

type nacos3GRPCVendor struct {
	proxy      *naming_grpc.NamingGrpcProxy
	persistent rpc.IRpcClient
	server     *nacos_server.NacosServer
	holder     *naming_cache.ServiceInfoHolder
	fuzzy      *naming_cache.FuzzyWatchServiceListHolder
	cancel     context.CancelFunc
	namespace  string
	group      string
	timeoutMs  uint64

	mu   sync.Mutex
	subs map[string][]grpcSubscription
}

type persistentInstanceRequest struct {
	rpc_request.Request
	Namespace, ServiceName, GroupName, Type string
	Instance                                model.Instance
}

func (r *persistentInstanceRequest) GetRequestType() string { return "PersistentInstanceRequest" }
func (r *persistentInstanceRequest) ProtoMessage() proto.Message {
	return &namingproto.PersistentInstanceRequest{RequestId: r.RequestId, Namespace: r.Namespace, ServiceName: r.ServiceName, GroupName: r.GroupName, Type: r.Type, Instance: &namingproto.Instance{InstanceId: r.Instance.InstanceId, Ip: r.Instance.Ip, Port: int32(r.Instance.Port), Weight: r.Instance.Weight, Healthy: r.Instance.Healthy, Enabled: r.Instance.Enable, Ephemeral: false, ClusterName: r.Instance.ClusterName, ServiceName: r.Instance.ServiceName, Metadata: r.Instance.Metadata}}
}

var _ rpc_request.IRequest = (*persistentInstanceRequest)(nil)
var _ codec.ProtoConvertible = (*persistentInstanceRequest)(nil)

// grpcSDKClient is the small sdkNamingClient implementation used by the
// facade. It deliberately delegates every operation to nacos3GRPCVendor;
// keeping this shim avoids constructing the SDK's NamingClient delegate,
// whose persistent path is legacy HTTP.
type grpcSDKClient struct{ vendor *nacos3GRPCVendor }

func (c *grpcSDKClient) RegisterPersistentBatch(items []InstanceParams) error {
	return c.vendor.RegisterPersistentBatch(items)
}

func (c *grpcSDKClient) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	err := c.vendor.RegisterPersistent(InstanceParams{ServiceName: p.ServiceName, IP: p.Ip, Port: int(p.Port), ClusterName: p.ClusterName, GroupName: p.GroupName, Enabled: p.Enable, Ephemeral: false, Metadata: p.Metadata})
	return err == nil, err
}
func (c *grpcSDKClient) BatchRegisterInstance(vo.BatchRegisterInstanceParam) (bool, error) {
	return false, ErrUnsupportedOperation
}
func (c *grpcSDKClient) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	err := c.vendor.DeregisterPersistent(InstanceParams{ServiceName: p.ServiceName, IP: p.Ip, Port: int(p.Port), ClusterName: p.Cluster, GroupName: p.GroupName, Ephemeral: false})
	return err == nil, err
}
func (c *grpcSDKClient) UpdateInstance(p vo.UpdateInstanceParam) (bool, error) {
	err := c.vendor.RegisterPersistent(InstanceParams{ServiceName: p.ServiceName, IP: p.Ip, Port: int(p.Port), ClusterName: p.ClusterName, GroupName: p.GroupName, Enabled: p.Enable, Ephemeral: false, Metadata: p.Metadata})
	return err == nil, err
}
func (c *grpcSDKClient) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	hosts, err := c.vendor.SelectAll(p.ServiceName, firstCluster(p.Clusters), p.GroupName)
	if err != nil {
		return nil, err
	}
	result := make([]model.Instance, 0, len(hosts))
	for _, h := range hosts {
		result = append(result, model.Instance{InstanceId: h.InstanceID, Ip: h.IP, Port: uint64(h.Port), Weight: h.Weight, Healthy: h.Healthy, Enable: h.Enabled, Ephemeral: h.Ephemeral, ClusterName: h.ClusterName, ServiceName: h.ServiceName, Metadata: h.Metadata})
	}
	return result, nil
}
func (c *grpcSDKClient) GetAllServicesInfo(p vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	names, count, err := c.vendor.ListServices(int(p.PageNo), int(p.PageSize), p.NameSpace, p.GroupName)
	return model.ServiceList{Doms: names, Count: int64(count)}, err
}
func (c *grpcSDKClient) Subscribe(p *vo.SubscribeParam) error {
	return c.vendor.Subscribe(p.ServiceName, p.GroupName, p.Clusters, func(hosts []Host, err error) {
		if p.SubscribeCallback == nil {
			return
		}
		items := make([]model.Instance, 0, len(hosts))
		for _, h := range hosts {
			items = append(items, model.Instance{InstanceId: h.InstanceID, Ip: h.IP, Port: uint64(h.Port), Weight: h.Weight, Healthy: h.Healthy, Enable: h.Enabled, Ephemeral: h.Ephemeral, ClusterName: h.ClusterName, ServiceName: h.ServiceName, Metadata: h.Metadata})
		}
		p.SubscribeCallback(items, err)
	})
}
func (c *grpcSDKClient) Unsubscribe(p *vo.SubscribeParam) error {
	return c.vendor.Unsubscribe(p.ServiceName, p.GroupName, p.Clusters, nil)
}
func (c *grpcSDKClient) ServerHealthy() bool {
	return c.vendor.proxy != nil && c.vendor.proxy.ServerHealthy()
}
func (c *grpcSDKClient) CloseClient() { _ = c.vendor.Close() }

func firstCluster(clusters []string) string {
	if len(clusters) == 0 {
		return ""
	}
	return clusters[0]
}

func newNacos3GRPCVendor(cfg constant.ClientConfig, servers []constant.ServerConfig) (*nacos3GRPCVendor, error) {
	ctx, cancel := context.WithCancel(context.Background())
	agent := &http_agent.HttpAgent{TlsConfig: cfg.TLSCfg}
	server, err := nacos_server.NewNacosServer(ctx, servers, cfg, agent, cfg.TimeoutMs, cfg.Endpoint, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	ns := cfg.NamespaceId
	if ns == "" {
		ns = constant.DEFAULT_NAMESPACE_ID
	}
	holder := naming_cache.NewServiceInfoHolder(ns, cfg.CacheDir, cfg.UpdateCacheWhenEmpty, cfg.NotLoadCacheAtStart)
	fuzzy := naming_cache.NewFuzzyWatchServiceListHolder(ns)
	proxy, err := naming_grpc.NewNamingGrpcProxy(ctx, cfg, server, holder, fuzzy)
	if err != nil {
		holder.Close()
		cancel()
		return nil, err
	}
	fuzzy.SetRequester(proxy)
	fuzzy.Start()
	persistentID, err := uuid.NewV4()
	if err != nil {
		proxy.CloseClient()
		holder.Close()
		cancel()
		return nil, err
	}
	persistent, err := rpc.CreateClient(ctx, persistentID.String(), rpc.GRPC, map[string]string{}, server, &cfg.TLSCfg, cfg.AppConnLabels)
	if err != nil {
		proxy.CloseClient()
		holder.Close()
		cancel()
		return nil, err
	}
	persistent.GetRpcClient().Start()
	return &nacos3GRPCVendor{proxy: proxy, persistent: persistent, server: server, holder: holder, fuzzy: fuzzy, cancel: cancel, namespace: cfg.NamespaceId, group: constant.DEFAULT_GROUP, timeoutMs: cfg.TimeoutMs, subs: make(map[string][]grpcSubscription)}, nil
}

func (v *nacos3GRPCVendor) RegisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	instance := model.Instance{InstanceId: instanceID(p), Ip: p.IP, Port: uint64(p.Port), Weight: 1, Enable: p.Enabled, Healthy: p.Enabled, Ephemeral: false, ClusterName: p.ClusterName, ServiceName: p.ServiceName, Metadata: p.Metadata}
	req := &persistentInstanceRequest{Request: rpc_request.Request{Headers: map[string]string{}}, Namespace: v.namespace, ServiceName: p.ServiceName, GroupName: effectiveGroupValue(p.GroupName, v.group), Type: "registerInstance", Instance: instance}
	v.server.InjectSecurityInfo(req.GetHeaders(), security.BuildNamingResource(v.namespace, req.GroupName, req.ServiceName))
	response, err := v.persistent.GetRpcClient().Request(req, int64(v.proxyTimeout()))
	ok := response != nil && response.IsSuccess()
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return ErrUnsupportedOperation
	}
	return nil
}

// RegisterPersistentBatch loops persistent requests; it never uses the
// protocol BatchRegisterInstance operation.
func (v *nacos3GRPCVendor) RegisterPersistentBatch(items []InstanceParams) error {
	var first error
	for _, item := range items {
		if err := v.RegisterPersistent(item); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (v *nacos3GRPCVendor) proxyTimeout() uint64 {
	if v.timeoutMs == 0 {
		return uint64(RequestTimeout / time.Millisecond)
	}
	return v.timeoutMs
}

func (v *nacos3GRPCVendor) DeregisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	instance := model.Instance{Ip: p.IP, Port: uint64(p.Port), Ephemeral: false, ClusterName: p.ClusterName, ServiceName: p.ServiceName}
	req := &persistentInstanceRequest{Request: rpc_request.Request{Headers: map[string]string{}}, Namespace: v.namespace, ServiceName: p.ServiceName, GroupName: effectiveGroupValue(p.GroupName, v.group), Type: "deregisterInstance", Instance: instance}
	v.server.InjectSecurityInfo(req.GetHeaders(), security.BuildNamingResource(v.namespace, req.GroupName, req.ServiceName))
	response, err := v.persistent.GetRpcClient().Request(req, int64(v.proxyTimeout()))
	ok := response != nil && response.IsSuccess()
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return ErrUnsupportedOperation
	}
	return nil
}

func (v *nacos3GRPCVendor) SelectAll(service, cluster, group string) ([]Host, error) {
	result, err := v.proxy.QueryInstancesOfService(service, effectiveGroupValue(group, v.group), cluster, 0, false)
	if err != nil {
		return nil, classifySDKError(err)
	}
	if result == nil || result.Hosts == nil {
		return []Host{}, nil
	}
	return modelHosts(result.Hosts), nil
}

func (v *nacos3GRPCVendor) ListServices(page, size int, namespace, group string) ([]string, int, error) {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 100
	}
	if namespace == DefaultNamespaceID {
		namespace = ""
	}
	result, err := v.proxy.GetServiceList(uint32(page), uint32(size), effectiveGroupValue(group, v.group), namespace, &model.ExpressionSelector{})
	if err != nil {
		return nil, 0, classifySDKError(err)
	}
	return append([]string(nil), result.Doms...), int(result.Count), nil
}

func (v *nacos3GRPCVendor) Subscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	group = effectiveGroupValue(group, v.group)
	clusterText := strings.Join(clusters, ",")
	key := service + "\x00" + group + "\x00" + clusterText
	if callback != nil {
		cb := callback
		wrapper := naming_cache.NewSubscribeCallbackFuncWrapper(naming_cache.NewClusterSelector(clusters), funcPtr(&cb))
		// ServiceInfoHolder keys callbacks by service+group AND clusters. The
		// previous empty-cluster registration never matched ProcessService's
		// `...@@k8s` cache key, so official SDK Subscribe updates were cached but
		// the caller callback was never invoked.
		v.holder.RegisterCallback(serviceKey(service, group), clusterText, wrapper)
		v.mu.Lock()
		v.subs[key] = append(v.subs[key], grpcSubscription{callback: callback, wrapper: wrapper})
		v.mu.Unlock()
	}
	result, err := v.proxy.Subscribe(service, group, clusterText)
	if err != nil {
		return classifySDKError(err)
	}
	if result.Name == "" {
		result.Name, result.GroupName = service, group
	}
	v.holder.ProcessService(&result)
	return nil
}

func (v *nacos3GRPCVendor) Unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	group = effectiveGroupValue(group, v.group)
	clusterText := strings.Join(clusters, ",")
	key := service + "\x00" + group + "\x00" + clusterText
	v.mu.Lock()
	entries := v.subs[key]
	remaining := entries[:0]
	for _, entry := range entries {
		if callback == nil || reflect.ValueOf(entry.callback).Pointer() == reflect.ValueOf(callback).Pointer() {
			v.holder.DeregisterCallback(serviceKey(service, group), clusterText, entry.wrapper)
			continue
		}
		remaining = append(remaining, entry)
	}
	if len(remaining) == 0 {
		delete(v.subs, key)
	} else {
		v.subs[key] = remaining
	}
	v.mu.Unlock()
	if err := v.proxy.Unsubscribe(service, group, clusterText); err != nil {
		return classifySDKError(err)
	}
	return nil
}

func (v *nacos3GRPCVendor) Close() error {
	if v == nil {
		return nil
	}
	if v.fuzzy != nil {
		v.fuzzy.Shutdown()
	}
	if v.proxy != nil {
		v.proxy.CloseClient()
	}
	if v.persistent != nil {
		v.persistent.GetRpcClient().Shutdown()
	}
	if v.holder != nil {
		v.holder.Close()
	}
	if v.cancel != nil {
		v.cancel()
	}
	return nil
}

func modelHosts(items []model.Instance) []Host {
	result := make([]Host, 0, len(items))
	for _, item := range items {
		result = append(result, Host{InstanceID: item.InstanceId, IP: item.Ip, Port: int(item.Port), Weight: item.Weight, Healthy: item.Healthy, Enabled: item.Enable, Ephemeral: item.Ephemeral, ClusterName: item.ClusterName, ServiceName: item.ServiceName, Metadata: item.Metadata})
	}
	return result
}

func instanceID(p InstanceParams) string {
	return fmt.Sprintf("%s#%d#%s#%s@@%s", p.IP, p.Port, p.ClusterName, effectiveGroup(p.GroupName), p.ServiceName)
}

func effectiveGroupValue(group, fallback string) string {
	if group == "" {
		group = fallback
	}
	return effectiveGroup(group)
}

func serviceKey(service, group string) string { return effectiveGroup(group) + "@@" + service }

func funcPtr(fn *func([]Host, error)) *func([]model.Instance, error) {
	wrapped := func(instances []model.Instance, err error) {
		if err != nil {
			(*fn)(nil, err)
			return
		}
		(*fn)(modelHosts(instances), nil)
	}
	return &wrapped
}

var _ nacos3SDKVendor = (*nacos3GRPCVendor)(nil)

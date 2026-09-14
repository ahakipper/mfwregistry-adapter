package nacos

// This file contains the concrete Nacos 3 naming gRPC adapter.  The pinned
// SDK's high-level NamingClient intentionally routes persistent instances to
// its legacy HTTP proxy; Spotter bypasses that delegate and owns a
// NamingGrpcProxy directly so every naming operation uses the official gRPC
// request types and method names.

import (
	"context"
	"reflect"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_cache"
	"github.com/nacos-group/nacos-sdk-go/v3/clients/naming_client/naming_grpc"
	"github.com/nacos-group/nacos-sdk-go/v3/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v3/common/http_agent"
	"github.com/nacos-group/nacos-sdk-go/v3/common/nacos_server"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

type grpcSubscription struct {
	callback func([]Host, error)
	wrapper  *naming_cache.SubscribeCallbackFuncWrapper
}

type nacos3GRPCVendor struct {
	proxy     *naming_grpc.NamingGrpcProxy
	holder    *naming_cache.ServiceInfoHolder
	fuzzy     *naming_cache.FuzzyWatchServiceListHolder
	cancel    context.CancelFunc
	namespace string
	group     string

	mu   sync.Mutex
	subs map[string][]grpcSubscription
}

// grpcSDKClient is the small sdkNamingClient implementation used by the
// facade. It deliberately delegates every operation to nacos3GRPCVendor;
// keeping this shim avoids constructing the SDK's NamingClient delegate,
// whose persistent path is legacy HTTP.
type grpcSDKClient struct{ vendor *nacos3GRPCVendor }

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
	return &nacos3GRPCVendor{proxy: proxy, holder: holder, fuzzy: fuzzy, cancel: cancel, namespace: cfg.NamespaceId, group: constant.DEFAULT_GROUP, subs: make(map[string][]grpcSubscription)}, nil
}

func (v *nacos3GRPCVendor) RegisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	instance := model.Instance{Ip: p.IP, Port: uint64(p.Port), Weight: 1, Enable: p.Enabled, Healthy: p.Enabled, Ephemeral: false, ClusterName: p.ClusterName, ServiceName: p.ServiceName, Metadata: p.Metadata}
	ok, err := v.proxy.RegisterInstance(p.ServiceName, effectiveGroupValue(p.GroupName, v.group), instance)
	if err != nil {
		return classifySDKError(err)
	}
	if !ok {
		return ErrUnsupportedOperation
	}
	return nil
}

func (v *nacos3GRPCVendor) DeregisterPersistent(p InstanceParams) error {
	p.Ephemeral = false
	instance := model.Instance{Ip: p.IP, Port: uint64(p.Port), Ephemeral: false, ClusterName: p.ClusterName, ServiceName: p.ServiceName}
	ok, err := v.proxy.DeregisterInstance(p.ServiceName, effectiveGroupValue(p.GroupName, v.group), instance)
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
		v.holder.RegisterCallback(serviceKey(service, group), "", wrapper)
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
			v.holder.DeregisterCallback(serviceKey(service, group), "", entry.wrapper)
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

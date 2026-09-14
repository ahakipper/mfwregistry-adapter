package nacos

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

type fakeClusterAdmin struct {
	mu       sync.Mutex
	events   *[]string
	err      error
	closeErr error
	started  chan struct{}
	release  chan struct{}
	calls    int
	closes   int
}

func (a *fakeClusterAdmin) UpdateHealthChecker(ctx context.Context, namespace, group, service, cluster string) error {
	a.mu.Lock()
	a.calls++
	if a.events != nil {
		*a.events = append(*a.events, "admin:"+service+"/"+cluster)
	}
	started, release, err := a.started, a.release, a.err
	a.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (a *fakeClusterAdmin) Close(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closes++
	return a.closeErr
}

func (a *fakeClusterAdmin) count() int      { a.mu.Lock(); defer a.mu.Unlock(); return a.calls }
func (a *fakeClusterAdmin) closeCount() int { a.mu.Lock(); defer a.mu.Unlock(); return a.closes }

type fakeSDKNaming struct {
	mu           sync.Mutex
	registered   []vo.RegisterInstanceParam
	batched      []vo.BatchRegisterInstanceParam
	deregistered []vo.DeregisterInstanceParam
	selected     vo.SelectAllInstancesParam
	services     vo.GetAllServiceInfoParam
	instances    []model.Instance
	selectErr    error
	err          error
	serviceErr   error
	healthyState *bool
	servicePages map[uint32]model.ServiceList
	events       *[]string
}

func (f *fakeSDKNaming) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, p)
	if f.events != nil {
		*f.events = append(*f.events, "register:"+p.ServiceName+"/"+p.ClusterName)
	}
	return f.err == nil, f.err
}
func (f *fakeSDKNaming) BatchRegisterInstance(p vo.BatchRegisterInstanceParam) (bool, error) {
	f.batched = append(f.batched, p)
	return f.err == nil, f.err
}
func (f *fakeSDKNaming) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	f.deregistered = append(f.deregistered, p)
	return f.err == nil, f.err
}
func (f *fakeSDKNaming) UpdateInstance(vo.UpdateInstanceParam) (bool, error) { return true, nil }
func (f *fakeSDKNaming) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	f.selected = p
	if f.selectErr != nil {
		return nil, f.selectErr
	}
	return f.instances, nil
}
func (f *fakeSDKNaming) GetAllServicesInfo(p vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	f.services = p
	if f.serviceErr != nil {
		return model.ServiceList{}, f.serviceErr
	}
	if f.servicePages != nil {
		if page, ok := f.servicePages[p.PageNo]; ok {
			return page, nil
		}
	}
	return model.ServiceList{Count: 1, Doms: []string{"svc"}}, nil
}
func (f *fakeSDKNaming) Subscribe(*vo.SubscribeParam) error   { return nil }
func (f *fakeSDKNaming) Unsubscribe(*vo.SubscribeParam) error { return nil }
func (f *fakeSDKNaming) ServerHealthy() bool {
	if f.healthyState != nil {
		return *f.healthyState
	}
	return true
}
func (f *fakeSDKNaming) CloseClient() {}

func TestSDKFacadeRoutesPersistentLifecycleAndPreservesFields(t *testing.T) {
	fake := &fakeSDKNaming{}
	facade := &sdkNamingFacade{client: fake, group: "blue"}
	params := InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 8080, ClusterName: "k8s", Enabled: false, Ephemeral: false, Metadata: map[string]string{"owner": "spotter"}}
	if err := facade.register(params); err != nil {
		t.Fatal(err)
	}
	if len(fake.registered) != 1 || fake.registered[0].Ephemeral || fake.registered[0].GroupName != "blue" || fake.registered[0].Enable {
		t.Fatalf("register mapping = %+v", fake.registered)
	}
	if err := facade.deregister(params); err != nil {
		t.Fatal(err)
	}
	if len(fake.deregistered) != 1 || fake.deregistered[0].Ephemeral || fake.deregistered[0].Cluster != "k8s" {
		t.Fatalf("deregister mapping = %+v", fake.deregistered)
	}
}

func TestSDKFacadeSelectAllErrorNeverReturnsCachedInstances(t *testing.T) {
	fake := &fakeSDKNaming{instances: []model.Instance{{InstanceId: "stale"}}, selectErr: errors.New("subscribe failed")}
	f := &sdkNamingFacade{client: fake, group: DefaultGroup}
	got, err := f.list("svc", "k8s")
	if err == nil || got != nil {
		t.Fatalf("list() = %#v, %v; want nil data and explicit error", got, err)
	}
}

func TestSDKFacadeExposesBatchOperationForEphemeralOnlySDKContract(t *testing.T) {
	fake := &fakeSDKNaming{}
	facade := &sdkNamingFacade{client: fake, group: DefaultGroup}
	param := vo.BatchRegisterInstanceParam{ServiceName: "svc", GroupName: DefaultGroup, Instances: []vo.RegisterInstanceParam{{Ip: "10.0.0.1", Port: 80, Ephemeral: true}}}
	if err := facade.batchRegister(param); err != nil {
		t.Fatalf("batchRegister() error = %v", err)
	}
	if len(fake.batched) != 1 || len(fake.batched[0].Instances) != 1 || !fake.batched[0].Instances[0].Ephemeral {
		t.Fatalf("batch operation mapping = %+v, want one ephemeral SDK batch", fake.batched)
	}
}

func TestClientBatchRegisterEphemeralRejectsPersistent(t *testing.T) {
	fake := &fakeSDKNaming{}
	c := &Client{sdk: &sdkNamingFacade{client: fake, group: DefaultGroup}}
	err := c.BatchRegisterEphemeral(vo.BatchRegisterInstanceParam{Instances: []vo.RegisterInstanceParam{{Ip: "10.0.0.1", Port: 80, Ephemeral: false}}})
	if err == nil || len(fake.batched) != 0 {
		t.Fatalf("persistent batch err=%v calls=%d", err, len(fake.batched))
	}
}

func TestSDKFacadeBatchRegisterPropagatesPartialError(t *testing.T) {
	fake := &fakeSDKNaming{err: errors.New("partial batch failure")}
	facade := &sdkNamingFacade{client: fake, group: DefaultGroup}
	err := facade.batchRegister(vo.BatchRegisterInstanceParam{ServiceName: "svc", Instances: []vo.RegisterInstanceParam{{Ip: "10.0.0.1", Port: 80, Ephemeral: true}}})
	if err == nil || len(fake.batched) != 1 {
		t.Fatalf("batch error=%v calls=%d, want propagated error and one call", err, len(fake.batched))
	}
}

func TestSDKFacadePreservesPermanentStatusClassification(t *testing.T) {
	facade := &sdkNamingFacade{client: &fakeSDKNaming{err: errors.New("retry 3 times request failed!: request return error code 400")}, group: DefaultGroup}
	err := facade.register(InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80, ClusterName: "k8s"})
	if err == nil {
		t.Fatal("register() error = nil, want SDK 400 error")
	}
	var permanent interface{ Permanent() bool }
	if !errors.As(err, &permanent) || !permanent.Permanent() {
		t.Fatalf("SDK error = %T (%v), want a permanent 4xx classification", err, err)
	}
}

func TestClassifySDKErrorAcceptsStatusCodeFormatting(t *testing.T) {
	err := classifySDKError(errors.New("request failed: status code: 403"))
	var permanent interface{ Permanent() bool }
	if !errors.As(err, &permanent) || !permanent.Permanent() {
		t.Fatalf("classified SDK status error = %T (%v), want permanent 403", err, err)
	}
}

func TestSDKFacadeSelectAllIncludesDisabledAndMapsHosts(t *testing.T) {
	fake := &fakeSDKNaming{instances: []model.Instance{{InstanceId: "id", Ip: "10.0.0.2", Port: 81, Enable: false, Healthy: false, ClusterName: "ecs", ServiceName: "svc", Metadata: map[string]string{"k": "v"}}}}
	facade := &sdkNamingFacade{client: fake, group: "blue"}
	hosts, err := facade.list("svc", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 || hosts[0].Enabled || hosts[0].ClusterName != "ecs" || fake.selected.GroupName != "blue" || len(fake.selected.Clusters) != 0 {
		t.Fatalf("hosts=%+v selected=%+v", hosts, fake.selected)
	}
}

func TestSDKFacadeServiceListCarriesNamespaceAndGroup(t *testing.T) {
	fake := &fakeSDKNaming{}
	facade := &sdkNamingFacade{client: fake, group: "blue"}
	names, count, err := facade.services(2, 50, "tenant-a")
	if err != nil || count != 1 || len(names) != 1 || fake.services.NameSpace != "tenant-a" || fake.services.GroupName != "blue" || fake.services.PageNo != 2 || fake.services.PageSize != 50 {
		t.Fatalf("names=%v param=%+v err=%v", names, fake.services, err)
	}
}

func TestSDKServiceListHonorsDeclaredCountWhenPageIsClamped(t *testing.T) {
	fake := &fakeSDKNaming{servicePages: map[uint32]model.ServiceList{
		1: {Count: 3, Doms: []string{"svc-a"}},
		2: {Count: 3, Doms: []string{"svc-b"}},
		3: {Count: 3, Doms: []string{"svc-c"}},
	}}
	c := &Client{config: ClientConfig{}, sdk: &sdkNamingFacade{client: fake, group: DefaultGroup}}
	services, err := c.ListServices(2)
	if err != nil {
		t.Fatalf("ListServices() error = %v", err)
	}
	if len(services) != 3 || services[0] != "svc-a" || services[2] != "svc-c" {
		t.Fatalf("ListServices() = %v, want all three services", services)
	}
	if fake.services.PageNo != 3 {
		t.Fatalf("last service-list page = %d, want 3 after clamped short pages", fake.services.PageNo)
	}
}

func TestNormalizeNacosURLRejectsQueryCredentials(t *testing.T) {
	if _, err := normalizeNacosURL("http://127.0.0.1:8848?accessToken=x"); err == nil {
		t.Fatal("expected query credentials to be rejected")
	}
}

func TestSDKFacadeRejectsMixedServerSchemes(t *testing.T) {
	if _, err := newSDKNamingFacade(ClientConfig{ServerURLs: []string{
		"http://127.0.0.1:8848", "https://127.0.0.1:8848",
	}}); err == nil || !strings.Contains(err.Error(), "mixed server URL schemes") {
		t.Fatalf("newSDKNamingFacade(mixed schemes) error = %v, want explicit rejection", err)
	}
}

func TestSDKFacadeRejectsUnsupportedStaticAccessToken(t *testing.T) {
	if _, err := newSDKNamingFacade(ClientConfig{ServerURL: "127.0.0.1:8848", AccessToken: "token"}); err == nil {
		t.Fatal("newSDKNamingFacade() error = nil, want static access token rejected")
	}
}

func TestNewSDKNamingFacadeBuildsOfficialClientWithoutDialing(t *testing.T) {
	facade, err := newSDKNamingFacade(ClientConfig{ServerURL: "127.0.0.1:18848", NamespaceID: "public", GroupName: "blue", Timeout: 250 * time.Millisecond})
	if err != nil {
		t.Fatalf("newSDKNamingFacade() error = %v", err)
	}
	if facade == nil || facade.client == nil || facade.group != "blue" {
		t.Fatalf("facade = %#v, want initialized SDK client and group", facade)
	}
	facade.client.CloseClient()
}

func TestClientSDKModeDoesNotUseHTTPForNamingOperations(t *testing.T) {
	fake := &fakeSDKNaming{}
	c := &Client{sdk: &sdkNamingFacade{client: fake, group: "blue"}}
	if err := c.RegisterInstance(InstanceParams{ServiceName: "svc", IP: "127.0.0.1", Port: 80}); err != nil {
		t.Fatal(err)
	}
	if len(fake.registered) != 1 {
		t.Fatalf("SDK register calls = %d, want 1", len(fake.registered))
	}
}

func TestSDKClientDoesNotAllocateCompatibilityHTTPTransport(t *testing.T) {
	c, err := NewClientWithConfig(ClientConfig{
		TransportMode: TransportSDK,
		ServerURL:     "127.0.0.1:18848",
	}, nil)
	if err != nil {
		t.Fatalf("NewClientWithConfig(sdk) error = %v", err)
	}
	defer func() { _ = c.Close() }()
	if c.http != nil {
		t.Fatal("SDK client allocated raw HTTP transport; production SDK path must not own a compatibility client")
	}
}

func TestSDKUnsupportedClusterAdminFailsClosed(t *testing.T) {
	c := &Client{sdk: &sdkNamingFacade{client: &fakeSDKNaming{}, group: DefaultGroup}}
	err := c.UpdateCluster("svc", "k8s")
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("UpdateCluster(sdk) error = %v, want ErrUnsupportedOperation", err)
	}
}

func TestSDKSinkConstructionDeploymentOwnedPolicyDefaultsToNoAdmin(t *testing.T) {
	_, err := NewSinkWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, HealthPolicy: HealthPolicyDeploymentOwned}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewSinkWithConfig(deployment-owned policy) error = %v, want nil", err)
	}

	_, err = NewSinkWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewSinkWithConfig(default policy) error = %v, want nil", err)
	}
}

func TestSDKSinkConstructionNeedsAdminWhenHealthPolicyIsAdminManaged(t *testing.T) {
	_, err := NewSinkWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, HealthPolicy: HealthPolicyAdminManaged}, ports.NopLogger{})
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("NewSinkWithConfig(admin-managed policy) error = %v, want ErrUnsupportedOperation", err)
	}
	if !strings.Contains(err.Error(), "cluster-health-check-update requires") {
		t.Fatalf("NewSinkWithConfig(admin-managed) error = %q, want preflight reason", err)
	}
}

func TestSDKClientConfigAdminFactoryIsInvoked(t *testing.T) {
	calls := 0
	admin := &fakeClusterAdmin{}
	client, err := NewClientWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, ClusterAdminFactory: func() (NacosClusterAdmin, error) {
		calls++
		return admin, nil
	}, HealthPolicy: HealthPolicyAdminManaged}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	if calls != 1 || client.clusterAdmin != admin || client.http != nil {
		t.Fatalf("factory calls=%d admin=%v http=%v", calls, client.clusterAdmin == admin, client.http != nil)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSDKClientConfigAdminFactoryIsNotInvokedForDeploymentOwnedPolicy(t *testing.T) {
	calls := 0
	client, err := NewClientWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, ClusterAdminFactory: func() (NacosClusterAdmin, error) {
		calls++
		return &fakeClusterAdmin{}, nil
	}}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("factory calls=%d, want 0 for deployment-owned policy", calls)
	}
	if client.config.HealthPolicy != HealthPolicyDeploymentOwned {
		t.Fatalf("client policy = %q, want %q", client.config.HealthPolicy, HealthPolicyDeploymentOwned)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSDKClientZeroHealthPolicyDefaultsToDeploymentOwned(t *testing.T) {
	client, err := NewClientWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewClientWithConfig() error = %v", err)
	}
	if client.config.HealthPolicy != HealthPolicyDeploymentOwned {
		t.Fatalf("client policy = %q, want %q", client.config.HealthPolicy, HealthPolicyDeploymentOwned)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSDKClientRejectsInvalidHealthPolicy(t *testing.T) {
	_, err := NewClientWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, HealthPolicy: "invalid"}, ports.NopLogger{})
	if err == nil {
		t.Fatal("NewClientWithConfig() error = nil, want unsupported health policy")
	}
	if !strings.Contains(err.Error(), "unsupported health policy") {
		t.Fatalf("NewClientWithConfig() error = %q, want unsupported health policy", err)
	}
}

func TestSDKSinkAdminFactoryAllowsConstruction(t *testing.T) {
	admin := &fakeClusterAdmin{}
	sink, err := NewSinkWithConfig(ClientConfig{ServerURL: "127.0.0.1:8848", TransportMode: TransportSDK, HealthPolicy: HealthPolicyAdminManaged, ClusterAdminFactory: func() (NacosClusterAdmin, error) {
		return admin, nil
	}}, ports.NopLogger{})
	if err != nil {
		t.Fatalf("NewSinkWithConfig(factory) error = %v", err)
	}
	if sink.client.clusterAdmin != admin || sink.client.http != nil {
		t.Fatalf("sink factory wiring admin=%v http=%v", sink.client.clusterAdmin == admin, sink.client.http != nil)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSDKRuntimeNamingCallsDoNotRequireAdminUnderDeploymentPolicy(t *testing.T) {
	fake := &fakeSDKNaming{}
	client := &Client{config: ClientConfig{HealthPolicy: HealthPolicyDeploymentOwned}, sdk: &sdkNamingFacade{client: fake, group: DefaultGroup}}
	if err := client.RegisterInstance(InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80}); err != nil {
		t.Fatalf("RegisterInstance() error = %v", err)
	}
	if _, err := client.ListInstances("svc"); err != nil {
		t.Fatalf("ListInstances() error = %v", err)
	}
	if err := client.DeregisterInstance(InstanceParams{ServiceName: "svc", IP: "10.0.0.1", Port: 80}); err != nil {
		t.Fatalf("DeregisterInstance() error = %v", err)
	}
	if err := client.Subscribe("svc", DefaultGroup, nil, func(_ []Host, _ error) {}); err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := client.Unsubscribe("svc", DefaultGroup, nil, func(_ []Host, _ error) {}); err != nil {
		t.Fatalf("Unsubscribe() error = %v", err)
	}
}

func TestSDKSinkPushUnderDeploymentOwnedPolicySkipsAdminCalls(t *testing.T) {
	naming := &fakeSDKNaming{}
	admin := &fakeClusterAdmin{}
	sink := &Sink{
		client: &Client{
			sdk:          &sdkNamingFacade{client: naming, group: DefaultGroup},
			clusterAdmin: admin,
			config:       ClientConfig{HealthPolicy: HealthPolicyDeploymentOwned},
		},
		logger:    ports.NopLogger{},
		groupName: DefaultGroup,
	}
	if err := sink.Push(1, []*instance.Instance{{InstanceId: "pod-a", AppCode: "svc", Provider: "k8s", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true}}); err != nil {
		t.Fatalf("Push() error = %v", err)
	}
	if admin.count() != 0 {
		t.Fatalf("admin update calls = %d, want 0 for deployment-owned policy", admin.count())
	}
	if got := len(naming.registered); got != 1 {
		t.Fatalf("naming register calls = %d, want 1", got)
	}
}

func TestSDKAdminManagedPolicyStillPreflightsAdminForPush(t *testing.T) {
	fake := &fakeSDKNaming{}
	sink := &Sink{
		client: &Client{sdk: &sdkNamingFacade{client: fake, group: DefaultGroup}, config: ClientConfig{HealthPolicy: HealthPolicyAdminManaged}},
		logger: ports.NopLogger{},
	}
	ins := &instance.Instance{InstanceId: "pod-a", AppCode: "svc", Provider: "k8s", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true}
	err := sink.Push(1, []*instance.Instance{ins})
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("SDK sink Push(admin-managed) error = %v, want ErrUnsupportedOperation", err)
	}
	if len(fake.registered) != 0 {
		t.Fatalf("SDK sink issued %d remote register calls despite missing admin", len(fake.registered))
	}
}

func adminTestSink(admin NacosClusterAdmin, naming *fakeSDKNaming, timeout time.Duration, healthPolicy HealthPolicy) *Sink {
	return &Sink{
		client: &Client{sdk: &sdkNamingFacade{client: naming, group: DefaultGroup}, clusterAdmin: admin, config: ClientConfig{Timeout: timeout, HealthPolicy: healthPolicy}},
		logger: ports.NopLogger{}, groupName: DefaultGroup,
	}
}

func adminTestInstance() *instance.Instance {
	return &instance.Instance{InstanceId: "pod-a", AppCode: "svc", Provider: "k8s", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true}
}

func TestSDKAdminRunsBeforeBusinessRegister(t *testing.T) {
	events := []string{}
	naming := &fakeSDKNaming{events: &events}
	admin := &fakeClusterAdmin{events: &events}
	sink := adminTestSink(admin, naming, time.Second, HealthPolicyAdminManaged)
	if err := sink.register(adminTestInstance()); err != nil {
		t.Fatalf("register() error = %v", err)
	}
	if len(events) != 2 || events[0] != "admin:svc/k8s" || events[1] != "register:svc/k8s" {
		t.Fatalf("events = %v, want admin before register", events)
	}
}

func TestSDKAdminFailurePreventsBusinessRegister(t *testing.T) {
	naming := &fakeSDKNaming{}
	admin := &fakeClusterAdmin{err: errors.New("admin unavailable")}
	sink := adminTestSink(admin, naming, time.Second, HealthPolicyAdminManaged)
	err := sink.register(adminTestInstance())
	if err == nil {
		t.Fatal("register() error = nil, want admin failure")
	}
	if len(naming.registered) != 0 {
		t.Fatalf("business register calls = %d, want 0", len(naming.registered))
	}
	var retryable interface{ Retryable() bool }
	if !errors.As(err, &retryable) || !retryable.Retryable() {
		t.Fatalf("register error = %T (%v), want retryable admin error", err, err)
	}
}

func TestSDKAdminConcurrentRegistrationsShareOneUpdate(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	admin := &fakeClusterAdmin{started: started, release: release}
	naming := &fakeSDKNaming{}
	sink := adminTestSink(admin, naming, time.Second, HealthPolicyAdminManaged)
	results := make(chan error, 2)
	go func() { results <- sink.register(adminTestInstance()) }()
	<-started
	go func() { results <- sink.register(adminTestInstance()) }()
	select {
	case <-time.After(100 * time.Millisecond):
		// The second register must wait for the first admin claim.
	case <-results:
		t.Fatal("concurrent register completed before admin update release")
	}
	close(release)
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if admin.count() != 1 {
		t.Fatalf("admin calls = %d, want 1", admin.count())
	}
}

func TestSDKAdminTimeoutPreventsRegister(t *testing.T) {
	admin := &fakeClusterAdmin{release: make(chan struct{})}
	sink := adminTestSink(admin, &fakeSDKNaming{}, 20*time.Millisecond, HealthPolicyAdminManaged)
	if err := sink.register(adminTestInstance()); err == nil {
		t.Fatal("register() error = nil, want timeout")
	}
}

func TestClientAdminCloseIsBoundedAndIdempotent(t *testing.T) {
	admin := &fakeClusterAdmin{closeErr: errors.New("close failed")}
	client := &Client{clusterAdmin: admin, config: ClientConfig{Timeout: time.Second}}
	if err := client.Close(); err == nil || !strings.Contains(err.Error(), "close failed") {
		t.Fatalf("Close() error = %v, want admin close failure", err)
	}
	if err := client.Close(); err == nil || admin.closeCount() != 1 {
		t.Fatalf("second Close() error=%v calls=%d, want idempotent one close", err, admin.closeCount())
	}
}

func TestSDKRawHTTPHelpersFailClosed(t *testing.T) {
	c := &Client{}
	if err := c.doForm("POST", "/nacos/v1/ns/instance", nil); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("doForm() error = %v, want ErrUnsupportedOperation", err)
	}
	if err := c.doJSON("GET", "/nacos/v1/ns/instance/list", nil, &Hosts{}); !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("doJSON() error = %v, want ErrUnsupportedOperation", err)
	}
}

func TestSDKReadinessUsesNamingReadAndPersistentCanary(t *testing.T) {
	fake := &fakeSDKNaming{}
	c := &Client{
		config: ClientConfig{GroupName: "blue"},
		sdk:    &sdkNamingFacade{client: fake, group: "blue"},
	}
	if err := checkReadinessSDK(c); err != nil {
		t.Fatalf("checkReadinessSDK() error = %v", err)
	}
	if fake.services.PageNo != 1 || fake.services.PageSize != 1 || fake.services.GroupName != "blue" {
		t.Fatalf("readiness service-list params = %+v", fake.services)
	}
	if len(fake.registered) != 1 || len(fake.deregistered) != 1 {
		t.Fatalf("readiness canary calls = register:%d deregister:%d, want 1/1", len(fake.registered), len(fake.deregistered))
	}
	if fake.registered[0].Ephemeral || fake.deregistered[0].Ephemeral {
		t.Fatal("readiness canary must use persistent SDK lifecycle")
	}
}

func TestSDKReadinessFailsClosedWhenReadFails(t *testing.T) {
	fake := &fakeSDKNaming{serviceErr: errors.New("grpc unavailable")}
	c := &Client{sdk: &sdkNamingFacade{client: fake, group: DefaultGroup}}
	err := checkReadinessSDK(c)
	if err == nil || !strings.Contains(err.Error(), "read") {
		t.Fatalf("checkReadinessSDK() error = %v, want read failure", err)
	}
	if len(fake.registered) != 0 || len(fake.deregistered) != 0 {
		t.Fatalf("readiness canary calls after failed read = register:%d deregister:%d, want 0/0", len(fake.registered), len(fake.deregistered))
	}
}

func TestSDKFacadeCatalogUsesSelectAllAndIncludesRequestedCluster(t *testing.T) {
	fake := &fakeSDKNaming{instances: []model.Instance{{
		InstanceId: "id", Ip: "10.0.0.2", Port: 81, Enable: false,
		Healthy: false, ClusterName: "ecs", ServiceName: "svc",
	}}}
	facade := &sdkNamingFacade{client: fake, group: "blue"}
	hosts, err := facade.catalog("svc", "ecs")
	if err != nil {
		t.Fatalf("catalog() error = %v", err)
	}
	if len(hosts) != 1 || hosts[0].ClusterName != "ecs" || hosts[0].Enabled {
		t.Fatalf("catalog hosts = %+v, want disabled ecs host", hosts)
	}
	if fake.selected.GroupName != "blue" || len(fake.selected.Clusters) != 1 || fake.selected.Clusters[0] != "ecs" {
		t.Fatalf("SelectAllInstances params = %+v, want group blue and ecs cluster", fake.selected)
	}
}

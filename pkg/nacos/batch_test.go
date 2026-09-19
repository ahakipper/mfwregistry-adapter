package nacos

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v3/model"
	"github.com/nacos-group/nacos-sdk-go/v3/vo"

	"spotter/internal/domain/instance"
)

func TestSplitPersistentBatchesGroupsByApplicationScopeAndOperation(t *testing.T) {
	items := []*instance.Instance{
		{InstanceId: "pay-k8s-online", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "pay-k8s-unhealthy", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusUnhealthy},
		{InstanceId: "pay-ecs-online", AppCode: "pay", Provider: "ecs", Status: instance.InstanceStatusOnline},
		{InstanceId: "pay-k8s-offline", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOffline},
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := len(batches), 3; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}

	if got := batchSizes(batches); !reflect.DeepEqual(got, []int{2, 1, 1}) {
		t.Fatalf("batch sizes = %v, want [2 1 1]", got)
	}
	wantKeys := []persistentBatchKey{
		{Namespace: DefaultNamespaceID, Group: DefaultGroup, Service: "pay", Cluster: "k8s", Operation: batchRegister},
		{Namespace: DefaultNamespaceID, Group: DefaultGroup, Service: "pay", Cluster: "ecs", Operation: batchRegister},
		{Namespace: DefaultNamespaceID, Group: DefaultGroup, Service: "pay", Cluster: "k8s", Operation: batchDeregister},
	}
	for i, want := range wantKeys {
		if got := batches[i].Key; got != want {
			t.Errorf("batch[%d] key = %+v, want %+v", i, got, want)
		}
	}
	wantIDs := [][]string{
		{"pay-k8s-online", "pay-k8s-unhealthy"},
		{"pay-ecs-online"},
		{"pay-k8s-offline"},
	}
	for i, want := range wantIDs {
		if got := instanceIDs(batches[i].Items); !reflect.DeepEqual(got, want) {
			t.Errorf("batch[%d] instance IDs = %v, want %v", i, got, want)
		}
	}
}

func TestSplitPersistentBatchesHardCapsEachApplicationAt100(t *testing.T) {
	items := make([]*instance.Instance, 201)
	for i := range items {
		items[i] = &instance.Instance{
			InstanceId: fmt.Sprintf("pay-k8s-%03d", i),
			AppCode:    "pay",
			Provider:   "k8s",
			Status:     instance.InstanceStatusOnline,
		}
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := batchSizes(batches), []int{100, 100, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch sizes = %v, want %v", got, want)
	}
}

func TestSplitPersistentBatchesScalesTo10001Instances(t *testing.T) {
	const total = 10001
	items := make([]*instance.Instance, total)
	for i := range items {
		items[i] = &instance.Instance{
			InstanceId: fmt.Sprintf("large-%05d", i),
			AppCode:    "large-app",
			Provider:   "k8s",
			Status:     instance.InstanceStatusOnline,
		}
	}
	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := len(batches), 101; got != want {
		t.Fatalf("batch count = %d, want %d for %d instances", got, want, total)
	}
	if got := len(batches[len(batches)-1].Items); got != 1 {
		t.Fatalf("final batch size = %d, want 1", got)
	}
	count := 0
	for i, batch := range batches {
		if len(batch.Items) == 0 || len(batch.Items) > MaxPersistentBatchSize {
			t.Fatalf("batch[%d] size = %d, outside 1..%d", i, len(batch.Items), MaxPersistentBatchSize)
		}
		if batch.Key.Service != "large-app" || batch.Key.Cluster != "k8s" || batch.Key.Operation != batchRegister {
			t.Fatalf("batch[%d] key = %+v, want one application/register scope", i, batch.Key)
		}
		count += len(batch.Items)
	}
	if count != total {
		t.Fatalf("partitioned item count = %d, want %d", count, total)
	}
}

func TestSplitPersistentBatchesPreservesStableInputOrder(t *testing.T) {
	items := []*instance.Instance{
		{InstanceId: "first", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "second", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "third", AppCode: "other", Provider: "k8s", Status: instance.InstanceStatusOnline},
		{InstanceId: "fourth", AppCode: "pay", Provider: "k8s", Status: instance.InstanceStatusOnline},
	}

	batches := splitPersistentBatches(items, MaxPersistentBatchSize)
	if got, want := len(batches), 2; got != want {
		t.Fatalf("batch count = %d, want %d", got, want)
	}
	if got := instanceIDs(batches[0].Items); !reflect.DeepEqual(got, []string{"first", "second", "fourth"}) {
		t.Fatalf("first application order = %v, want [first second fourth]", got)
	}
	if got := instanceIDs(batches[1].Items); !reflect.DeepEqual(got, []string{"third"}) {
		t.Fatalf("second application order = %v, want [third]", got)
	}
}

func batchSizes(batches []persistentBatch) []int {
	sizes := make([]int, len(batches))
	for i, batch := range batches {
		sizes[i] = len(batch.Items)
	}
	return sizes
}

func instanceIDs(items []*instance.Instance) []string {
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.InstanceId
	}
	return ids
}

type batchRecorder struct {
	mu           sync.Mutex
	active       int
	max          int
	order        []string
	registered   []vo.RegisterInstanceParam
	deregistered []vo.DeregisterInstanceParam
	errByIP      map[string]error
	listCall     int
	delay        time.Duration
}

func (r *batchRecorder) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.max {
		r.max = r.active
	}
	r.order = append(r.order, p.Ip)
	r.registered = append(r.registered, p)
	err := r.errByIP[p.Ip]
	r.mu.Unlock()
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return err == nil, err
}
func (r *batchRecorder) BatchRegisterInstance(vo.BatchRegisterInstanceParam) (bool, error) {
	return true, nil
}
func (r *batchRecorder) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.max {
		r.max = r.active
	}
	r.order = append(r.order, p.Ip)
	r.deregistered = append(r.deregistered, p)
	err := r.errByIP[p.Ip]
	r.mu.Unlock()
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	return err == nil, err
}
func (r *batchRecorder) UpdateInstance(vo.UpdateInstanceParam) (bool, error) { return true, nil }
func (r *batchRecorder) SelectAllInstances(query vo.SelectAllInstancesParam) ([]model.Instance, error) {
	r.mu.Lock()
	r.listCall++
	registered := append([]vo.RegisterInstanceParam(nil), r.registered...)
	r.mu.Unlock()
	result := make([]model.Instance, 0, len(registered))
	for _, p := range registered {
		if query.ServiceName != "" && p.ServiceName != query.ServiceName {
			continue
		}
		if len(query.Clusters) > 0 {
			matched := false
			for _, cluster := range query.Clusters {
				if cluster == p.ClusterName {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		healthy := p.Enable
		if !p.Healthy {
			healthy = false
		}
		result = append(result, model.Instance{
			InstanceId: fmt.Sprintf("%s#%d#%s#%s@@%s", p.Ip, p.Port, p.ClusterName, effectiveGroup(p.GroupName), p.ServiceName),
			Ip:         p.Ip, Port: p.Port, ClusterName: p.ClusterName, ServiceName: p.ServiceName,
			Enable: p.Enable, Healthy: healthy, Ephemeral: p.Ephemeral, Metadata: p.Metadata,
		})
	}
	return result, nil
}
func (r *batchRecorder) GetAllServicesInfo(vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	return model.ServiceList{}, nil
}
func (r *batchRecorder) Subscribe(*vo.SubscribeParam) error   { return nil }
func (r *batchRecorder) Unsubscribe(*vo.SubscribeParam) error { return nil }
func (r *batchRecorder) ServerHealthy() bool                  { return true }
func (r *batchRecorder) CloseClient()                         {}

type successfulBatchAdmin struct{}

func (successfulBatchAdmin) UpdateHealthChecker(context.Context, string, string, string, string) error {
	return nil
}
func (successfulBatchAdmin) Close(context.Context) error { return nil }

type persistentVisibilityVendor struct {
	mu               sync.Mutex
	items            map[string]InstanceParams
	registerErrByIP  map[string]error
	registerAttempts []string
	deregistered     []InstanceParams
	hiddenIPs        map[string]bool
	selectErr        error
	selectCalls      int
}

func (v *persistentVisibilityVendor) RegisterPersistent(p InstanceParams) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.registerAttempts = append(v.registerAttempts, p.IP)
	if err := v.registerErrByIP[p.IP]; err != nil {
		return err
	}
	if v.items == nil {
		v.items = map[string]InstanceParams{}
	}
	v.items[instanceID(p)] = p
	return nil
}
func (v *persistentVisibilityVendor) DeregisterPersistent(p InstanceParams) error {
	v.mu.Lock()
	v.deregistered = append(v.deregistered, p)
	delete(v.items, instanceID(p))
	v.mu.Unlock()
	return nil
}
func (v *persistentVisibilityVendor) SelectAll(service, cluster, group string) ([]Host, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.selectCalls++
	if v.selectErr != nil {
		return nil, v.selectErr
	}
	hosts := make([]Host, 0, len(v.items))
	for id, p := range v.items {
		if p.ServiceName != service || p.ClusterName != cluster || v.hiddenIPs[p.IP] {
			continue
		}
		healthy := p.Enabled
		if p.Healthy != nil {
			healthy = *p.Healthy
		}
		hosts = append(hosts, Host{InstanceID: id, IP: p.IP, Port: p.Port, ClusterName: p.ClusterName, ServiceName: p.ServiceName, Enabled: p.Enabled, Healthy: healthy, Ephemeral: p.Ephemeral, Metadata: p.Metadata})
	}
	return hosts, nil
}
func (*persistentVisibilityVendor) ListServices(int, int, string, string) ([]string, int, error) {
	return nil, 0, nil
}
func (*persistentVisibilityVendor) Subscribe(string, string, []string, func([]Host, error)) error {
	return nil
}
func (*persistentVisibilityVendor) Unsubscribe(string, string, []string, func([]Host, error)) error {
	return nil
}
func (*persistentVisibilityVendor) Close() error { return nil }

func newBatchTestSink(recorder *batchRecorder) *Sink {
	return &Sink{
		client:    &Client{sdk: &sdkNamingFacade{client: recorder, group: DefaultGroup}, clusterAdmin: successfulBatchAdmin{}, config: ClientConfig{}},
		logger:    nopBatchLogger{},
		groupName: DefaultGroup,
	}
}

type nopBatchLogger struct{}

func (nopBatchLogger) Info(...interface{})           {}
func (nopBatchLogger) Debugf(string, ...interface{}) {}
func (nopBatchLogger) Infof(string, ...interface{})  {}
func (nopBatchLogger) Warn(...interface{})           {}
func (nopBatchLogger) Warnf(string, ...interface{})  {}
func (nopBatchLogger) Error(...interface{})          {}
func (nopBatchLogger) Errorf(string, ...interface{}) {}

func TestPushPersistentBatchesLimitsRequestsAndPreservesApplicationOrder(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{}, delay: 2 * time.Millisecond}
	sink := newBatchTestSink(recorder)
	items := make([]*instance.Instance, 0, MaxPersistentBatchSize+1)
	for i := 0; i < MaxPersistentBatchSize+1; i++ {
		items = append(items, &instance.Instance{InstanceId: fmt.Sprintf("a-%03d", i), AppCode: "app-a", Provider: "k8s", Ip: fmt.Sprintf("10.0.0.%d", i+1), Status: instance.InstanceStatusOnline, Enabled: true})
	}
	items = append(items, &instance.Instance{InstanceId: "app-b", AppCode: "app-b", Provider: "k8s", Ip: "10.0.1.1", Status: instance.InstanceStatusUnhealthy, Enabled: true})
	SetPushConcurrency(8)
	t.Cleanup(func() { SetPushConcurrency(DefaultPushConcurrency) })
	if err := sink.pushPersistentBatches(items); err != nil {
		t.Fatalf("pushPersistentBatches() error = %v", err)
	}
	recorder.mu.Lock()
	max, order := recorder.max, append([]string(nil), recorder.order...)
	registered := append([]vo.RegisterInstanceParam(nil), recorder.registered...)
	recorder.mu.Unlock()
	if max > 8 {
		t.Fatalf("max concurrent SDK calls = %d, want <= 8", max)
	}
	if len(order) != len(items) {
		t.Fatalf("SDK calls = %d, want %d", len(order), len(items))
	}
	for _, param := range registered {
		if param.ServiceName != "app-a" && param.ServiceName != "app-b" {
			t.Fatalf("register service = %q, want app-a or app-b", param.ServiceName)
		}
		if param.GroupName != DefaultGroup || param.ClusterName != "k8s" || param.Ephemeral {
			t.Fatalf("register parameters = %+v, want DEFAULT_GROUP/k8s/persistent", param)
		}
		if param.ServiceName == "app-b" && (!param.Enable || param.Healthy) {
			t.Fatalf("unhealthy register parameters = %+v, want query-visible Enable=true and Healthy=false", param)
		}
		if param.ServiceName == "app-a" && (!param.Enable || !param.Healthy) {
			t.Fatalf("online register parameters = %+v, want Enable=true and Healthy=true", param)
		}
	}
	// The second app-a batch cannot overtake app-a's first batch. App-b may
	// overlap because it is an independent scope.
	secondBatchPosition := -1
	lastFirstBatchPosition := -1
	for position, ip := range order {
		if ip == "10.0.0.101" {
			secondBatchPosition = position
		}
		var n int
		if _, err := fmt.Sscanf(ip, "10.0.0.%d", &n); err == nil && n <= 100 && position > lastFirstBatchPosition {
			lastFirstBatchPosition = position
		}
	}
	if secondBatchPosition <= lastFirstBatchPosition {
		t.Fatalf("application scope batch order was not serialized: second batch at %d, first batch ended at %d; order=%v", secondBatchPosition, lastFirstBatchPosition, order)
	}
}

// The concurrency contract is Sink-wide, not per method invocation. A watch
// burst, a full snapshot, and a retry may enter the same Sink concurrently;
// independent local semaphores would multiply the configured limit.
func TestSinkWidePushLimiterCapsConcurrentIncrementalAndFullCalls(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{}, delay: 5 * time.Millisecond}
	sink := newBatchTestSink(recorder)
	SetPushConcurrency(3)
	t.Cleanup(func() { SetPushConcurrency(DefaultPushConcurrency) })

	makeItems := func(prefix, app string, count int) []*instance.Instance {
		items := make([]*instance.Instance, count)
		for i := range items {
			items[i] = &instance.Instance{
				InstanceId: fmt.Sprintf("%s-%02d", prefix, i),
				AppCode:    app,
				Provider:   "k8s",
				Ip:         fmt.Sprintf("10.%d.%d.%d", len(prefix), len(app), i+1),
				Status:     instance.InstanceStatusOnline,
				Enabled:    true,
			}
		}
		return items
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 2; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- sink.Push(time.Now().UnixNano(), makeItems(fmt.Sprintf("inc-%d", i), fmt.Sprintf("inc-app-%d", i), 20))
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- sink.pushPersistentBatches(makeItems(fmt.Sprintf("full-%d", i), fmt.Sprintf("full-app-%d", i), 20))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent push error: %v", err)
		}
	}

	recorder.mu.Lock()
	max, calls := recorder.max, len(recorder.order)
	recorder.mu.Unlock()
	if max > 3 {
		t.Fatalf("maximum concurrent SDK calls = %d, want <= 3 across all Sink entry points", max)
	}
	if calls != 80 {
		t.Fatalf("SDK calls = %d, want all 80 items attempted", calls)
	}
}

func TestPersistentVendorBatchRegistersAndObservesUnhealthyWireShape(t *testing.T) {
	vendor := &persistentVisibilityVendor{}
	sink := &Sink{
		client: &Client{sdk: &sdkNamingFacade{vendor: vendor, group: DefaultGroup}, config: ClientConfig{}},
		logger: nopBatchLogger{}, groupName: DefaultGroup,
	}
	item := &instance.Instance{
		InstanceId: "pod-uh", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.0.9", Ports: []*instance.PortInfo{{Port: 8080}},
		Status: instance.InstanceStatusUnhealthy, Enabled: false, Reversion: 42,
	}
	if err := sink.pushPersistentBatches([]*instance.Instance{item}); err != nil {
		t.Fatalf("persistent vendor unhealthy batch: %v", err)
	}
	hosts, err := vendor.SelectAll("pay-user", "k8s", DefaultGroup)
	if err != nil {
		t.Fatalf("read persistent vendor state after acknowledged write: %v", err)
	}
	if len(hosts) != 1 {
		t.Fatalf("vendor entries=%d, want 1", len(hosts))
	}
	if got := hosts[0]; !got.Enabled || got.Healthy || got.Ephemeral {
		t.Fatalf("unhealthy vendor wire shape=%+v, want enabled=true healthy=false persistent", got)
	}
}

func TestPersistentVendorBatchDoesNotSynchronouslyPollCatalog(t *testing.T) {
	vendor := &persistentVisibilityVendor{selectErr: errors.New("catalog snapshot is deliberately unavailable")}
	sink := &Sink{
		client: &Client{sdk: &sdkNamingFacade{vendor: vendor, group: DefaultGroup}, config: ClientConfig{}},
		logger: nopBatchLogger{}, groupName: DefaultGroup,
	}
	item := &instance.Instance{
		InstanceId: "pod-ack", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.0.10", Ports: []*instance.PortInfo{{Port: 8080}},
		Status: instance.InstanceStatusOnline, Enabled: true, Reversion: 43,
	}
	if err := sink.pushPersistentBatches([]*instance.Instance{item}); err != nil {
		t.Fatalf("acknowledged persistent write must not depend on synchronous catalog visibility: %v", err)
	}
	vendor.mu.Lock()
	selectCalls, registered := vendor.selectCalls, len(vendor.items)
	vendor.mu.Unlock()
	if selectCalls != 0 {
		t.Fatalf("SelectAll calls=%d, want 0 in the write path", selectCalls)
	}
	if registered != 1 {
		t.Fatalf("registered entries=%d, want 1 acknowledged write", registered)
	}
}

func TestPersistentVendorBatchReturnsFirstRPCErrorAndSkipsPrune(t *testing.T) {
	first := errors.New("first persistent RPC failed")
	vendor := &persistentVisibilityVendor{registerErrByIP: map[string]error{
		"10.0.1.1": first,
		"10.0.1.2": errors.New("second persistent RPC failed"),
	}}
	sink := &Sink{
		client:     &Client{sdk: &sdkNamingFacade{vendor: vendor, group: DefaultGroup}, config: ClientConfig{}},
		logger:     nopBatchLogger{},
		groupName:  DefaultGroup,
		remembered: map[clusterKeyOf]bool{},
	}
	items := []*instance.Instance{
		{InstanceId: "one", AppCode: "pay-user", Provider: "k8s", Ip: "10.0.1.1", Status: instance.InstanceStatusOnline, Enabled: true},
		{InstanceId: "two", AppCode: "pay-user", Provider: "k8s", Ip: "10.0.1.2", Status: instance.InstanceStatusOnline, Enabled: true},
		{InstanceId: "three", AppCode: "pay-user", Provider: "k8s", Ip: "10.0.1.3", Status: instance.InstanceStatusOnline, Enabled: true},
	}
	if err := sink.PushAll(1, items); !errors.Is(err, first) {
		t.Fatalf("PushAll() error=%v, want first input-order RPC error %v", err, first)
	}
	vendor.mu.Lock()
	attempts := append([]string(nil), vendor.registerAttempts...)
	selectCalls := vendor.selectCalls
	vendor.mu.Unlock()
	if len(attempts) != len(items) {
		t.Fatalf("persistent RPC attempts=%v, want all %d items attempted", attempts, len(items))
	}
	if selectCalls != 0 {
		t.Fatalf("prune SelectAll calls after failed write=%d, want 0", selectCalls)
	}
}

func TestPushAllNeverDeletesDesiredInstanceMissingFromLaggingCatalog(t *testing.T) {
	healthy := true
	ghost := &instance.Instance{
		InstanceId: "ghost", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.2.99", Ports: []*instance.PortInfo{{Port: 8080}},
		Status: instance.InstanceStatusOnline, Enabled: true, Reversion: 40,
	}
	ghostParam := InstanceParams{
		ServiceName: ghost.AppCode, IP: ghost.Ip, Port: firstPort(ghost),
		ClusterName: clusterOf(ghost), GroupName: DefaultGroup,
		Enabled: true, Healthy: &healthy, Ephemeral: false, Metadata: metadataOf(ghost),
	}
	vendor := &persistentVisibilityVendor{
		items:     map[string]InstanceParams{instanceID(ghostParam): ghostParam},
		hiddenIPs: map[string]bool{"10.0.2.10": true},
	}
	sink := &Sink{
		client: &Client{sdk: &sdkNamingFacade{
			client: &batchRecorder{errByIP: map[string]error{}}, vendor: vendor, group: DefaultGroup,
		}, config: ClientConfig{}},
		logger:     nopBatchLogger{},
		groupName:  DefaultGroup,
		remembered: map[clusterKeyOf]bool{},
	}
	desired := &instance.Instance{
		InstanceId: "desired", AppCode: "pay-user", Provider: "k8s",
		Ip: "10.0.2.10", Ports: []*instance.PortInfo{{Port: 8080}},
		Status: instance.InstanceStatusOnline, Enabled: true, Reversion: 41,
	}
	if err := sink.PushAll(2, []*instance.Instance{desired}); err != nil {
		t.Fatalf("PushAll() with lagging catalog: %v", err)
	}
	vendor.mu.Lock()
	deregistered := append([]InstanceParams(nil), vendor.deregistered...)
	remaining := make([]InstanceParams, 0, len(vendor.items))
	for _, item := range vendor.items {
		remaining = append(remaining, item)
	}
	vendor.mu.Unlock()
	if len(deregistered) != 1 || deregistered[0].IP != ghost.Ip {
		t.Fatalf("deregistered=%+v, want only visible owned ghost %s", deregistered, ghost.Ip)
	}
	if len(remaining) != 1 || remaining[0].IP != desired.Ip {
		t.Fatalf("remaining=%+v, want acknowledged desired instance %s preserved despite lagging catalog", remaining, desired.Ip)
	}
}

// A Nacos gRPC connection keeps one publication per (service, connection);
// another single RegisterInstance replaces that publication. The application
// batch executor must therefore publish all 201 entries without losing the
// earlier 100+100 chunks.
func TestPushPersistentBatchesPreservesAllEntriesAcross201Items(t *testing.T) {
	recorder := &replacementBatchRecorder{}
	sink := &Sink{client: &Client{sdk: &sdkNamingFacade{client: recorder, group: DefaultGroup}, clusterAdmin: successfulBatchAdmin{}, config: ClientConfig{}}, logger: nopBatchLogger{}, groupName: DefaultGroup}
	sink.client.sdkFactory = func(ClientConfig) (*sdkNamingFacade, error) { return sink.client.sdk, nil }
	items := make([]*instance.Instance, 201)
	for i := range items {
		items[i] = &instance.Instance{InstanceId: fmt.Sprintf("batch-%03d", i), AppCode: "svc", Provider: "k8s", Ip: fmt.Sprintf("10.0.0.%d", i+1), Ports: []*instance.PortInfo{{Port: int32(20000 + i)}}, Enabled: true, Status: instance.InstanceStatusOnline}
	}
	if err := sink.pushPersistentBatches(items); err != nil {
		t.Fatalf("pushPersistentBatches() error = %v", err)
	}
	if got := recorder.count("svc", "k8s"); got != len(items) {
		t.Fatalf("final published entries = %d, want exactly %d after 100+100+1", got, len(items))
	}
}

func TestSDKShimExposesPersistentVendorCapability(t *testing.T) {
	facade := &sdkNamingFacade{client: &grpcSDKClient{vendor: nil}}
	if facade.hasPersistentVendor() {
		t.Fatal("nil shim vendor reported as available")
	}
	facade.client = &grpcSDKClient{vendor: &nacos3GRPCVendor{}}
	if !facade.hasPersistentVendor() {
		t.Fatal("shim vendor capability not exposed")
	}
}

type replacementBatchRecorder struct {
	mu      sync.Mutex
	byScope map[string]map[string]bool
}

func (r *replacementBatchRecorder) RegisterPersistentBatch(items []InstanceParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byScope == nil {
		r.byScope = map[string]map[string]bool{}
	}
	if len(items) == 0 {
		return nil
	}
	key := items[0].ServiceName + "/" + items[0].ClusterName
	if r.byScope[key] == nil {
		r.byScope[key] = map[string]bool{}
	}
	for _, item := range items {
		r.byScope[key][item.Metadata["instanceId"]] = true
	}
	return nil
}

func (r *replacementBatchRecorder) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byScope == nil {
		r.byScope = map[string]map[string]bool{}
	}
	key := p.ServiceName + "/" + p.ClusterName
	if r.byScope[key] == nil {
		r.byScope[key] = map[string]bool{}
	}
	// Single-register semantics on one gRPC connection replace the service
	// publication; this models the Nacos 3 behavior exposed by the SDK proxy.
	r.byScope[key][fmt.Sprintf("%s#%d#%s#%s@@%s", p.Ip, p.Port, p.ClusterName, p.GroupName, p.ServiceName)] = true
	return true, nil
}
func (r *replacementBatchRecorder) BatchRegisterInstance(vo.BatchRegisterInstanceParam) (bool, error) {
	return false, ErrUnsupportedOperation
}
func (r *replacementBatchRecorder) DeregisterInstance(vo.DeregisterInstanceParam) (bool, error) {
	return true, nil
}
func (r *replacementBatchRecorder) UpdateInstance(vo.UpdateInstanceParam) (bool, error) {
	return true, nil
}
func (r *replacementBatchRecorder) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]model.Instance, 0)
	for id := range r.byScope[p.ServiceName+"/k8s"] {
		result = append(result, model.Instance{InstanceId: id})
	}
	return result, nil
}
func (r *replacementBatchRecorder) GetAllServicesInfo(vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	return model.ServiceList{}, nil
}
func (r *replacementBatchRecorder) Subscribe(*vo.SubscribeParam) error   { return nil }
func (r *replacementBatchRecorder) Unsubscribe(*vo.SubscribeParam) error { return nil }
func (r *replacementBatchRecorder) ServerHealthy() bool                  { return true }
func (r *replacementBatchRecorder) CloseClient()                         {}
func (r *replacementBatchRecorder) count(service, cluster string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byScope[service+"/"+cluster])
}

func TestPushPersistentBatchesAttemptsEveryItemAndReturnsFirstInputError(t *testing.T) {
	first := errors.New("first input error")
	recorder := &batchRecorder{errByIP: map[string]error{"10.0.0.1": first, "10.0.0.2": errors.New("second input error")}}
	sink := newBatchTestSink(recorder)
	items := []*instance.Instance{
		{InstanceId: "one", AppCode: "app", Provider: "k8s", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true},
		{InstanceId: "two", AppCode: "app", Provider: "k8s", Ip: "10.0.0.2", Status: instance.InstanceStatusOnline, Enabled: true},
		{InstanceId: "three", AppCode: "app", Provider: "k8s", Ip: "10.0.0.3", Status: instance.InstanceStatusOnline, Enabled: true},
	}
	err := sink.pushPersistentBatches(items)
	if !errors.Is(err, first) {
		t.Fatalf("error = %v, want first input error %v", err, first)
	}
	recorder.mu.Lock()
	called := len(recorder.order)
	recorder.mu.Unlock()
	if called != len(items) {
		t.Fatalf("SDK calls = %d, want every item attempted (%d)", called, len(items))
	}
}

func TestPushPersistentBatchesReportsLogicalBatchMetrics(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{}}
	sink := newBatchTestSink(recorder)
	items := make([]*instance.Instance, 201)
	for i := range items {
		items[i] = &instance.Instance{InstanceId: fmt.Sprintf("metric-%03d", i), AppCode: "metrics-app", Provider: "k8s", Ip: fmt.Sprintf("10.20.0.%d", i+1), Status: instance.InstanceStatusOnline, Enabled: true}
	}
	SetPushConcurrency(8)
	t.Cleanup(func() { SetPushConcurrency(DefaultPushConcurrency) })
	if err := sink.pushPersistentBatches(items); err != nil {
		t.Fatalf("pushPersistentBatches() error = %v", err)
	}
	got := sink.BatchMetrics()
	if got.LogicalBatches != 3 || got.Items != uint64(len(items)) {
		t.Fatalf("batch metrics = %+v, want logical_batches=3 items=201", got)
	}
	if got.ConcurrencyCap != 8 || got.RetryCount != 0 {
		t.Fatalf("batch metrics = %+v, want concurrency_cap=8 retry_count=0", got)
	}
}

func TestPushPersistentBatchesCountsFailedExecutionForWorkerRetry(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{"10.21.0.1": errors.New("transient registration failure")}}
	sink := newBatchTestSink(recorder)
	item := &instance.Instance{InstanceId: "retry", AppCode: "retry-app", Provider: "k8s", Ip: "10.21.0.1", Status: instance.InstanceStatusOnline, Enabled: true}
	if err := sink.pushPersistentBatches([]*instance.Instance{item}); err == nil {
		t.Fatal("pushPersistentBatches() error = nil, want registration failure")
	}
	got := sink.BatchMetrics()
	if got.LogicalBatches != 1 || got.Items != 1 || got.RetryCount != 1 {
		t.Fatalf("batch metrics = %+v, want one failed logical execution", got)
	}
}

func TestPushAllSkipsPruneWhenPersistentBatchFails(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{"10.0.0.1": errors.New("registration failed")}}
	sink := newBatchTestSink(recorder)
	item := &instance.Instance{InstanceId: "one", AppCode: "app", Provider: "k8s", Ip: "10.0.0.1", Status: instance.InstanceStatusOnline, Enabled: true}
	if err := sink.PushAll(1, []*instance.Instance{item}); err == nil {
		t.Fatal("PushAll() error = nil, want registration failure")
	}
	recorder.mu.Lock()
	listCalls := recorder.listCall
	recorder.mu.Unlock()
	if listCalls != 0 {
		t.Fatalf("prune list calls = %d, want zero after batch failure", listCalls)
	}
}

func TestPushPersistentBatchesMapsDeregisterParameters(t *testing.T) {
	recorder := &batchRecorder{errByIP: map[string]error{}}
	sink := newBatchTestSink(recorder)
	item := &instance.Instance{InstanceId: "offline", AppCode: "app", Provider: "ecs", Ip: "10.0.2.1", Status: instance.InstanceStatusOffline, Enabled: false}
	if err := sink.pushPersistentBatches([]*instance.Instance{item}); err != nil {
		t.Fatalf("pushPersistentBatches() error = %v", err)
	}
	recorder.mu.Lock()
	deferred := append([]vo.DeregisterInstanceParam(nil), recorder.deregistered...)
	recorder.mu.Unlock()
	if len(deferred) != 1 {
		t.Fatalf("deregister calls = %d, want 1", len(deferred))
	}
	param := deferred[0]
	if param.ServiceName != "app" || param.Cluster != "ecs" || param.GroupName != DefaultGroup || param.Ephemeral {
		t.Fatalf("deregister parameters = %+v, want app/ecs/DEFAULT_GROUP/persistent", param)
	}
}

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
func (r *batchRecorder) SelectAllInstances(vo.SelectAllInstancesParam) ([]model.Instance, error) {
	r.mu.Lock()
	r.listCall++
	r.mu.Unlock()
	return nil, nil
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
		if param.ServiceName == "app-b" && (param.Enable || param.Healthy) {
			t.Fatalf("unhealthy register parameters = %+v, want Enable=false and Healthy=false", param)
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
	r.byScope[key] = map[string]bool{p.Metadata["instanceId"]: true}
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

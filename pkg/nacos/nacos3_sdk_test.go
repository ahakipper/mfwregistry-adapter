package nacos

import (
	"errors"
	"reflect"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/common/remote/codec"
	"github.com/nacos-group/nacos-sdk-go/v3/model"
	namingproto "github.com/nacos-group/nacos-sdk-proto/go/naming"
)

type recordingNacos3Vendor struct {
	registered    []InstanceParams
	deregistered  []InstanceParams
	selectArgs    []string
	serviceArgs   []interface{}
	subscribeArgs []interface{}
	unsubArgs     []interface{}
	closed        int
	err           error
}

func (v *recordingNacos3Vendor) RegisterPersistent(p InstanceParams) error {
	v.registered = append(v.registered, p)
	return v.err
}
func (v *recordingNacos3Vendor) DeregisterPersistent(p InstanceParams) error {
	v.deregistered = append(v.deregistered, p)
	return v.err
}
func (v *recordingNacos3Vendor) SelectAll(service, cluster, group string) ([]Host, error) {
	v.selectArgs = []string{service, cluster, group}
	return []Host{{ServiceName: service, ClusterName: cluster}}, v.err
}
func (v *recordingNacos3Vendor) ListServices(page, size int, namespace, group string) ([]string, int, error) {
	v.serviceArgs = []interface{}{page, size, namespace, group}
	return []string{"svc-a"}, 1, v.err
}
func (v *recordingNacos3Vendor) Subscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	v.subscribeArgs = []interface{}{service, group, append([]string(nil), clusters...), callback}
	return v.err
}
func (v *recordingNacos3Vendor) Unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error {
	v.unsubArgs = []interface{}{service, group, append([]string(nil), clusters...), callback}
	return v.err
}
func (v *recordingNacos3Vendor) Close() error { v.closed++; return v.err }

// Keep the test name as a readable alias of the production seam contract.
type nacos3Vendor = nacos3SDKVendor

// The facade contract emits RegisterInstanceRequest/DeregisterInstanceRequest
// through the persistent vendor seam with Ephemeral=false.
func TestNacos3FacadeOwnsVendorOperations(t *testing.T) {
	vendor := &recordingNacos3Vendor{}
	facade := &sdkNamingFacade{vendor: vendor}
	params := InstanceParams{ServiceName: "payments", IP: "10.0.0.8", Port: 8080,
		NamespaceID: "tenant-a", GroupName: "blue", ClusterName: "arm64",
		Ephemeral: true, Metadata: map[string]string{"owner": "spotter"}}
	if err := facade.RegisterPersistent(params); err != nil {
		t.Fatal(err)
	}
	if len(vendor.registered) != 1 || vendor.registered[0].Ephemeral {
		t.Fatalf("persistent register = %+v, want one vendor persistent call with Ephemeral=false", vendor.registered)
	}
	if vendor.registered[0].NamespaceID != "tenant-a" || vendor.registered[0].GroupName != "blue" || vendor.registered[0].ClusterName != "arm64" || vendor.registered[0].Metadata["owner"] != "spotter" {
		t.Fatalf("identity/metadata mapping = %+v", vendor.registered[0])
	}
	if err := facade.DeregisterPersistent(params); err != nil {
		t.Fatal(err)
	}
	if len(vendor.deregistered) != 1 || vendor.deregistered[0].Ephemeral {
		t.Fatalf("persistent deregister = %+v, want one vendor persistent call with Ephemeral=false", vendor.deregistered)
	}
}

func TestNacos3FacadeRoutesReadsWatchesAndClose(t *testing.T) {
	vendor := &recordingNacos3Vendor{}
	facade := &sdkNamingFacade{vendor: vendor}
	callback := func([]Host, error) {}
	if hosts, err := facade.SelectAll("payments", "arm64", "blue"); err != nil || len(hosts) != 1 {
		t.Fatalf("SelectAll() = %#v, %v", hosts, err)
	}
	if !reflect.DeepEqual(vendor.selectArgs, []string{"payments", "arm64", "blue"}) {
		t.Fatalf("SelectAll args = %#v", vendor.selectArgs)
	}
	if names, count, err := facade.ListServices(2, 50, "tenant-a", "blue"); err != nil || count != 1 || !reflect.DeepEqual(names, []string{"svc-a"}) {
		t.Fatalf("ListServices() = %#v, %d, %v", names, count, err)
	}
	if !reflect.DeepEqual(vendor.serviceArgs, []interface{}{2, 50, "tenant-a", "blue"}) {
		t.Fatalf("ListServices args = %#v", vendor.serviceArgs)
	}
	clusters := []string{"arm64"}
	if err := facade.Subscribe("payments", "blue", clusters, callback); err != nil {
		t.Fatal(err)
	}
	if got := vendor.subscribeArgs[0:3]; !reflect.DeepEqual(got, []interface{}{"payments", "blue", clusters}) {
		t.Fatalf("Subscribe args = %#v", vendor.subscribeArgs)
	}
	if err := facade.Unsubscribe("payments", "blue", clusters, callback); err != nil {
		t.Fatal(err)
	}
	if got := vendor.unsubArgs[0:3]; !reflect.DeepEqual(got, []interface{}{"payments", "blue", clusters}) {
		t.Fatalf("Unsubscribe args = %#v", vendor.unsubArgs)
	}
	if err := facade.Close(); err != nil || vendor.closed != 1 {
		t.Fatalf("Close() = %v, calls=%d", err, vendor.closed)
	}
}

func TestNacos3FacadePropagatesVendorErrors(t *testing.T) {
	want := errors.New("nacos3 vendor unavailable")
	vendor := &recordingNacos3Vendor{err: want}
	facade := &sdkNamingFacade{vendor: vendor}
	if err := facade.RegisterPersistent(InstanceParams{ServiceName: "svc"}); !errors.Is(err, want) {
		t.Fatalf("RegisterPersistent() error = %v, want %v", err, want)
	}
	if err := facade.DeregisterPersistent(InstanceParams{ServiceName: "svc"}); !errors.Is(err, want) {
		t.Fatalf("DeregisterPersistent() error = %v, want %v", err, want)
	}
	if _, err := facade.SelectAll("svc", "", ""); !errors.Is(err, want) {
		t.Fatalf("SelectAll() error = %v, want %v", err, want)
	}
	if _, _, err := facade.ListServices(1, 1, "", ""); !errors.Is(err, want) {
		t.Fatalf("ListServices() error = %v, want %v", err, want)
	}
	if err := facade.Subscribe("svc", "", nil, nil); !errors.Is(err, want) {
		t.Fatalf("Subscribe() error = %v, want %v", err, want)
	}
	if err := facade.Unsubscribe("svc", "", nil, nil); !errors.Is(err, want) {
		t.Fatalf("Unsubscribe() error = %v, want %v", err, want)
	}
	if err := facade.Close(); !errors.Is(err, want) {
		t.Fatalf("Close() error = %v, want %v", err, want)
	}
}

// TestNacos3PersistentProtoContract pins the actual official SDK protobuf
// payload used by NamingGrpcProxy. This guards the adapter's persistent
// boundary against accidentally regressing to the legacy HTTP delegate or an
// ephemeral-only batch shape.
func TestNacos3PersistentProtoContract(t *testing.T) {
	instance := model.Instance{Ip: "10.0.0.8", Port: 8080, Ephemeral: false}
	register := &persistentInstanceRequest{Namespace: "tenant-a", ServiceName: "payments", GroupName: "blue", Type: "registerInstance", Instance: instance}
	if register.GetRequestType() != "PersistentInstanceRequest" {
		t.Fatalf("request type = %q", register.GetRequestType())
	}
	registerProto, ok := register.ProtoMessage().(*namingproto.PersistentInstanceRequest)
	if !ok {
		t.Fatalf("register proto type = %T, want *naming.PersistentInstanceRequest", register.ProtoMessage())
	}
	if registerProto.GetType() != "registerInstance" || registerProto.GetInstance().GetEphemeral() {
		t.Fatalf("register proto = %+v, want registerInstance with Ephemeral=false", registerProto)
	}
	deregister := &persistentInstanceRequest{Namespace: "tenant-a", ServiceName: "payments", GroupName: "blue", Type: "deregisterInstance", Instance: instance}
	deregisterProto, ok := deregister.ProtoMessage().(*namingproto.PersistentInstanceRequest)
	if !ok {
		t.Fatalf("deregister proto type = %T, want *naming.PersistentInstanceRequest", deregister.ProtoMessage())
	}
	if deregisterProto.GetType() != "deregisterInstance" || deregisterProto.GetInstance().GetEphemeral() {
		t.Fatalf("deregister proto = %+v, want deregisterInstance with Ephemeral=false", deregisterProto)
	}
	payload, err := codec.NewPayloadCodec().Encode("PersistentInstanceRequest", register.ProtoMessage(), register.GetHeaders(), "127.0.0.1")
	if err != nil || payload == nil {
		t.Fatalf("codec encode: %v", err)
	}
	if payload.GetMetadata().GetType() != "PersistentInstanceRequest" {
		t.Fatalf("metadata type=%q", payload.GetMetadata().GetType())
	}
}

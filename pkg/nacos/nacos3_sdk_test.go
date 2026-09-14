package nacos

import "testing"

type recordingNacos3Vendor struct {
	registered   []InstanceParams
	deregistered []InstanceParams
}

func (v *recordingNacos3Vendor) RegisterPersistent(p InstanceParams) error {
	v.registered = append(v.registered, p)
	return nil
}
func (v *recordingNacos3Vendor) DeregisterPersistent(p InstanceParams) error {
	v.deregistered = append(v.deregistered, p)
	return nil
}
func (*recordingNacos3Vendor) SelectAll(string, string, string) ([]Host, error) { return nil, nil }
func (*recordingNacos3Vendor) ListServices(int, int, string, string) ([]string, int, error) {
	return nil, 0, nil
}
func (*recordingNacos3Vendor) Subscribe(string, string, []string, func([]Host, error)) error {
	return nil
}
func (*recordingNacos3Vendor) Unsubscribe(string, string, []string, func([]Host, error)) error {
	return nil
}
func (*recordingNacos3Vendor) Close() error { return nil }

// nacos3Vendor is the Spotter-owned seam for the Nacos 3 naming SDK.  The
// implementation must route persistent lifecycle and all naming reads through
// this seam; tests intentionally require operation-specific methods so a
// future adapter cannot silently fall back to naming_http or /v1/ns.
type nacos3Vendor interface {
	RegisterPersistent(InstanceParams) error
	DeregisterPersistent(InstanceParams) error
	SelectAll(service, cluster, group string) ([]Host, error)
	ListServices(page, size int, namespace, group string) ([]string, int, error)
	Subscribe(service, group string, clusters []string, callback func([]Host, error)) error
	Unsubscribe(service, group string, clusters []string, callback func([]Host, error)) error
	Close() error
}

// RED: sdkNamingFacade does not yet expose the Nacos 3 vendor seam.  This
// compile-time contract is deliberate; Task 1.2 will provide the adapter that
// satisfies it and emits RegisterInstanceRequest/DeregisterInstanceRequest
// over gRPC with Ephemeral=false.
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
		t.Fatalf("persistent register = %+v, want one gRPC RegisterInstanceRequest with Ephemeral=false", vendor.registered)
	}
	if vendor.registered[0].NamespaceID != "tenant-a" || vendor.registered[0].GroupName != "blue" || vendor.registered[0].ClusterName != "arm64" || vendor.registered[0].Metadata["owner"] != "spotter" {
		t.Fatalf("identity/metadata mapping = %+v", vendor.registered[0])
	}
	if err := facade.DeregisterPersistent(params); err != nil {
		t.Fatal(err)
	}
	if len(vendor.deregistered) != 1 || vendor.deregistered[0].Ephemeral {
		t.Fatalf("persistent deregister = %+v, want one gRPC DeregisterInstanceRequest with Ephemeral=false", vendor.deregistered)
	}
}

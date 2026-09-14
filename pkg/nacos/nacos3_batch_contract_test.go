package nacos

import (
	"errors"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v3/vo"
)

// Nacos 3's protocol BatchInstanceRequest is a complete publication and the
// pinned official Go SDK exposes it only for ephemeral instances. Persistent
// Spotter writes must therefore remain item-wise PersistentInstanceRequest
// calls inside the adapter's logical application batches.
func TestNacos3PersistentPathRejectsProtocolBatch(t *testing.T) {
	client := &grpcSDKClient{}
	_, err := client.BatchRegisterInstance(vo.BatchRegisterInstanceParam{
		ServiceName: "orders",
		GroupName:   DefaultGroup,
		Instances:   []vo.RegisterInstanceParam{{Ip: "10.0.0.1", Port: 8080, Ephemeral: false}},
	})
	if !errors.Is(err, ErrUnsupportedOperation) {
		t.Fatalf("BatchRegisterInstance() error = %v, want ErrUnsupportedOperation", err)
	}
}

func TestNacos3FacadeLogicalBatchUsesPersistentItems(t *testing.T) {
	vendor := &recordingNacos3Vendor{}
	facade := &sdkNamingFacade{vendor: vendor}
	items := []InstanceParams{
		{ServiceName: "orders", IP: "10.0.0.1", Port: 8080, GroupName: DefaultGroup, ClusterName: "k8s", Ephemeral: true},
		{ServiceName: "orders", IP: "10.0.0.2", Port: 8081, GroupName: DefaultGroup, ClusterName: "k8s", Ephemeral: true},
	}
	if err := facade.RegisterPersistentBatch(items); err != nil {
		t.Fatalf("RegisterPersistentBatch() error = %v", err)
	}
	if got := len(vendor.registered); got != len(items) {
		t.Fatalf("persistent item calls = %d, want %d", got, len(items))
	}
	for i, item := range vendor.registered {
		if item.Ephemeral {
			t.Fatalf("item %d Ephemeral=true, want false", i)
		}
	}
}

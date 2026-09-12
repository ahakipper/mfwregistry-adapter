package v2

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

// TestMirrorTypesAreNotGeneratedProtoMessages is a fail-closed compatibility
// sentinel.  The v2 package currently contains ordinary Go mirror structs;
// they cannot be marshaled by gRPC's default protobuf codec.  Keep this test
// explicit so a future generated-proto migration must update the B4 evidence
// and adapter path instead of silently changing the wire contract.
func TestMirrorTypesAreNotGeneratedProtoMessages(t *testing.T) {
	if _, ok := interface{}(&Instance{}).(proto.Message); ok {
		t.Fatal("v2.Instance unexpectedly implements proto.Message; update the Atlas wire compatibility gate and adapter")
	}
	if _, ok := interface{}(&SynInstancesRequest{}).(proto.Message); ok {
		t.Fatal("v2.SynInstancesRequest unexpectedly implements proto.Message; update the Atlas wire compatibility gate and adapter")
	}
}

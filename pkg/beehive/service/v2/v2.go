// Package v2 is a local, self-contained replacement of the private
// "beehive-proto api/service/v2" module of the internal beehive platform.
//
// It mirrors the shape of the generated protobuf types and the gRPC client
// surface that this repository (spotter) actually consumes. The structs are
// plain Go structs (no generated code), so no protobuf toolchain is needed to
// build the project.
//
// The data types below are type aliases over internal/domain/instance (the
// DDD domain model), so v2.Instance and instance.Instance are the same type
// at compile time: legacy import sites keep compiling while the sink path
// migrates to the domain vocabulary (docs/nacos-sink-plan.md section 4.1,
// decision D1). Aliases are compile-time identity only — they change nothing
// about marshaling.
//
// Wire compatibility is NOT provided: these are plain Go structs, not
// generated proto messages. grpc.ClientConn.Invoke uses the default proto
// codec, so calls will fail at runtime ("proto: not a proto message") unless
// the server is configured with a codec that accepts these structs (e.g. a
// JSON codec registered via grpc.WithDefaultCallOption(grpc.ForceCodec(...))).
// This mirror exists to keep the repository self-contained and compiling
// without the private beehive-proto module; real proto marshaling is a
// follow-up if this binary ever needs to talk to the live discovery center.
package v2

import (
	"context"

	"google.golang.org/grpc"

	"spotter/internal/domain/instance"
)

// The mirror types ARE the domain types (type aliases over
// internal/domain/instance, which has every field and accessor this mirror
// used to define). Legacy import sites keep compiling while the sink path
// migrates.
type (
	Instance               = instance.Instance
	PortInfo               = instance.PortInfo
	InstanceList           = instance.InstanceList
	CommonResponse         = instance.CommonResponse
	SynInstancesRequest    = instance.SynInstancesRequest
	SynAllInstancesRequest = instance.SynAllInstancesRequest
	GetAllInstancesRequest = instance.GetAllInstancesRequest
)

// InstanceServiceClient mirrors the beehive-proto v2.InstanceServiceClient
// surface used by this repository.
type InstanceServiceClient interface {
	SynInstance(ctx context.Context, in *SynInstancesRequest, opts ...grpc.CallOption) (*CommonResponse, error)
	SynAllInstance(ctx context.Context, in *SynAllInstancesRequest, opts ...grpc.CallOption) (*CommonResponse, error)
	GetAllInstance(ctx context.Context, in *GetAllInstancesRequest, opts ...grpc.CallOption) (*InstanceList, error)
}

// NewInstanceServiceClient builds an InstanceServiceClient on top of an
// established gRPC client connection.
func NewInstanceServiceClient(cc *grpc.ClientConn) InstanceServiceClient {
	return &instanceServiceClient{cc}
}

// instanceServiceClient is the private implementation of InstanceServiceClient.
// It invokes the very same RPC paths that the original generated client used,
// so a server built from the original protobuf definitions stays compatible.
type instanceServiceClient struct {
	cc *grpc.ClientConn
}

func (c *instanceServiceClient) SynInstance(ctx context.Context, in *SynInstancesRequest, opts ...grpc.CallOption) (*CommonResponse, error) {
	out := new(CommonResponse)
	err := c.cc.Invoke(ctx, "/service.v2.InstanceService/SynInstance", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceServiceClient) SynAllInstance(ctx context.Context, in *SynAllInstancesRequest, opts ...grpc.CallOption) (*CommonResponse, error) {
	out := new(CommonResponse)
	err := c.cc.Invoke(ctx, "/service.v2.InstanceService/SynAllInstance", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *instanceServiceClient) GetAllInstance(ctx context.Context, in *GetAllInstancesRequest, opts ...grpc.CallOption) (*InstanceList, error) {
	out := new(InstanceList)
	err := c.cc.Invoke(ctx, "/service.v2.InstanceService/GetAllInstance", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

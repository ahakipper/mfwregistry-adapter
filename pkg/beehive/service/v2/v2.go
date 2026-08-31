// Package v2 is a local, self-contained replacement of the private
// "beehive-proto api/service/v2" module of the internal beehive platform.
//
// It mirrors the shape of the generated protobuf types and the gRPC client
// surface that this repository (spotter) actually consumes. The structs are
// plain Go structs (no generated code), so no protobuf toolchain is needed to
// build the project.
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
)

// Instance mirrors the beehive-proto service/v2 Instance message.
// Field names are the ones used by this repository's code.
type Instance struct {
	InstanceId  string // protobuf: string instance_id = 1
	Level       string
	Ports       []*PortInfo
	Ip          string
	EnvCode     string
	EnvType     string
	EnvGroup    string
	Cluster     string
	Version     string
	Enabled     bool
	State       string
	HealthState string
	AppCode     string
	Provider    string
	Label       map[string]string
	Hostname    string
	Cpu         float32
	Memory      int32
	Disk        int32
	Os          string
	Image       map[string]string
	Idc         string
	Reversion   int64
	Status      int32
}

// PortInfo mirrors the beehive-proto service/v2 PortInfo message.
type PortInfo struct {
	Name        string
	Protocol    string
	Port        int32
	ServicePort int32
}

// InstanceList mirrors the beehive-proto service/v2 InstanceList message.
type InstanceList struct {
	Instance []*Instance
}

// GetInstance safely returns the contained instances (nil-safe, like generated code).
func (l *InstanceList) GetInstance() []*Instance {
	if l == nil {
		return nil
	}
	return l.Instance
}

// CommonResponse mirrors the beehive-proto service/v2 CommonResponse message.
type CommonResponse struct {
	Code int32
	Msg  string
}

// GetCode safely returns the response code (nil-safe, like generated code).
func (r *CommonResponse) GetCode() int32 {
	if r == nil {
		return 0
	}
	return r.Code
}

// GetMsg safely returns the response message (nil-safe, like generated code).
func (r *CommonResponse) GetMsg() string {
	if r == nil {
		return ""
	}
	return r.Msg
}

// SynInstancesRequest mirrors the beehive-proto service/v2 SynInstancesRequest message.
type SynInstancesRequest struct {
	Instance []*Instance
}

// SynAllInstancesRequest mirrors the beehive-proto service/v2 SynAllInstancesRequest message.
type SynAllInstancesRequest struct {
	Instance []*Instance
}

// GetAllInstancesRequest mirrors the beehive-proto service/v2 GetAllInstancesRequest message.
type GetAllInstancesRequest struct {
	Status   int32
	Provider string
}

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

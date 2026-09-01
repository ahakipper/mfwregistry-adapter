// Package discoverymock provides an in-memory discovery-center gRPC server for tests.
package discoverymock

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/test/bufconn"

	"spotter/internal/domain/instance"
)

const (
	serviceName = "service.v2.InstanceService"
	bufSize     = 1024 * 1024
)

var errServerClosed = errors.New("discoverymock: server is closed")

// Call is an immutable snapshot of a request received by Server.
type Call struct {
	Method    string
	Instances []*instance.Instance
	Status    int32
	Provider  string
}

// Server is a thread-safe, in-memory discovery-center gRPC server.
type Server struct {
	mu sync.RWMutex

	grpcServer *grpc.Server
	listener   *bufconn.Listener
	codec      *jsonCodec
	closed     bool

	responseCode int32
	responseMsg  string
	instances    []*instance.Instance
	calls        []Call
}

// Start starts an in-memory discovery-center gRPC server.
func Start() (*Server, error) {
	codec := &jsonCodec{}
	server := &Server{
		listener: bufconn.Listen(bufSize),
		codec:    codec,
	}
	server.grpcServer = grpc.NewServer(grpc.CustomCodec(legacyCodec{codec: codec}))
	server.grpcServer.RegisterService(&serviceDesc, server)
	go func() {
		_ = server.grpcServer.Serve(server.listener)
	}()
	return server, nil
}

// DialContext dials the in-memory server using its JSON codec.
func (s *Server) DialContext(ctx context.Context) (*grpc.ClientConn, error) {
	if s == nil {
		return nil, errServerClosed
	}

	s.mu.RLock()
	listener := s.listener
	codec := encoding.Codec(s.codec)
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, errServerClosed
	}

	return grpc.DialContext(
		ctx,
		"discoverymock",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			result := make(chan dialResult, 1)
			go func() {
				conn, err := listener.Dial()
				result <- dialResult{conn: conn, err: err}
			}()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case result := <-result:
				if result.err != nil {
					return nil, result.err
				}
				return result.conn, nil
			}
		}),
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(codec)),
	)
}

// SetResponseCode configures the response returned by synchronization methods.
func (s *Server) SetResponseCode(code int32, msg string) {
	s.mu.Lock()
	s.responseCode = code
	s.responseMsg = msg
	s.mu.Unlock()
}

// SetInstances replaces the instances returned by GetAllInstance.
func (s *Server) SetInstances(instances []*instance.Instance) {
	snapshot := cloneInstances(instances)
	s.mu.Lock()
	s.instances = snapshot
	s.mu.Unlock()
}

// Calls returns deep-copied snapshots of all received calls.
func (s *Server) Calls() []Call {
	s.mu.RLock()
	calls := cloneCalls(s.calls)
	s.mu.RUnlock()
	return calls
}

// Codec returns the JSON codec used by the server and client connections.
func (s *Server) Codec() encoding.Codec {
	if s == nil {
		return nil
	}
	return s.codec
}

// Close stops the server. It is safe to call more than once.
func (s *Server) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	listener := s.listener
	grpcServer := s.grpcServer
	s.mu.Unlock()

	_ = listener.Close()
	grpcServer.Stop()
}

type dialResult struct {
	conn net.Conn
	err  error
}

type jsonCodec struct{}

func (*jsonCodec) Name() string {
	return "json"
}

func (*jsonCodec) Marshal(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (*jsonCodec) Unmarshal(data []byte, value interface{}) error {
	return json.Unmarshal(data, value)
}

// grpc.CustomCodec is the server-side codec API in gRPC v1.27.1. This adapter
// supplies its legacy String method while retaining the encoding.Codec API.
type legacyCodec struct {
	codec encoding.Codec
}

func (c legacyCodec) String() string {
	return c.codec.Name()
}

func (c legacyCodec) Marshal(value interface{}) ([]byte, error) {
	return c.codec.Marshal(value)
}

func (c legacyCodec) Unmarshal(data []byte, value interface{}) error {
	return c.codec.Unmarshal(data, value)
}

type discoveryService interface{}

var serviceDesc = grpc.ServiceDesc{
	ServiceName: serviceName,
	HandlerType: (*discoveryService)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SynInstance",
			Handler:    synInstanceHandler,
		},
		{
			MethodName: "SynAllInstance",
			Handler:    synAllInstanceHandler,
		},
		{
			MethodName: "GetAllInstance",
			Handler:    getAllInstanceHandler,
		},
	},
}

func synInstanceHandler(srv interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	request := new(instance.SynInstancesRequest)
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*Server).synInstance(request), nil
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/service.v2.InstanceService/SynInstance"}
	handler := func(ctx context.Context, request interface{}) (interface{}, error) {
		return srv.(*Server).synInstance(request.(*instance.SynInstancesRequest)), nil
	}
	return interceptor(ctx, request, info, handler)
}

func synAllInstanceHandler(srv interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	request := new(instance.SynAllInstancesRequest)
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*Server).synAllInstance(request), nil
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/service.v2.InstanceService/SynAllInstance"}
	handler := func(ctx context.Context, request interface{}) (interface{}, error) {
		return srv.(*Server).synAllInstance(request.(*instance.SynAllInstancesRequest)), nil
	}
	return interceptor(ctx, request, info, handler)
}

func getAllInstanceHandler(srv interface{}, ctx context.Context, decode func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	request := new(instance.GetAllInstancesRequest)
	if err := decode(request); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*Server).getAllInstance(request), nil
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/service.v2.InstanceService/GetAllInstance"}
	handler := func(ctx context.Context, request interface{}) (interface{}, error) {
		return srv.(*Server).getAllInstance(request.(*instance.GetAllInstancesRequest)), nil
	}
	return interceptor(ctx, request, info, handler)
}

func (s *Server) synInstance(request *instance.SynInstancesRequest) *instance.CommonResponse {
	var instances []*instance.Instance
	if request != nil {
		instances = request.Instance
	}
	s.mu.Lock()
	s.calls = append(s.calls, Call{Method: "SynInstance", Instances: cloneInstances(instances)})
	response := &instance.CommonResponse{Code: s.responseCode, Msg: s.responseMsg}
	s.mu.Unlock()
	return response
}

func (s *Server) synAllInstance(request *instance.SynAllInstancesRequest) *instance.CommonResponse {
	var instances []*instance.Instance
	if request != nil {
		instances = request.Instance
	}
	s.mu.Lock()
	s.calls = append(s.calls, Call{Method: "SynAllInstance", Instances: cloneInstances(instances)})
	response := &instance.CommonResponse{Code: s.responseCode, Msg: s.responseMsg}
	s.mu.Unlock()
	return response
}

func (s *Server) getAllInstance(request *instance.GetAllInstancesRequest) *instance.InstanceList {
	var status int32
	var provider string
	if request != nil {
		status = request.Status
		provider = request.Provider
	}

	s.mu.Lock()
	s.calls = append(s.calls, Call{Method: "GetAllInstance", Status: status, Provider: provider})
	filtered := make([]*instance.Instance, 0, len(s.instances))
	for _, item := range s.instances {
		if item == nil || item.Status != status || (provider != "" && item.Provider != provider) {
			continue
		}
		filtered = append(filtered, cloneInstance(item))
	}
	s.mu.Unlock()
	return &instance.InstanceList{Instance: filtered}
}

func cloneCalls(calls []Call) []Call {
	if calls == nil {
		return nil
	}
	cloned := make([]Call, len(calls))
	for i, call := range calls {
		cloned[i] = Call{
			Method:    call.Method,
			Instances: cloneInstances(call.Instances),
			Status:    call.Status,
			Provider:  call.Provider,
		}
	}
	return cloned
}

func cloneInstances(instances []*instance.Instance) []*instance.Instance {
	if instances == nil {
		return nil
	}
	cloned := make([]*instance.Instance, len(instances))
	for i, item := range instances {
		cloned[i] = cloneInstance(item)
	}
	return cloned
}

func cloneInstance(item *instance.Instance) *instance.Instance {
	if item == nil {
		return nil
	}
	cloned := *item
	if item.Ports != nil {
		cloned.Ports = make([]*instance.PortInfo, len(item.Ports))
		for i, port := range item.Ports {
			if port != nil {
				portCopy := *port
				cloned.Ports[i] = &portCopy
			}
		}
	}
	cloned.Label = cloneStringMap(item.Label)
	cloned.Image = cloneStringMap(item.Image)
	return &cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

var _ grpc.Codec = legacyCodec{}
var _ encoding.Codec = (*jsonCodec)(nil)
var _ io.Reader = nil

package discoverycenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"spotter/internal/domain/instance"
	"spotter/internal/ports"
)

const readTimeout = 10 * time.Second

// instanceService is the gRPC service contract this client talks to: the
// beehive-proto "service.v2.InstanceService" client surface, declared over
// the domain types (identical to the mirror types via the alias bridge);
// the RPC paths in grpcServiceClient below are unchanged.
type instanceService interface {
	SynInstance(ctx context.Context, in *instance.SynInstancesRequest, opts ...grpc.CallOption) (*instance.CommonResponse, error)
	SynAllInstance(ctx context.Context, in *instance.SynAllInstancesRequest, opts ...grpc.CallOption) (*instance.CommonResponse, error)
	GetAllInstance(ctx context.Context, in *instance.GetAllInstancesRequest, opts ...grpc.CallOption) (*instance.InstanceList, error)
}

// Client calls the discovery-center instance service.
type Client struct {
	service instanceService
	logger  ports.Logger
	metrics ports.MetricsRecorder

	connMu sync.Mutex
	conn   *grpc.ClientConn
}

// NewClient creates a client from an already constructed instance service.
// Implementations of the beehive-proto v2 client interface are accepted
// unchanged: those data types are aliases of the domain types this contract
// is declared over.
func NewClient(service instanceService, logger ports.Logger, metrics ports.MetricsRecorder) (*Client, error) {
	if service == nil {
		return nil, errors.New("discoverycenter: instance service is required")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if metrics == nil {
		metrics = nopMetricsRecorder{}
	}
	return &Client{
		service: service,
		logger:  logger,
		metrics: metrics,
	}, nil
}

// Dial connects to addr and creates a client that owns the resulting connection.
// When opts is empty, Dial uses insecure transport and blocks until connected.
// Supplying any option makes the caller responsible for the complete dial setup.
func Dial(ctx context.Context, addr string, logger ports.Logger, metrics ports.MetricsRecorder, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithInsecure(), grpc.WithBlock()}
	}
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(&grpcServiceClient{conn: conn}, logger, metrics)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	client.conn = conn
	return client, nil
}

type grpcServiceClient struct {
	conn *grpc.ClientConn
}

func (c *grpcServiceClient) SynInstance(ctx context.Context, request *instance.SynInstancesRequest, opts ...grpc.CallOption) (*instance.CommonResponse, error) {
	response := new(instance.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *grpcServiceClient) SynAllInstance(ctx context.Context, request *instance.SynAllInstancesRequest, opts ...grpc.CallOption) (*instance.CommonResponse, error) {
	response := new(instance.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *grpcServiceClient) GetAllInstance(ctx context.Context, request *instance.GetAllInstancesRequest, opts ...grpc.CallOption) (*instance.InstanceList, error) {
	response := new(instance.InstanceList)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/GetAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Sync(instances []*instance.Instance) (response *instance.CommonResponse, err error) {
	if instances != nil {
		if data, marshalErr := json.Marshal(instances); marshalErr == nil {
			c.logger.Infof("rsyncing instance: %s", string(data))
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	req := &instance.SynInstancesRequest{Instance: instances}
	before := time.Now()
	response, err = c.service.SynInstance(ctx, req)
	c.metrics.ObserveSyncOnceDuration(time.Since(before))
	c.metrics.MarkSyncOnce()
	if err != nil {
		c.logger.Errorf("Sync fail: %v instance: %v", err, req.Instance)
	}
	if response != nil && response.Code != 0 {
		return response, fmt.Errorf("SynInstance failed with code: %d,error: %s", response.Code, response.Msg)
	}
	return response, err
}

func (c *Client) SyncAll(instances []*instance.Instance) (response *instance.CommonResponse, err error) {
	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	req := &instance.SynAllInstancesRequest{Instance: instances}
	response, err = c.service.SynAllInstance(ctx, req)
	if err != nil {
		c.logger.Errorf("SyncAll fail: %v instance: %v", err, req.Instance)
	}
	if response != nil && response.Code != 0 {
		return response, fmt.Errorf("SynAllInstance failed with code: %d,error: %s", response.Code, response.Msg)
	}
	return response, err
}

func (c *Client) GetAll(statuses []int32, provider string) (*instance.InstanceList, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	instances := &instance.InstanceList{Instance: []*instance.Instance{}}
	for _, status := range statuses {
		req := &instance.GetAllInstancesRequest{Status: status, Provider: provider}
		list, err := c.service.GetAllInstance(ctx, req)
		if err != nil {
			c.logger.Errorf("GetAll fail: %v req: %v", err, req)
			return nil, err
		}
		if provider != "" && list != nil && len(list.Instance) > 0 {
			for _, instance := range list.Instance {
				if instance.Provider == provider {
					instances.Instance = append(instances.Instance, instance)
				}
			}
		}
	}
	return instances, nil
}

// Close releases a connection created by Dial. It is safe to call repeatedly.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

type nopMetricsRecorder struct{}

func (nopMetricsRecorder) ObserveSyncOnceDuration(time.Duration) {}

func (nopMetricsRecorder) ObserveSyncAllDuration(string, time.Duration) {}

func (nopMetricsRecorder) SetSyncErrorQueueDepth(string, int) {}

func (nopMetricsRecorder) MarkSyncOnce() {}

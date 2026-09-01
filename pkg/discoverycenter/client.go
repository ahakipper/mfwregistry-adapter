package discoverycenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	"spotter/internal/ports"
	v2 "spotter/pkg/beehive/service/v2"
)

const readTimeout = 10 * time.Second

// Client calls the discovery-center instance service.
type Client struct {
	service v2.InstanceServiceClient
	logger  ports.Logger
	metrics ports.MetricsRecorder

	connMu sync.Mutex
	conn   *grpc.ClientConn
}

// NewClient creates a client from an already constructed instance service.
func NewClient(service v2.InstanceServiceClient, logger ports.Logger, metrics ports.MetricsRecorder) (*Client, error) {
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

func (c *grpcServiceClient) SynInstance(ctx context.Context, request *v2.SynInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *grpcServiceClient) SynAllInstance(ctx context.Context, request *v2.SynAllInstancesRequest, opts ...grpc.CallOption) (*v2.CommonResponse, error) {
	response := new(v2.CommonResponse)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/SynAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *grpcServiceClient) GetAllInstance(ctx context.Context, request *v2.GetAllInstancesRequest, opts ...grpc.CallOption) (*v2.InstanceList, error) {
	response := new(v2.InstanceList)
	if err := c.conn.Invoke(ctx, "/service.v2.InstanceService/GetAllInstance", request, response, opts...); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *Client) Sync(instances []*v2.Instance) (response *v2.CommonResponse, err error) {
	if instances != nil {
		if data, marshalErr := json.Marshal(instances); marshalErr == nil {
			c.logger.Infof("rsyncing instance: %s", string(data))
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	req := &v2.SynInstancesRequest{Instance: instances}
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

func (c *Client) SyncAll(instances []*v2.Instance) (response *v2.CommonResponse, err error) {
	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	req := &v2.SynAllInstancesRequest{Instance: instances}
	response, err = c.service.SynAllInstance(ctx, req)
	if err != nil {
		c.logger.Errorf("SyncAll fail: %v instance: %v", err, req.Instance)
	}
	if response != nil && response.Code != 0 {
		return response, fmt.Errorf("SynAllInstance failed with code: %d,error: %s", response.Code, response.Msg)
	}
	return response, err
}

func (c *Client) GetAll(statuses []int32, provider string) (*v2.InstanceList, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), readTimeout)
	defer cancel()
	instances := &v2.InstanceList{Instance: []*v2.Instance{}}
	for _, status := range statuses {
		req := &v2.GetAllInstancesRequest{Status: status, Provider: provider}
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

func (nopMetricsRecorder) SetSyncErrorQueueDepth(int) {}

func (nopMetricsRecorder) MarkSyncOnce() {}

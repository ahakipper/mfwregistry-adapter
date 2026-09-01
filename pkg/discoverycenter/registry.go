package discoverycenter

import (
	"errors"
	"strings"

	"spotter/internal/ports"
	v2 "spotter/pkg/beehive/service/v2"
)

// Pusher is the contract used by the workers to talk to the discovery center.
type Pusher interface {
	Push(triggerTime int64, instance []*v2.Instance) error
	PushAll(triggerTime int64, instance []*v2.Instance) error
	GetAll(enable []int32, provider string) (*v2.InstanceList, error)
}

// DiscoveryCenter is the client of the discovery center (Atlas).
type DiscoveryCenter struct {
	client      *Client
	logger      ports.Logger
	notifier    ports.Notifier
	disablePush bool
}

// NewDiscoveryCenter creates a pusher bound to an explicit discovery client.
func NewDiscoveryCenter(client *Client, logger ports.Logger, notifier ports.Notifier, disablePush bool) (*DiscoveryCenter, error) {
	if client == nil {
		return nil, errors.New("discoverycenter: client is required")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if notifier == nil {
		notifier = nopNotifier{}
	}
	return &DiscoveryCenter{
		client:      client,
		logger:      logger,
		notifier:    notifier,
		disablePush: disablePush,
	}, nil
}

func (mr *DiscoveryCenter) Push(triggerTime int64, instance []*v2.Instance) error {
	if mr.disablePush {
		mr.logger.Infof("perform a fake push operation, instances: %v", instance)
		return nil
	}
	ids := make([]string, 0, len(instance))
	for _, ins := range instance {
		ids = append(ids, ins.InstanceId)
	}
	res, err := mr.client.Sync(instance)
	if err != nil {
		mr.notifier.Notify("Failed to sync data incrementally", err.Error())
		var code int32
		var msg string
		if res != nil {
			code = res.GetCode()
			msg = res.GetMsg()
		}
		mr.logger.Infof("synced instance to the discovery center failed, instance ids: %s, rpc code: %d, rpc error: %s", strings.Join(ids, ","), code, msg)
		return err
	}
	mr.logger.Infof("synced instance to the discovery center successfully, instance ids: %s", strings.Join(ids, ","))
	return nil
}

func (mr *DiscoveryCenter) PushAll(triggerTime int64, instance []*v2.Instance) error {
	res, err := mr.client.SyncAll(instance)
	if err != nil {
		mr.notifier.Notify("Failed to sync all data", err.Error())
		return err
	}
	mr.logger.Info(res)
	return nil
}

func (mr *DiscoveryCenter) GetAll(statuses []int32, provider string) (*v2.InstanceList, error) {
	return mr.client.GetAll(statuses, provider)
}

// Close releases the client connection when this registry owns a dialed client.
func (mr *DiscoveryCenter) Close() error {
	if mr == nil || mr.client == nil {
		return nil
	}
	return mr.client.Close()
}

type nopNotifier struct{}

func (nopNotifier) Notify(string, string) {}

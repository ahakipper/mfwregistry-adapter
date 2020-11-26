package mfwregistry

import (
	"context"
	"encoding/json"
	v2 "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
	"gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
	"google.golang.org/grpc"
	"time"
)

const(
	connectTimeout = 5
	readTimeout = 10
	sleepTime = 5
	connectRetryCount = 3
)

type Client struct {
	service v2.InstanceServiceClient
}

func NewInstance() (ins *Client, err error) {
	c := &Client{}
	if err = c.getConnect(); err != nil {
		log.Logger.Errorf("get instance err:%s", err)
		return nil, err
	}
	return c, err
}

func (c *Client) Sync(instance []*v2.Instance) (r *v2.CommonResponse, err error) {
	if instance != nil {
		if d, e := json.Marshal(instance); e == nil {
			log.Logger.Infof("rsyncing instance: %s", string(d))

		}
	}
	ctx,_ := context.WithTimeout(context.TODO(),time.Second * readTimeout)
	req := new(v2.SynInstancesRequest)
	req.Instance = instance
	r, err = c.service.SynInstance(ctx, req)
	if err != nil {
		log.Logger.Errorf("Sync fail: %v instance: %v", err,req.Instance)
	}
	return
}

func (c *Client) SyncAll(instance []*v2.Instance) (r *v2.CommonResponse, err error) {
	ctx,_ := context.WithTimeout(context.TODO(),time.Second * readTimeout)
	req := new(v2.SynAllInstancesRequest)
	req.Instance = instance
	r, err = c.service.SynAllInstance(ctx, req)
	if err != nil {
		log.Logger.Errorf("SyncAll fail: %v instance: %v", err,req.Instance)
	}
	return
}

func (c *Client) GetAll(status int32) (r *v2.InstanceList, err error) {
	ctx,_ := context.WithTimeout(context.TODO(),time.Second * readTimeout)
	req := new(v2.GetAllInstancesRequest)
	req.Status = status
	r,err = c.service.GetAllInstance(ctx,req)
	if err != nil {
		log.Logger.Errorf("GetAll fail: %v req: %v", err,req)
	}
	return
}

func (c *Client) getConnect() (err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*connectTimeout)
	var conn *grpc.ClientConn
	for i := 0; i < connectRetryCount; i++ {
		conn, err = grpc.DialContext(ctx, config.GrpcAddr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Logger.Errorf("connect fail: %s", err)
			time.Sleep(time.Second * sleepTime)
		} else {
			break
		}
	}
	if err != nil {
		return err
	}
	service := v2.NewInstanceServiceClient(conn)
	c.service = service
	return nil
}

package etcd

import (
    "context"
    "errors"
    "github.com/coreos/etcd/clientv3"
    "github.com/coreos/etcd/pkg/transport"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "time"
)

func NewEtcdClient() (client *clientv3.Client, err error) {
    cfg := clientv3.Config{
        Endpoints: config.EtcdEndpoints,
    }

    if config.CAFile != "" && config.KeyFile != "" && config.CertFile != "" {
        tlsInfo := transport.TLSInfo{
            CertFile: config.CertFile,
            KeyFile:  config.KeyFile,
            CAFile:   config.CAFile,
        }
        tlsConfig, err := tlsInfo.ClientConfig()
        if err != nil {
            return nil, err
        }
        cfg.TLS = tlsConfig
    }
    client, err = clientv3.New(cfg)
    if err != nil {
        return nil, err
    } else {
        ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
        if _, er := client.Cluster.MemberList(ctx); er != nil {
            return nil, errors.New("connect to etcd server failed: " + er.Error())
        }
    }

    return
}

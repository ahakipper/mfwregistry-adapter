package etcd

import (
	"context"
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"spotter/config"
	"time"
)

// NewClientWithEndpoints creates an etcd client from explicit endpoints and
// TLS file paths. It mirrors NewEtcdClient (including the 5 second
// member-list connectivity probe) but takes its inputs as parameters
// instead of reading the config package globals. Empty TLS file values
// disable TLS, exactly like NewEtcdClient.
func NewClientWithEndpoints(endpoints []string, certFile, keyFile, caFile string) (client *clientv3.Client, err error) {
	cfg := clientv3.Config{
		Endpoints: endpoints,
	}

	if caFile != "" && keyFile != "" && certFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile: certFile,
			KeyFile:  keyFile,
			CAFile:   caFile,
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

// NewEtcdClient creates an etcd client from the config package globals. It
// is the legacy wrapper kept for callers that are not wired through the
// composition root yet.
func NewEtcdClient() (client *clientv3.Client, err error) {
	return NewClientWithEndpoints(config.EtcdEndpoints, config.CertFile, config.KeyFile, config.CAFile)
}

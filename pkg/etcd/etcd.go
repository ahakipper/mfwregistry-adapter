package etcd

import (
	"context"
	"errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/client/v3"
	"time"
)

// NewClientWithEndpoints creates an etcd client from explicit endpoints and
// TLS file paths. It probes connectivity with a 5 second member-list
// deadline and returns an error if the cluster is unreachable. Empty TLS
// file values disable TLS.
func NewClientWithEndpoints(endpoints []string, certFile, keyFile, caFile string) (client *clientv3.Client, err error) {
	cfg := clientv3.Config{
		Endpoints: endpoints,
	}

	if caFile != "" && keyFile != "" && certFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      certFile,
			KeyFile:       keyFile,
			TrustedCAFile: caFile,
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

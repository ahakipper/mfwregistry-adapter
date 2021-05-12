package discovery

import (
    "context"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/net"
    "time"

    "github.com/coreos/etcd/clientv3"
    "github.com/coreos/etcd/mvcc/mvccpb"

    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/cache"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
)

var (
    // Indicates the prefix key of register center, store in etcd
    registerCenter = "/paas/mfwregistry-k8sadapter/register/"

    // pushChannelBaseKey the base key of slave node subscript channel
    pushChannelBaseKey = "/paas/mfwregistry-k8sadapter/subscribech"

    // Operate ectd timeout
    timeout = 5 * time.Second
)

type Node interface {
    // PushChannel return the channel of this node subscription
    PushChannel() string

    // KeepAlive send the heart beat package to etcd
    // Make sure this node is active
    Keepalive() error

    // watch the status of other nodes in this cluster
    Watch() error

    // GetClusterInfo get all node info of this cluster
    GetClusterInfo() map[string]interface{}

    // Quit represent node quit
    Quit()
}

// node implement Node interface
type node struct {
    key     string
    leaseID clientv3.LeaseID
    ctx     context.Context

    // table store cluster info map[pushChannel]IP
    table      cache.Table
    etcdclient *clientv3.Client
}

// NewNode init node info
func NewNode(ctx context.Context, etcdclient *clientv3.Client, key string, id clientv3.LeaseID) Node {
    return &node{
        key:        key,
        ctx:        ctx,
        leaseID:    id,
        etcdclient: etcdclient,
    }
}

// PushChannel return the channel of this node subscription
func (n *node) PushChannel() string {
    return pushChannelBaseKey + n.key
}

func (n *node) keepalive() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
    ctx, cancel := context.WithTimeout(n.ctx, timeout)
    ip := net.GetIPAddress()
    if ip == "" {
        ip = "none"
    }
    if _, err := n.etcdclient.Put(ctx, registerCenter+n.key, ip, clientv3.WithLease(n.leaseID)); err != nil {
        log.Logger.Error("keepAlive error:", err)
        return nil, err
    }
    cancel()

    return n.etcdclient.KeepAlive(n.ctx, n.leaseID)
}

func (n *node) revoke() {
    ctx, cancel := context.WithTimeout(n.ctx, timeout)
    defer cancel()
    if _, err := n.etcdclient.Revoke(ctx, n.leaseID); err != nil {
        log.Logger.Error("revoke error", err)
    }
}

// Keepalive use for server keepalive
func (n *node) Keepalive() (e error) {
    resp, err := n.keepalive()
    if err != nil {
        log.Logger.Error("Start error", err)
        return e
    }

    for range resp {
        //log.Info("Node is alive key:%s, ttl:%d", n.key)
    }
    log.Logger.Info("node has dead")

    return nil
}

// Watch observe cluster changes
func (n *node) Watch() (e error) {
    // first get all cluster info from etcd
    ctx, cancel := context.WithTimeout(n.ctx, timeout)
    resp, err := n.etcdclient.Get(ctx, registerCenter, clientv3.WithPrefix())
    if err != nil {
        log.Logger.Error("WatchCenter error:", err)
        return e
    }
    cancel()

    for _, kv := range resp.Kvs {
        n.table.Add(pushChannelBaseKey+string(kv.Key)[len(registerCenter):], string(kv.Value), 0)
    }

    // get current version for next watch
    currentVersion := resp.Header.Revision + 1

    // watch new changes according to the current version
    watcher := clientv3.NewWatcher(n.etcdclient)
    watchChan := watcher.Watch(n.ctx, registerCenter, clientv3.WithPrefix(), clientv3.WithRev(currentVersion))

    for watchResp := range watchChan {
        for _, event := range watchResp.Events {
            pushChannel := pushChannelBaseKey + string(event.Kv.Key)[len(registerCenter):]
            switch event.Type {
            case mvccpb.PUT:
                n.table.Add(pushChannel, string(event.Kv.Value), 0)
            case mvccpb.DELETE:
                n.table.Delete(pushChannel)
            }
        }
    }
    log.Logger.Info("node stop watch cluster changes")

    return nil
}

func (n *node) Quit() {
    // node quit, release providers
    n.table.Clean()

    n.revoke()
}

// GetClusterInfo get cluster info, return the specific key and ip map of this cluster
func (n *node) GetClusterInfo() map[string]interface{} {
    return n.table.Range()
}

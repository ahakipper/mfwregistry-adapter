package election

import (
    "context"
    "errors"
    "github.com/coreos/etcd/clientv3"
    "github.com/coreos/etcd/clientv3/concurrency"
    uuid "github.com/satori/go.uuid"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "sync"
    "time"
)

const (
    CampainTimeout     = 10
    LeaderChangePeriod = 2
)

// Indicates the prefix key for participating in the campaign, store in etcd
var campaignCenter = "/paas/mfwregistry-k8sadapter"

type Candidate interface {
    // Campaign puts a value as eligible for the election. It blocks until
    // it is elected, an error occurs, or the context is cancelled.
    Campaign(timeout time.Duration) error

    // IsLeader judge this candidate whether it is a leader
    IsLeader() bool

    // Resign lets a leader start a new election.
    Resign() error

    // AddObserveCallFunc add a callback func for leader changes
    AddObserveCallFunc(f LeaderChangeFunc)

    // Tag represent a tag for this node participate in the election campaign
    Tag() string

    // Wait will
    Wait()

    // LeaseID return the lease id for this candidate need to keepalive
    LeaseID() clientv3.LeaseID
}

// node implement Candidate
type candidate struct {
    ctx context.Context

    election *concurrency.Election

    tag string

    client *clientv3.Client

    leaseID clientv3.LeaseID

    callBackFuncs []LeaderChangeFunc

    sync.Mutex
}

type LeaderChangeFunc func(isLeader bool)

// NewCandidate new a Candidate
func NewCandidate(ctx context.Context, etcdclient *clientv3.Client) (can Candidate, err error) {
    if etcdclient == nil {
        return nil, errors.New("invalid etcd client")
    }
    cd := &candidate{
        ctx:           ctx,
        client:        etcdclient,
        callBackFuncs: []LeaderChangeFunc{},
    }
    //
    cd.tag = uuid.NewV4().String()
    // if server shutdown by panic, this guarantee the leader will resign soon.
    resp, err := cd.client.Grant(ctx, 0)
    if err != nil {
        // log
        return nil, err
    }
    leaseID := resp.ID
    session, err := concurrency.NewSession(cd.client, concurrency.WithLease(leaseID))
    if err != nil {
        // log
        return nil, err
    }
    cd.election = concurrency.NewElection(session, campaignCenter)

    // sleep a while
    // time.Sleep(time.Millisecond * 1000)

    return cd, nil
}

func (c *candidate) NewElectionSession(timeout time.Duration) {
    c.Lock()
    defer c.Unlock()
    timeoutctx, _ := context.WithTimeout(context.Background(), timeout)
    c.tag = uuid.NewV4().String()
    // if server shutdown by panic, this guarantee the leader will resign soon.
    resp, err := c.client.Grant(timeoutctx, 0)
    if err != nil {
        // log
        return
    }
    leaseID := resp.ID
    session, err := concurrency.NewSession(c.client, concurrency.WithLease(leaseID), concurrency.WithTTL(10))
    if err != nil {
        // log
        return
    }
    c.election = concurrency.NewElection(session, campaignCenter)
    // sleep a while
    // time.Sleep(time.Millisecond * 1000)
}

func (c *candidate) Wait() {
    for {
        select {
        case <-c.ctx.Done():
            log.Logger.Info("exit the candidate")
            return
        default:
            isLeader := c.IsLeader()
            for _, call := range c.callBackFuncs {
                // Here note!!!!
                // must use goroutine for asynchronous notification to prevent it from blocking elections
                go call(isLeader)
            }
            if !isLeader {
                c.NewElectionSession(CampainTimeout * time.Second)
                c.Campaign(CampainTimeout * time.Second)
            }
            time.Sleep(LeaderChangePeriod * time.Second)
        }
    }
}

func (c *candidate) Campaign(timeout time.Duration) (err error) {
    c.Lock()
    defer c.Unlock()
    timeoutctx, _ := context.WithTimeout(context.Background(), timeout)
    if err = c.election.Campaign(timeoutctx, c.tag); err != nil {
        if err == context.Canceled {
        } else {
            return
        }
        return
    }
    log.Logger.Info("Campaign finish, I`m leader")

    return nil
}

func (c *candidate) IsLeader() bool {
    resp, err := c.election.Leader(c.ctx)
    if err != nil {
        return false
    }
    return string(resp.Kvs[0].Value) == c.tag
}

func (c *candidate) Resign() (err error) {
    if err = c.election.Resign(c.ctx); err != nil {
        return
    }
    log.Logger.Info("leader resign")

    return
}

func (c *candidate) AddObserveCallFunc(callback LeaderChangeFunc) {
    if callback != nil {
        c.callBackFuncs = append(c.callBackFuncs, callback)
    }
}

func (c *candidate) Tag() string {
    return c.tag
}

func (c *candidate) LeaseID() clientv3.LeaseID {
    return c.leaseID
}

package election

import (
	"context"
	"errors"
	uuid "github.com/satori/go.uuid"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"spotter/config"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"sync"
	"time"
)

const (
	CampainTimeout     = 10
	LeaderChangePeriod = 2
)

type Candidate interface {
	// Campaign puts a value as eligible for the election. It blocks until
	// it is elected, an error occurs, or the context is cancelled.
	Campaign(timeout time.Duration) error

	// IsLeader judge this candidate whether it is a leader
	IsLeader() (bool, error)

	// AddObserveCallFunc add a callback func for leader changes
	AddObserveCallFunc(f LeaderChangeFunc)

	// Wait will
	Wait()
}

// node implement Candidate
type candidate struct {
	ctx context.Context

	election *concurrency.Election

	session *concurrency.Session

	// campaignKey is the etcd prefix key this candidate campaigns on.
	campaignKey string

	tag string

	client *clientv3.Client

	callBackFuncs []LeaderChangeFunc

	sync.Mutex
}

type LeaderChangeFunc func(isLeader bool)

// NewCandidate new a Candidate
//
// campaignKey is the etcd prefix key used for the leader campaign (the
// legacy value came from the config.LockCampaignKey global). Pass an empty
// campaignKey to fall back to that global, which keeps older callers
// working unchanged.
func NewCandidate(ctx context.Context, etcdclient *clientv3.Client, campaignKey string) (can Candidate, err error) {
	if etcdclient == nil {
		return nil, errors.New("invalid etcd client")
	}
	if campaignKey == "" {
		campaignKey = config.LockCampaignKey
	}
	cd := &candidate{
		ctx:           ctx,
		client:        etcdclient,
		campaignKey:   campaignKey,
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
	session, err := concurrency.NewSession(cd.client, concurrency.WithLease(resp.ID))
	if err != nil {
		// log
		return nil, err
	}
	cd.session = session
	cd.election = concurrency.NewElection(session, campaignKey)

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
	session, err := concurrency.NewSession(c.client, concurrency.WithLease(resp.ID), concurrency.WithTTL(10))
	if err != nil {
		// log
		return
	}
	c.session = session
	c.election = concurrency.NewElection(session, c.campaignKey)
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
			time.Sleep(LeaderChangePeriod * time.Second)
			isLeader, err := c.IsLeader()
			if err != nil && err != concurrency.ErrElectionNoLeader {
				log.Logger.Errorf("get leader state error: %s", err.Error())
				continue
			}
			for _, call := range c.callBackFuncs {
				// Here note!!!!
				// must use goroutine for asynchronous notification to prevent it from blocking elections
				go call(isLeader)
			}
			if !isLeader {
				// Relese the candidate resources
				c.Close()
				// Assign new candidate resources
				c.NewElectionSession(CampainTimeout * time.Second)
				c.Campaign(CampainTimeout * time.Second)
			}
		}
	}
}

func (c *candidate) Campaign(timeout time.Duration) (err error) {
	c.Lock()
	defer c.Unlock()
	timeoutctx, _ := context.WithTimeout(context.Background(), timeout)
	if err = c.election.Campaign(timeoutctx, c.tag); err != nil {
		if err == context.Canceled {
		}
		if err.Error() != "context deadline exceeded" {
			// Notice
			notice.Notice("Candidate server node election failed", err.Error())
		}
		return
	}
	log.Logger.Info("Campaign finish, I`m leader")

	return nil
}

func (c *candidate) IsLeader() (bool, error) {
	resp, err := c.election.Leader(c.ctx)
	if err != nil {
		return false, err
	}

	return string(resp.Kvs[0].Value) == c.tag, nil
}

func (c *candidate) AddObserveCallFunc(callback LeaderChangeFunc) {
	if callback != nil {
		c.callBackFuncs = append(c.callBackFuncs, callback)
	}
}

func (c *candidate) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

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

	// observed deduplicates consecutive leadership observations so ticks
	// without a change are not re-dispatched (F5b). Only the Wait loop
	// goroutine touches it.
	observed leaderState

	callBackFuncs []LeaderChangeFunc

	sync.Mutex
}

// leaderState deduplicates consecutive leadership observations: the first
// observed value and every change dispatch; repeated identical values do
// not.
type leaderState struct {
	known bool
	value bool
}

// shouldDispatch reports whether a newly observed value differs from the
// last dispatched one and records the transition. The first observation
// always dispatches.
func (s *leaderState) shouldDispatch(value bool) bool {
	if s.known && s.value == value {
		return false
	}
	s.known = true
	s.value = value
	return true
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
	timeoutctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c.tag = uuid.NewV4().String()
	// Clear the stale pointers up front: a failed rebuild must leave the
	// candidate WITHOUT a session rather than pointing at the closed one,
	// so the recovery path never re-campaigns against a dead session (F3).
	c.session = nil
	c.election = nil
	// if server shutdown by panic, this guarantee the leader will resign soon.
	resp, err := c.client.Grant(timeoutctx, 0)
	if err != nil {
		// F3: the failure was silent before, leaving the closed session
		// in place with zero observability. Log it; the Wait loop retries
		// the rebuild on its next tick.
		log.Logger.Errorf("election session rebuild failed: grant lease: %s", err.Error())
		return
	}
	session, err := concurrency.NewSession(c.client, concurrency.WithLease(resp.ID), concurrency.WithTTL(10))
	if err != nil {
		log.Logger.Errorf("election session rebuild failed: new session: %s", err.Error())
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
			// Dispatch callbacks only when the observed leadership
			// CHANGED from the previous tick (F5b). The legacy code fired
			// every callback on every 2s tick (~30 events/min/channel
			// forever). This is behavior-compatible for the only consumer
			// (the server), which already deduplicates identical values
			// (server.go: `isLeader == s.isLeader → continue`); the first
			// observation still dispatches. The channel-send blocking
			// hazard is deferred to the next phase (channel semantics
			// redesign).
			if c.observed.shouldDispatch(isLeader) {
				// Here note!!!!
				// must use goroutine for asynchronous notification to prevent it from blocking elections
				go c.notify(isLeader)
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

// notify dispatches isLeader to every registered leader-change callback
// with panic isolation. Callbacks are snapshotted under the candidate
// mutex so registration concurrent with a dispatch is safe.
func (c *candidate) notify(isLeader bool) {
	c.Lock()
	callbacks := make([]LeaderChangeFunc, len(c.callBackFuncs))
	copy(callbacks, c.callBackFuncs)
	c.Unlock()
	for _, call := range callbacks {
		c.dispatch(call, isLeader)
	}
}

// dispatch invokes a leader-change callback with panic isolation (F5a): a
// panicking callback is recovered and logged instead of crashing the
// process. The legacy bare `go call(isLeader)` let one bad callback take
// the whole server down.
func (c *candidate) dispatch(call LeaderChangeFunc, isLeader bool) {
	defer func() {
		if e := recover(); e != nil {
			log.Logger.Errorf("leader change callback panicked: %v", e)
		}
	}()
	call(isLeader)
}

func (c *candidate) Campaign(timeout time.Duration) (err error) {
	c.Lock()
	defer c.Unlock()
	timeoutctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if c.election == nil {
		// F3: no live session to campaign on (the rebuild failed or has
		// not run yet); Wait retries the rebuild on its next tick.
		return errors.New("no election session to campaign on")
	}
	if err = c.election.Campaign(timeoutctx, c.tag); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			// Expected when the campaign did not win within the timeout.
			return err
		}
		// Notice
		notice.Notice("Candidate server node election failed", err.Error())
		return err
	}
	log.Logger.Info("Campaign finish, I`m leader")

	return nil
}

func (c *candidate) IsLeader() (bool, error) {
	c.Lock()
	election := c.election
	c.Unlock()
	if election == nil {
		// F3: no live session; report no leadership so Wait keeps
		// retrying the rebuild instead of panicking on a stale pointer.
		return false, concurrency.ErrElectionNoLeader
	}
	resp, err := election.Leader(c.ctx)
	if err != nil {
		return false, err
	}

	return string(resp.Kvs[0].Value) == c.tag, nil
}

func (c *candidate) AddObserveCallFunc(callback LeaderChangeFunc) {
	if callback != nil {
		c.Lock()
		c.callBackFuncs = append(c.callBackFuncs, callback)
		c.Unlock()
	}
}

func (c *candidate) Close() {
	c.Lock()
	session := c.session
	c.session = nil
	c.election = nil
	c.Unlock()
	if session != nil {
		session.Close()
	}
}

package worker

import (
	"context"
	"go.etcd.io/etcd/client/v3"
	"spotter/config"
	"spotter/pkg/distribute/election"
	"spotter/pkg/etcd"
	"spotter/pkg/log"
	"sync"
	"sync/atomic"
	"time"
)

// Elector is used for master-slave election
type Elector interface {
	// Start the distribute node
	// init distributed related work
	ElectWait()

	// Stop represent node exit
	Stop()
}

// worker implement Worker interface
type ElectWorker struct {
	ctx context.Context

	// candidate use for distributed election
	candidate election.Candidate

	etcdclient *clientv3.Client

	logger logger

	// stopped is read from leader-change callback goroutines dispatched by
	// the candidate and written by Stop/syncStoppedState, so every access
	// goes through atomic operations (F1: unsynchronized plain field
	// accesses were a data race).
	stopped atomic.Bool

	// closeClientOnce guards closing etcdclient so repeated Stop calls
	// close the owned client exactly once (F6).
	closeClientOnce sync.Once
}

// logger is the minimal logging surface the elector needs. It is satisfied
// by ports.Logger implementations as well as the pkg/log global logger.
type logger interface {
	Info(args ...interface{})
}

// NewElectorWithDeps creates an elector from explicit etcd endpoints, TLS
// file paths, the campaign key and a logger, instead of reading the
// config/log package globals. It mirrors NewElector.
func NewElectorWithDeps(ctx context.Context, leaderChCh chan bool, endpoints []string, certFile, keyFile, caFile, campaignKey string, logger logger) (Elector, error) {
	etcdclient, err := etcd.NewClientWithEndpoints(endpoints, certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	candite, err := election.NewCandidate(ctx, etcdclient, campaignKey)
	if err != nil {
		// The elector will not own the client after this return, so
		// release it here instead of leaking it (F6 hygiene).
		_ = etcdclient.Close()
		return nil, err
	}
	ew := &ElectWorker{
		ctx:        ctx,
		candidate:  candite,
		etcdclient: etcdclient,
		logger:     logger,
	}
	ew.setLeaderChangeNotifyCall(leaderChCh)
	return ew, nil
}

func NewElector(ctx context.Context, leaderChCh chan bool) (Elector, error) {
	return NewElectorWithDeps(ctx, leaderChCh, config.EtcdEndpoints, config.CertFile, config.KeyFile, config.CAFile, config.LockCampaignKey, log.Logger)
}

// ElectWait will perform the behavior of electing the leader. It will always block and notify the relevant channel
// of leader changes. It returns once the context passed at construction is
// canceled (the candidate's Wait loop observes the cancellation).
func (w *ElectWorker) ElectWait() {
	// Watch for owner-side cancellation once per call (F2: the legacy
	// code spawned a never-exiting syncStoppedState goroutine per loop
	// iteration, which spun at 100% CPU after cancellation and leaked).
	go w.syncStoppedState()
	for {
		// The owner cancels the context on shutdown; the candidate's
		// Wait then returns immediately, so exit here instead of
		// re-campaigning in a tight loop forever (F2).
		select {
		case <-w.ctx.Done():
			return
		default:
		}
		// start election campaign
		w.candidate.Campaign(election.CampainTimeout * time.Second)

		// waiting for loop election
		w.candidate.Wait()
	}
}

func (w *ElectWorker) setLeaderChangeNotifyCall(ch chan bool) {
	var callback election.LeaderChangeFunc = func(isLeader bool) {
		// send leader changes to the channel if the elector is not stopped
		if !w.stopped.Load() {
			// pp.Println("leader change func: ", isLeader, "stopped", w.stopped, &ch)
			ch <- isLeader
		}
	}
	w.candidate.AddObserveCallFunc(callback)
}

// Stop the worker, if the worker`s identity is leader
//
// Stop is advisory: it marks the elector stopped (suppressing further
// leader-change forwarding) and closes the etcd client this worker owns
// (only when constructed via NewElectorWithDeps; test-constructed workers
// keep a nil client). It deliberately does NOT cancel the elector context:
// the context owner (the server) is responsible for that, and canceling it
// here would race the owner's own cancellation. Closing the client ends
// the underlying session leases, letting the cluster elect a new leader.
func (w *ElectWorker) Stop() {
	w.stopped.Store(true)
	w.closeOwnedClient()
	w.logStop()
}

// closeOwnedClient closes the etcd client exactly once, and only when the
// worker actually owns one.
func (w *ElectWorker) closeOwnedClient() {
	if w.etcdclient == nil {
		return
	}
	w.closeClientOnce.Do(func() {
		if err := w.etcdclient.Close(); err != nil {
			w.logStopCloseError(err)
			return
		}
		w.loggerInfo("etcd client closed by elector stop")
	})
}

// logStopCloseError emits a client-close failure through the injected
// logger, falling back to the pkg/log global when none was provided.
func (w *ElectWorker) logStopCloseError(err error) {
	if w.logger != nil {
		w.logger.Info("distribute worker stop close etcd client error: ", err.Error())
		return
	}
	log.Logger.Info("distribute worker stop close etcd client error: ", err.Error())
}

// loggerInfo emits an informational line through the injected logger,
// falling back to the pkg/log global when none was provided.
func (w *ElectWorker) loggerInfo(args ...interface{}) {
	if w.logger != nil {
		w.logger.Info(args...)
		return
	}
	log.Logger.Info(args...)
}

// logStop emits the legacy stop log through the injected logger, falling
// back to the pkg/log global when no logger was provided.
func (w *ElectWorker) logStop() {
	if w.logger != nil {
		w.logger.Info("distribute worker stop")
		return
	}
	log.Logger.Info("distribute worker stop")
}

// syncStoppedState is a one-shot watcher: it parks until the owner cancels
// the elector context, marks the elector stopped, and returns (F2: the
// legacy single-case select looped forever after cancellation, spinning at
// 100% CPU).
func (w *ElectWorker) syncStoppedState() {
	<-w.ctx.Done()
	w.stopped.Store(true)
}

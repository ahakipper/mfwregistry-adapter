package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"spotter/config"
	"spotter/pkg/distribute/election"
	"spotter/pkg/etcd"
	"spotter/pkg/log"
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

	stopped bool
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
// of leader changes
func (w *ElectWorker) ElectWait() {
	for {
		// start election campaign
		w.candidate.Campaign(election.CampainTimeout * time.Second)

		go w.syncStoppedState()

		// waiting for loop election
		w.candidate.Wait()
	}
}

func (w *ElectWorker) setLeaderChangeNotifyCall(ch chan bool) {
	var callback election.LeaderChangeFunc = func(isLeader bool) {
		// send leader changes to the channel if the elector is not stopped
		if !w.stopped {
			// pp.Println("leader change func: ", isLeader, "stopped", w.stopped, &ch)
			ch <- isLeader
		}
	}
	w.candidate.AddObserveCallFunc(callback)
}

// Stop the worker, if the worker`s identity is leader
func (w *ElectWorker) Stop() {
	w.stopped = true
	w.logStop()
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

func (w *ElectWorker) syncStoppedState() {
	for {
		select {
		case <-w.ctx.Done():
			w.stopped = true
		}
	}
}

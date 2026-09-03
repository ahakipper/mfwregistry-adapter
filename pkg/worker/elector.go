package worker

import (
	"context"
	"errors"
	"go.etcd.io/etcd/client/v3"
	"spotter/config"
	"spotter/internal/ports"
	"spotter/pkg/distribute/election"
	"spotter/pkg/etcd"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"sync"
	"sync/atomic"
	"time"
)

// Elector is used for master-slave election
type Elector interface {
	// Start the distribute node
	// init distributed related work
	//
	// changes receives the leader-change notifications (true = became
	// leader, false = lost leadership); the signature matches
	// internal/ports.LeaderElector so ElectWorker satisfies the port.
	ElectWait(changes chan<- bool)

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

	// leaderCh is the current leader-change forwarding target, bound at
	// construction and rebindable at ElectWait time (seam 5). leaderChMu
	// guards it because dispatch goroutines read it while ElectWait may
	// rebind it.
	leaderChMu sync.RWMutex
	leaderCh   chan<- bool

	// callbackRegistered ensures the forwarding callback is registered with
	// the candidate exactly once no matter how often the target is rebound.
	callbackRegistered bool
}

// logger is the minimal logging surface the elector needs. It is satisfied
// by ports.Logger implementations as well as the pkg/log global logger.
type logger interface {
	Info(args ...interface{})
}

// ElectWorker satisfies the internal/ports.LeaderElector port exactly
// (seam 5: the port unification that previously only FakeLeaderElector
// satisfied; the compile-time check fails on any future signature drift).
var _ ports.LeaderElector = (*ElectWorker)(nil)

// loggerToPorts widens the elector's minimal logger seam to the full
// ports.Logger the candidate accepts. A nil logger stays nil so the
// candidate constructor applies its own default; a partial implementation
// (Info only) is widened with nop implementations for the remaining
// methods so the candidate never nil-derefs.
func loggerToPorts(l logger) ports.Logger {
	if l == nil {
		return nil
	}
	if full, ok := l.(ports.Logger); ok {
		return full
	}
	return infoOnlyLogger{l}
}

// infoOnlyLogger adapts an Info-only logger to ports.Logger with nop
// implementations of the remaining methods.
type infoOnlyLogger struct {
	logger
}

func (infoOnlyLogger) Infof(string, ...interface{})  {}
func (infoOnlyLogger) Warn(...interface{})           {}
func (infoOnlyLogger) Warnf(string, ...interface{})  {}
func (infoOnlyLogger) Error(...interface{})          {}
func (infoOnlyLogger) Errorf(string, ...interface{}) {}

// NewElectorWithDeps creates an elector from explicit etcd endpoints, TLS
// file paths, the campaign key, a logger and a notifier, instead of reading
// the config/log/notice package globals. It mirrors NewElector.
//
// The notifier receives campaign-failure pages (EMERGENCY level, unchanged
// from the legacy notice.Notice behavior); nil means the candidate falls
// back to its nop notifier and pages nothing.
func NewElectorWithDeps(ctx context.Context, leaderChCh chan bool, endpoints []string, certFile, keyFile, caFile, campaignKey string, logger logger, notifier ports.Notifier) (Elector, error) {
	etcdclient, err := etcd.NewClientWithEndpoints(endpoints, certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	candite, err := election.NewCandidateWithDeps(ctx, etcdclient, campaignKey, nil, loggerToPorts(logger), notifier)
	if err != nil {
		// The elector will not own the client after this return, so
		// release it here instead of leaking it (F6 hygiene).
		_ = etcdclient.Close()
		return nil, err
	}
	ew := newElectWorker(ctx, candite, leaderChCh, logger)
	// This constructor built the etcd client, so the worker owns it and
	// Stop closes it (F6). Candidates injected through
	// NewElectorWithCandidate leave the client nil because their owner
	// keeps it.
	ew.etcdclient = etcdclient
	return ew, nil
}

// NewElectorWithCandidate creates an elector around an already-built
// candidate instead of constructing the etcd client and candidate itself.
// This is the candidate-injection seam: tests (and future callers) can pass
// a fake election.Candidate and drive the elector deterministically without
// a live etcd. The worker owns no etcd client — etcdclient stays nil and
// Stop's nil guard handles that. A nil logger defaults to a nop logger so
// Stop never depends on the pkg/log global being initialized.
func NewElectorWithCandidate(ctx context.Context, candidate election.Candidate, leaderChCh chan bool, logger ports.Logger) (Elector, error) {
	if candidate == nil {
		return nil, errors.New("nil election candidate")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	return newElectWorker(ctx, candidate, leaderChCh, logger), nil
}

// newElectWorker is the shared builder every constructor funnels through:
// an injected candidate, the leader-change channel the notify callback
// forwards to, and an optional logger (nil keeps the pkg/log global
// fallback). The returned worker owns no etcd client.
func newElectWorker(ctx context.Context, candidate election.Candidate, leaderChCh chan bool, logger logger) *ElectWorker {
	ew := &ElectWorker{
		ctx:       ctx,
		candidate: candidate,
		logger:    logger,
	}
	ew.setLeaderChangeNotifyCall(leaderChCh)
	return ew
}

// legacyNotifier forwards elector notifications to the legacy global
// notifier. The legacy NewElector wrapper still reads the config/log
// globals, so it keeps paging through notice.Notice as well — the pre-E3
// behavior — instead of silently dropping campaign-failure pages.
type legacyNotifier struct{}

func (legacyNotifier) Notify(title, content string) {
	notice.Notice(title, content)
}

func NewElector(ctx context.Context, leaderChCh chan bool) (Elector, error) {
	return NewElectorWithDeps(ctx, leaderChCh, config.EtcdEndpoints, config.CertFile, config.KeyFile, config.CAFile, config.LockCampaignKey, log.Logger, legacyNotifier{})
}

// ElectWait will perform the behavior of electing the leader. It will
// always block and notify the given changes channel of leader changes,
// returning once the context passed at construction is canceled (the
// candidate's Wait loop observes the cancellation).
//
// The notify callback targets changes when it is non-nil — the channel is
// (re)bound at call time, which is what makes the signature match
// internal/ports.LeaderElector exactly. A nil changes keeps the
// constructor-bound target (the back-compat path for callers that already
// passed their channel to NewElectorWithDeps/NewElectorWithCandidate).
func (w *ElectWorker) ElectWait(changes chan<- bool) {
	if changes != nil {
		w.setLeaderChangeNotifyCall(changes)
	}
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

// setLeaderChangeNotifyCall registers the elector's single leader-change
// callback with the candidate and targets it at ch. The callback reads the
// target through leaderChMu/leaderCh, so a later ElectWait(ch) rebinds the
// target instead of registering a second callback (AddObserveCallFunc only
// appends, and two callbacks would double-deliver transitions).
func (w *ElectWorker) setLeaderChangeNotifyCall(ch chan<- bool) {
	// The registered flag is decided inside the same critical section that
	// sets the target: two concurrent ElectWait calls both rebind safely
	// and exactly one of them registers the callback (a torn
	// check-then-set would double-register and double-deliver).
	w.leaderChMu.Lock()
	w.leaderCh = ch
	register := false
	if !w.callbackRegistered {
		w.callbackRegistered = true
		register = true
	}
	w.leaderChMu.Unlock()
	if register {
		var callback election.LeaderChangeFunc = func(isLeader bool) {
			// send leader changes to the channel if the elector is not stopped
			//
			// F5c: the send is non-fatal. A plain `ch <- isLeader` parks
			// this dispatch goroutine forever once the consumer stalls
			// past the channel buffer, wedging every future dispatch.
			// Selecting on the elector context bounds the park:
			// cancellation frees the goroutine and the transition is
			// dropped, which is correct at shutdown.
			if w.stopped.Load() {
				return
			}
			w.leaderChMu.Lock()
			target := w.leaderCh
			w.leaderChMu.Unlock()
			if target == nil {
				return
			}
			select {
			case target <- isLeader:
			case <-w.ctx.Done():
			}
		}
		w.candidate.AddObserveCallFunc(callback)
	}
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

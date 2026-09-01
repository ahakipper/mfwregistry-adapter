package internal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"spotter/config"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/log"
	"spotter/pkg/metrics"
	"spotter/pkg/notice"
	"spotter/pkg/providers"
	consul2 "spotter/pkg/providers/consul"
	"spotter/pkg/providers/k8s"
	worker "spotter/pkg/worker"
	"spotter/tools"
)

// Distribute Core Server configuration providers provider
type Server struct {

	// When server stop need to call this funcs
	stopProviderFunc context.CancelFunc
	stopElectorFunc  context.CancelFunc
	stopOnce         sync.Once
	lifecycleMu      sync.Mutex

	startupGeneration uint64
	stopped           bool

	dialDiscovery       func(context.Context) (*discoverycenter.Client, error)
	waitRetry           func(context.Context, time.Duration) error
	initializeProviders func(context.Context, worker.Worker) ([]providers.Provider, error)
	lifecycleLocker     sync.Locker

	// providers providers
	Providers []providers.Provider

	elector worker.Elector

	// the channel of leader changes
	leaderChCh chan bool

	stop chan struct{}

	// current server election state
	isLeader bool

	// prometheus server
	promesvr *metrics.PrometheusService

	sync.Mutex
}

// Server Init
func NewServer() (*Server, error) {
	var err error
	// providers check
	if len(config.Providers) == 0 {
		err = errors.New("there is none providers configured")
		return nil, err
	} else {
		log.Logger.Infof("configured providers %v", config.Providers)
	}
	// new master elector
	ectx, ecancel := context.WithCancel(context.Background())
	// this channel must have a buffer, otherwise, the operation of leader change notification of the elector that sendting to
	// the channel may be blocked.
	leaderChanges := make(chan bool, 2048)
	var elector worker.Elector
	elector, err = worker.NewElector(ectx, leaderChanges)
	if err != nil {
		notice.Notice("Failed to initialize the elector", err.Error())
		return nil, err
	}
	// init Server
	return &Server{
		stopElectorFunc:     ecancel,
		stopProviderFunc:    nil,
		Providers:           nil,
		elector:             elector,
		leaderChCh:          leaderChanges,
		stop:                make(chan struct{}),
		promesvr:            metrics.NewPrometheusServer(config.MetricsAddr),
		dialDiscovery:       dialDiscoveryClient,
		waitRetry:           waitForRetry,
		initializeProviders: InitializeProviders,
	}, nil
}

// Run server
func (s *Server) Run() {
	if config.EnableLeaderElection {
		// start and process leader election
		log.Logger.Info("trying to become to master through election")
		go s.elector.ElectWait()
	} else {
		log.Logger.Warnf("the election is disabled, just make current process as fake leadere forever")
		s.leaderChCh <- true
	}
	// start prome and pprof http server
	go s.promesvr.Start()

	isBreak := false
	for {
		if isBreak {
			break
		}
		select {
		case isLeader := <-s.leaderChCh:
			s.Lock()
			if s.isLeader == isLeader || s.stopped {
				s.Unlock()
				continue
			}
			s.isLeader = isLeader
			s.Unlock()
			// if current leader is true, but changes to false, then stop the worker
			if !isLeader {
				log.Logger.Warn("i am losing the leader state")
				// Switch the leader,notice
				currentIP, err := notice.GetLocalIP()
				if err != nil {
					log.Logger.Errorf("get the current node IP error:%s", err.Error())
				}
				notice.Notice("Leader role lost", fmt.Sprintf("Current node: %s lost the Leader role. After stopping the work of the current server, it re-enters the election process", currentIP))
				// if the current node is not the leader, stop the work of the worker (the election work will continue)
				// s.stopWorkerFunc()
				s.stopProviders()
				continue
			}
			// if current leader is false, but changes to true, then start the worker again
			if isLeader {
				log.Logger.Info("i successfully competed for the leader")
				// if is leader again, create worker again
				go func() {
					err := s.stopAndStartProviders()
					if err != nil {
						// start provider failed,notice
						notice.Notice("Failed to start the provider", err.Error())
						log.Logger.Errorf("start provider error:%s", err)
					}
				}()
			}

			continue
		case <-s.stop:
			s.Lock()
			s.stopped = true
			s.isLeader = false
			s.Unlock()
			if s.stopElectorFunc != nil {
				s.stopElectorFunc()
			}
			_ = s.stopProviders()
			isBreak = true
		}
	}

	//

}

// Stop server and release resources
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		s.Lock()
		s.stopped = true
		s.isLeader = false
		cancel := s.stopProviderFunc
		s.Unlock()
		if cancel != nil {
			cancel()
		}
		if s.stop != nil {
			close(s.stop)
		}
	})
	log.Logger.Info("internal server stop background context")
}

func (s *Server) stopProviders() error {
	log.Logger.Infof("stop providers activly, the stopWorkerFunc will be called and providers will be clear")
	s.Lock()
	s.startupGeneration++
	cancel := s.stopProviderFunc
	s.stopProviderFunc = nil
	s.Providers = nil
	s.Unlock()
	if cancel != nil {
		tools.WithRecover(cancel)
	}
	return nil
}

func (s *Server) startProviders() error {
	wctx, wcancel := context.WithCancel(context.Background())
	s.Lock()
	if !s.isLeader || s.stopped {
		s.Unlock()
		wcancel()
		return context.Canceled
	}
	s.startupGeneration++
	generation := s.startupGeneration
	s.stopProviderFunc = wcancel
	s.Unlock()

	client, err := s.dialDiscoveryWithRetry(wctx)
	if err != nil {
		s.clearStartup(generation, wcancel)
		return errors.WithMessage(err, "dial discovery center")
	}
	registry, err := discoverycenter.NewDiscoveryCenter(client, log.Logger, noticeNotifier{}, config.DisablePushWorker)
	if err != nil {
		wcancel()
		_ = client.Close()
		s.clearStartup(generation, nil)
		return errors.WithMessage(err, "new discovery center")
	}
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			wcancel()
			if closeErr := registry.Close(); closeErr != nil {
				log.Logger.Errorf("close discovery center client: %s", closeErr)
			}
		})
	}

	w, err := worker.NewResourceWorker(wctx, registry, log.Logger, globalMetricsRecorder{})
	if err != nil {
		cleanup()
		s.clearStartup(generation, nil)
		return errors.WithMessage(err, "new resource worker")
	}
	initialize := s.initializeProviders
	if initialize == nil {
		initialize = InitializeProviders
	}
	prs, err := initialize(wctx, w)
	if err != nil {
		cleanup()
		s.clearStartup(generation, nil)
		return err
	}

	s.Lock()
	if s.startupGeneration != generation || !s.isLeader || s.stopped || wctx.Err() != nil {
		s.Unlock()
		cleanup()
		return context.Canceled
	}
	s.stopProviderFunc = cleanup
	s.Providers = prs
	s.Unlock()

	eg := errgroup.Group{}
	for _, provider := range prs {
		provider := provider
		eg.Go(func() error {
			defer tools.WithRecover(cleanup)
			return provider.Run()
		})
	}
	err = eg.Wait()
	cleanup()
	s.clearStartup(generation, nil)
	return err
}

const (
	discoveryDialAttempts = 3
	discoveryDialTimeout  = 5 * time.Second
	discoveryRetryDelay   = 5 * time.Second
)

func dialDiscoveryClient(ctx context.Context) (*discoverycenter.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, discoveryDialTimeout)
	defer cancel()
	return discoverycenter.Dial(dialCtx, config.GrpcAddr, log.Logger, globalMetricsRecorder{})
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *Server) dialDiscoveryWithRetry(ctx context.Context) (*discoverycenter.Client, error) {
	dial := s.dialDiscovery
	if dial == nil {
		dial = dialDiscoveryClient
	}
	wait := s.waitRetry
	if wait == nil {
		wait = waitForRetry
	}
	var err error
	for attempt := 0; attempt < discoveryDialAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		var client *discoverycenter.Client
		client, err = dial(ctx)
		if err == nil {
			return client, nil
		}
		log.Logger.Errorf("connect fail: %s", err)
		if attempt == discoveryDialAttempts-1 {
			break
		}
		if waitErr := wait(ctx, discoveryRetryDelay); waitErr != nil {
			return nil, waitErr
		}
	}
	return nil, err
}

func (s *Server) clearStartup(generation uint64, cancel context.CancelFunc) {
	s.Lock()
	if s.startupGeneration == generation {
		s.stopProviderFunc = nil
		s.Providers = nil
	}
	s.Unlock()
	if cancel != nil {
		cancel()
	}
}

type noticeNotifier struct{}

func (noticeNotifier) Notify(title, content string) {
	notice.Notice(title, content)
}

type globalMetricsRecorder struct{}

func (globalMetricsRecorder) ObserveSyncOnceDuration(duration time.Duration) {
	metrics.SyncOnceDurationsHistogram.Observe(float64(duration.Milliseconds()))
}

func (globalMetricsRecorder) ObserveSyncAllDuration(provider string, duration time.Duration) {
	switch provider {
	case providers.ProviderK8s:
		metrics.SyncAllK8sDurationsHistogram.Observe(float64(duration.Milliseconds()))
	case providers.ProviderEcs:
		metrics.SyncAllEcsDurationsHistogram.Observe(float64(duration.Milliseconds()))
	default:
		metrics.SyncAllDurationsHistogram.Observe(float64(duration.Milliseconds()))
	}
}

func (globalMetricsRecorder) SetSyncErrorQueueDepth(depth int) {
	metrics.SyncErrorGauge.WithLabelValues("sync_error_gauge").Set(float64(depth))
}

func (globalMetricsRecorder) MarkSyncOnce() {
	metrics.SyncOnceGauge.WithLabelValues("sync_once_gauge").Set(1)
}

// stopAndStartProviders will stop providers and then start new providers
// If an error occurs when stopping providers, ignore it.
func (s *Server) stopAndStartProviders() (err error) {
	locker := s.lifecycleLocker
	if locker == nil {
		locker = &s.lifecycleMu
	}
	locker.Lock()
	defer locker.Unlock()
	log.Logger.Info("stop and start providers begin..")
	defer log.Logger.Info("stop and start providers stopped..")
	if err = s.stopProviders(); err != nil {
		return err
	}
	if err = s.startProviders(); err != nil {
		err = errors.WithMessage(err, "start providers")
		log.Logger.Errorf(err.Error())
	}
	return err
}

func InitializeProviders(ctx context.Context, w worker.Worker) (prs []providers.Provider, err error) {
	if len(config.Providers) == 0 {
		err = errors.New("empty provider names for initializing")
		return nil, err
	}
	prs = []providers.Provider{}
	for _, pname := range config.Providers {
		switch pname {
		case providers.ProviderK8s:
			// create k8s provider and pass the worker to the providers
			var k8sProvider providers.Provider
			k8sProvider, err = k8s.NewK8SProvider(ctx, w, config.PushAllInterval, config.KubeConfigPath)
			if err != nil {
				err = errors.WithMessagef(err, "new k8s provider")
				return nil, err
			}
			prs = append(prs, k8sProvider)
		case providers.ProviderEcs:
			var consulProvider providers.Provider
			if len(config.ConsulAddress) == 0 {
				err = errors.New("the consul server address is not configured")
				return nil, err
			}
			consulProvider, err = consul2.NewConsulProvider(ctx, w, config.PushAllInterval, config.ConsulAddress)
			if err != nil {
				err = errors.WithMessagef(err, "new consul provider")
				return nil, err
			}
			prs = append(prs, consulProvider)
		default:
			err = errors.New(fmt.Sprintf("invalid provider name: %s", pname))
			return nil, err
		}
	}

	return prs, err
}

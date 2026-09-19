package internal

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"spotter/internal/composition"
	domaininstance "spotter/internal/domain/instance"
	infraconfig "spotter/internal/infra/config"
	inframetrics "spotter/internal/infra/metrics"
	"spotter/internal/ports"
	v2 "spotter/pkg/beehive/service/v2"
	"spotter/pkg/discoverycenter"
	"spotter/pkg/k8srobot"
	"spotter/pkg/nacos"
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

	// injected dependencies
	logger            ports.Logger
	notifier          ports.Notifier
	metrics           ports.MetricsRecorder
	nacosAdminFactory func() (nacos.NacosClusterAdmin, error)
	cfg               infraconfig.Config
	localIP           func() (string, error)

	dialDiscovery       func(context.Context) (*discoverycenter.Client, error)
	waitRetry           func(context.Context, time.Duration) error
	initializeProviders func(context.Context, worker.Worker) ([]providers.Provider, error)
	lifecycleLocker     sync.Locker

	// providers providers
	Providers []providers.Provider

	// elector coordinates the leader election. Typed as the port (not the
	// concrete worker.Elector) so tests inject fakes.FakeLeaderElector and
	// any ports.LeaderElector implementation works interchangeably.
	elector ports.LeaderElector

	// the channel of leader changes
	leaderChCh chan bool

	stop chan struct{}

	// current server election state
	isLeader bool

	// stopMetrics stops the Prometheus HTTP server started by Run; nil when
	// no metrics server was started.
	stopMetrics           func() error
	observeDebug          *observeEventRecorder
	observeDebugProviders int

	sync.Mutex
}

// NewServerFromDeps creates the server from the composition root output.
//
// rt carries the logger, notifier, metrics recorder, resolved config and
// local IP resolver injected by the composition root (internal/composition).
// The elector is still constructed here (etcd connection and campaign key
// come from the injected config) because it is part of the server startup,
// not of the offline object graph.
func NewServerFromDeps(rt *composition.Runtime) (*Server, error) {
	if rt == nil {
		return nil, errors.New("nil runtime")
	}
	var err error
	// providers check
	if len(rt.Config.Providers) == 0 {
		err = errors.New("there is none providers configured")
		return nil, err
	} else {
		rt.Logger.Infof("configured providers %v", rt.Config.Providers)
	}
	// new master elector
	ectx, ecancel := context.WithCancel(context.Background())
	// this channel must have a buffer, otherwise, the operation of leader change notification of the elector that sendting to
	// the channel may be blocked.
	leaderChanges := make(chan bool, 2048)
	var elector ports.LeaderElector
	elector, err = worker.NewElectorWithDeps(ectx, leaderChanges,
		rt.Config.EtcdEndpoints, rt.Config.CertFile, rt.Config.KeyFile, rt.Config.CAFile,
		rt.Config.LockCampaignKey, rt.Logger, rt.Notifier)
	if err != nil {
		ecancel()
		rt.Notifier.Notify("Failed to initialize the elector", err.Error())
		return nil, err
	}
	// init Server
	srv := &Server{
		stopElectorFunc:   ecancel,
		stopProviderFunc:  nil,
		Providers:         nil,
		elector:           elector,
		leaderChCh:        leaderChanges,
		stop:              make(chan struct{}),
		logger:            rt.Logger,
		notifier:          rt.Notifier,
		metrics:           rt.Metrics,
		nacosAdminFactory: rt.NacosClusterAdminFactory,
		cfg:               rt.Config,
		localIP:           rt.LocalIP,
		waitRetry:         waitForRetry,
	}
	// Resolve provider reconcile mode from the same effective config used by
	// sink construction. Capturing the server (rather than the original
	// runtime value) lets Nacos-only startup promote an omitted reconcile
	// source to Nacos without introducing a second configuration path.
	srv.initializeProviders = func(ctx context.Context, w worker.Worker) ([]providers.Provider, error) {
		return InitializeProvidersWithDeps(ctx, w, srv.cfg, srv.logger, srv.notifier)
	}
	return srv, nil
}

// Run server
func (s *Server) Run() {
	if s.cfg.EnableLeaderElection {
		// start and process leader election
		s.logger.Info("trying to become to master through election")
		// ElectWait binds the server's leader-change channel at call time
		// (the port signature), so the elector forwards transitions to
		// exactly this channel.
		go s.elector.ElectWait(s.leaderChCh)
	} else {
		s.logger.Warnf("the election is disabled, just make current process as fake leadere forever")
		s.leaderChCh <- true
	}
	// start prome and pprof http server
	s.startMetricsServer()

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
				s.logger.Warn("i am losing the leader state")
				// Switch the leader,notice
				currentIP, err := s.localIP()
				if err != nil {
					s.logger.Errorf("get the current node IP error:%s", err.Error())
				}
				s.notifier.Notify("Leader role lost", fmt.Sprintf("Current node: %s lost the Leader role. After stopping the work of the current server, it re-enters the election process", currentIP))
				// if the current node is not the leader, stop the work of the worker (the election work will continue)
				// s.stopWorkerFunc()
				s.stopProviders()
				continue
			}
			// if current leader is false, but changes to true, then start the worker again
			if isLeader {
				s.logger.Info("i successfully competed for the leader")
				// if is leader again, create worker again
				go func() {
					err := s.stopAndStartProviders()
					if err != nil {
						// start provider failed,notice
						s.notifier.Notify("Failed to start the provider", err.Error())
						s.logger.Errorf("start provider error:%s", err)
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
			s.stopMetricsServer()
			isBreak = true
		}
	}

	//

}

// startMetricsServer starts the Prometheus (and pprof) HTTP server on the
// configured metrics address. It mirrors the legacy promesvr.Start() call
// but uses the infra metrics HTTP server, which reports errors instead of
// panicking and supports a graceful stop.
func (s *Server) startMetricsServer() {
	s.registerObserveDebugEndpoint()
	stop, err := inframetrics.StartHTTP(s.cfg.MetricsAddr)
	if err != nil {
		s.logger.Errorf("metrics server start failed: %s", err)
		return
	}
	s.Lock()
	s.stopMetrics = stop
	s.Unlock()
}

// registerObserveDebugEndpoint exposes the provider's own converted K8s
// projection for the watch-based reliability harness. It is disabled by
// default and only registered in a process explicitly started with
// SPOTTER_OBSERVE_DEBUG=1, so production deployments do not expose instance
// metadata on the metrics listener.
func (s *Server) registerObserveDebugEndpoint() {
	if os.Getenv("SPOTTER_OBSERVE_DEBUG") != "1" {
		return
	}
	if s.observeDebug == nil {
		s.observeDebug = newObserveEventRecorder(observeEventCapacity)
	}
	registerObserveDebugHandlers(s)
}

func (s *Server) handleObserveDebugSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.Lock()
	providersSnapshot := append([]providers.Provider(nil), s.Providers...)
	s.Unlock()
	instances := make([]*v2.Instance, 0)
	canonical := make([]string, 0)
	cacheGeneration := uint64(0)
	for _, provider := range providersSnapshot {
		if provider == nil {
			continue
		}
		providerInstances := provider.GetAll()
		if cacheSnapshotter, ok := provider.(k8s.ObserveCacheSnapshotter); ok {
			var generation uint64
			providerInstances, generation = cacheSnapshotter.ObserveCacheSnapshot()
			if generation > cacheGeneration {
				cacheGeneration = generation
			}
		}
		for _, instance := range providerInstances {
			if instance != nil && instance.Provider == providers.ProviderK8s {
				instances = append(instances, instance)
				canonical = append(canonical, domaininstance.CanonicalPayload(instance))
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	s.Lock()
	observerProviders := s.observeDebugProviders
	s.Unlock()
	latestSequence := uint64(0)
	if s.observeDebug != nil {
		latestSequence = s.observeDebug.latestSequence()
	}
	_ = json.NewEncoder(w).Encode(struct {
		ObservedAt          string         `json:"observedAt"`
		Instances           []*v2.Instance `json:"instances"`
		CanonicalPayload    []string       `json:"canonicalPayload"`
		ObserverProviders   int            `json:"observerProviders"`
		LatestEventSequence uint64         `json:"latestEventSequence"`
		CacheGeneration     uint64         `json:"cacheGeneration"`
	}{ObservedAt: time.Now().UTC().Format(time.RFC3339Nano), Instances: instances, CanonicalPayload: canonical, ObserverProviders: observerProviders, LatestEventSequence: latestSequence, CacheGeneration: cacheGeneration})
}

func (s *Server) stopMetricsServer() {
	s.Lock()
	stop := s.stopMetrics
	s.stopMetrics = nil
	s.Unlock()
	if stop != nil {
		if err := stop(); err != nil {
			s.logger.Errorf("metrics server stop failed: %s", err)
		}
	}
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
		if closer, ok := s.notifier.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && s.logger != nil {
				s.logger.Errorf("close notifier: %s", err)
			}
		}
	})
	s.logger.Info("internal server stop background context")
}

func (s *Server) stopProviders() error {
	s.logger.Infof("stop providers activly, the stopWorkerFunc will be called and providers will be clear")
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

	// Nacos is the active service-discovery sink whenever it is configured.
	// The Atlas client is constructed only for the explicit compatibility path.
	// This boundary is intentionally before any Atlas dial so a Nacos-only
	// process cannot fail because an excluded Atlas endpoint is unavailable.
	nacosConfigured := s.cfg.NacosAddr != "" || len(s.cfg.NacosServerList) > 0
	useAtlas := s.cfg.EnableAtlasCompatibility
	if !nacosConfigured && !useAtlas {
		s.clearStartup(generation, wcancel)
		return errors.New("no active discovery sink: configure Nacos or explicitly enable Atlas compatibility")
	}
	var err error
	var client *discoverycenter.Client
	var registry *discoverycenter.DiscoveryCenter
	if useAtlas {
		client, err = s.dialDiscoveryWithRetry(wctx)
		if err != nil {
			s.clearStartup(generation, wcancel)
			return errors.WithMessage(err, "dial discovery center")
		}
		registry, err = discoverycenter.NewDiscoveryCenter(client, s.logger, s.notifier, s.cfg.DisablePushWorker)
		if err != nil {
			wcancel()
			_ = client.Close()
			s.clearStartup(generation, nil)
			return errors.WithMessage(err, "new discovery center")
		}
	}
	var cleanupOnce sync.Once
	var fanout *worker.FanoutSink
	var nacosSink *nacos.Sink
	cleanup := func() {
		cleanupOnce.Do(func() {
			wcancel()
			if fanout != nil {
				if closeErr := fanout.Close(); closeErr != nil {
					s.logger.Errorf("close sink fanout: %s", closeErr)
				}
				return
			}
			if nacosSink != nil {
				if closeErr := nacosSink.Close(); closeErr != nil {
					s.logger.Errorf("close nacos sink: %s", closeErr)
				}
			}
			if registry != nil {
				if closeErr := registry.Close(); closeErr != nil {
					s.logger.Errorf("close discovery center client: %s", closeErr)
				}
			} else if client != nil {
				if closeErr := client.Close(); closeErr != nil {
					s.logger.Errorf("close discovery center client: %s", closeErr)
				}
			}
		})
	}

	// The worker still uses the named fan-out abstraction for retry and full
	// reconcile operations. In the active graph this is normally a one-sink
	// fan-out containing only Nacos; Atlas is appended only when the explicit
	// compatibility switch is enabled.
	sinks := make([]worker.NamedSink, 0, 2)
	if registry != nil {
		sinks = append(sinks, worker.NamedSink{Name: worker.AtlasSinkName, Sink: registry})
	}
	if s.cfg.NacosAddr != "" || len(s.cfg.NacosServerList) > 0 {
		transportMode := s.cfg.NacosTransport
		if transportMode == "" {
			transportMode = string(nacos.TransportSDK)
		}
		if transportMode != string(nacos.TransportSDK) && transportMode != string(nacos.TransportHTTPCompat) {
			cleanup()
			s.clearStartup(generation, nil)
			return fmt.Errorf("unsupported Nacos transport %q (want %q or %q)", transportMode, nacos.TransportSDK, nacos.TransportHTTPCompat)
		}
		if transportMode == string(nacos.TransportHTTPCompat) && strings.EqualFold(s.cfg.Env, "product") {
			cleanup()
			s.clearStartup(generation, nil)
			return fmt.Errorf("Nacos http-compat transport is forbidden in product; use the official SDK transport")
		}
		if transportMode == string(nacos.TransportHTTPCompat) {
			address := s.cfg.NacosAddr
			if address == "" && len(s.cfg.NacosServerList) > 0 {
				address = strings.Join(s.cfg.NacosServerList, ",")
			}
			message := fmt.Sprintf("NON_PRODUCTION_COMPAT: Nacos http-compat transport is enabled for %s; use transport=sdk for production", address)
			s.logger.Warnf("%s", message)
			if s.notifier != nil {
				s.notifier.Notify("Nacos compatibility transport enabled", message)
			}
		}
		effectiveHealthPolicy := s.cfg.NacosHealthPolicy
		if effectiveHealthPolicy == "" {
			effectiveHealthPolicy = nacos.HealthPolicyDeploymentOwned
		}

		var clusterAdmin nacos.NacosClusterAdmin
		if transportMode == string(nacos.TransportSDK) && effectiveHealthPolicy == nacos.HealthPolicyAdminManaged && s.nacosAdminFactory != nil {
			clusterAdmin, err = s.nacosAdminFactory()
			if err != nil {
				if clusterAdmin != nil {
					_ = closeNacosAdmin(clusterAdmin, 0)
				}
				cleanup()
				s.clearStartup(generation, nil)
				return errors.WithMessage(err, "new nacos cluster-admin facade")
			}
			if clusterAdmin == nil {
				cleanup()
				s.clearStartup(generation, nil)
				return errors.New("new nacos cluster-admin facade: factory returned nil admin")
			}
		}
		nacosCfg := nacos.ClientConfig{TransportMode: nacos.TransportMode(transportMode), HealthPolicy: effectiveHealthPolicy, ServerURL: s.cfg.NacosAddr, ServerURLs: s.cfg.NacosServerList, NamespaceID: s.cfg.NacosNamespace, GroupName: s.cfg.NacosGroup, Username: s.cfg.NacosUsername, Password: s.cfg.NacosPassword, AccessToken: s.cfg.NacosAccessToken, CAFile: s.cfg.NacosCAFile, ServerName: s.cfg.NacosServerName, InsecureSkipVerify: s.cfg.NacosInsecureSkipVerify}
		nacosCfg.ClusterAdmin = clusterAdmin
		if s.cfg.NacosTimeout > 0 {
			nacosCfg.Timeout = time.Duration(s.cfg.NacosTimeout) * time.Second
		}
		nacosSink, err = nacos.NewSinkWithConfig(nacosCfg, s.logger)
		if err != nil {
			if clusterAdmin != nil {
				_ = closeNacosAdmin(clusterAdmin, nacosCfg.Timeout)
			}
			cleanup()
			s.clearStartup(generation, nil)
			return errors.WithMessage(err, "new nacos sink")
		}
		readinessCfg := nacosCfg
		// Readiness owns a temporary naming client; the long-lived admin
		// belongs to nacosSink and must not be closed by the probe client.
		readinessCfg.ClusterAdmin = nil
		if err := nacos.CheckReadinessWithConfig(readinessCfg, s.logger); err != nil {
			if closeErr := nacosSink.Close(); closeErr != nil {
				err = stderrors.Join(err, closeErr)
			}
			nacosSink = nil
			cleanup()
			s.clearStartup(generation, nil)
			return errors.WithMessage(err, "nacos readiness check")
		}
		// The Nacos sink owns the official SDK naming client and closes its
		// gRPC/redo resources with the fanout. In Nacos-only mode this is the
		// sole registered sink, so no Atlas connection exists to close.
		sinks = append(sinks, worker.NamedSink{Name: nacos.SinkName, Sink: nacosSink})
		address := s.cfg.NacosAddr
		if address == "" && len(s.cfg.NacosServerList) > 0 {
			address = strings.Join(s.cfg.NacosServerList, ",")
		}
		group := s.cfg.NacosGroup
		if group == "" {
			group = nacos.DefaultGroup
		}
		s.logger.Infof("nacos sink registered at %s (persistent instances, group %s)", address, group)
	}
	// NewFanoutSinkWithMetrics (not NewFanoutSink): the per-sink e2e
	// decorator of dsca-2 §6 row 7 observes on the real recorder, so every
	// Push/PushAll/PushTo (including every 5s retry) produces one
	// event_to_store_e2e_duration_seconds observation in production.
	fanout, err = worker.NewFanoutSinkWithMetrics(s.logger, s.metrics, sinks...)
	if err != nil {
		cleanup()
		s.clearStartup(generation, nil)
		return errors.WithMessage(err, "new fanout sink")
	}

	// The reconcile-source designation (dsca-3 §3.1) follows the active sink
	// graph. Nacos-only mode defaults to the Nacos catalog even when the flag
	// is omitted; an Atlas-compatible graph keeps its historical primary view
	// unless the operator explicitly selects another registered sink.
	source := s.cfg.ReconcileSource
	if source == "" && nacosConfigured && !useAtlas {
		source = nacos.SinkName
	}
	if source != "" {
		if source == nacos.SinkName && nacosSink == nil {
			cleanup()
			s.clearStartup(generation, nil)
			return errors.New("--reconcile-source nacos requires --nacos-addr: the nacos sink is not registered")
		}
		if err := fanout.SetReconcileSource(source); err != nil {
			cleanup()
			s.clearStartup(generation, nil)
			return errors.WithMessage(err, "designate reconcile source")
		}
		s.logger.Infof("reconcile source designated: %s (the periodic compare reads this sink's view)", source)
	}

	w, err := worker.NewResourceWorker(wctx, fanout, s.logger, s.metrics)
	if err != nil {
		cleanup()
		s.clearStartup(generation, nil)
		return errors.WithMessage(err, "new resource worker")
	}

	// The queue-full drop observer (dsca-1 DS-1-1 fix item 1 / dsca-2 §6
	// row 8, the unified drop spec): k8srobot keeps no metrics dependency,
	// the wiring closes over the recorder. Set BEFORE the providers run
	// (the observer's contract: set once, before Run — the providers below
	// start the robots). The cluster label value is the watcher's kubeconfig
	// path. Covers every k8s provider of this server, including the demo
	// binary's (the spotter command reaches startProviders through
	// NewServerFromDeps).
	k8srobot.SetDropObserver(func(cluster string) {
		s.metrics.IncEventsDropped(cluster)
	})

	initialize := s.initializeProviders
	if initialize == nil {
		initialize = initializeProvidersFromConfig(s.cfg, s.logger, s.notifier)
	}
	prs, err := initialize(wctx, w)
	if err != nil {
		cleanup()
		s.clearStartup(generation, nil)
		return err
	}

	// The queueDepth gauge wiring (dsca-1 DS-1-1 fix item 1, the second
	// observable of the same item): the k8s provider publishes the robot's
	// coalescing-queue depth on k8s_queue_depth from its own 5s ticker
	// (SetQueueDepthReporter + the robot's read-only QueueDepth — the
	// minimal plumbing that keeps the provider constructor's shared signature
	// unchanged). The recorder is the same one the drop observer closes
	// over, so both series of the queue's health land on one recorder.
	for _, provider := range prs {
		if observer, ok := provider.(k8s.InstanceEventObserver); ok && s.observeDebug != nil {
			observer.SetInstanceEventObserver(func(observation k8s.InstanceEventObservation) {
				s.observeDebug.record(observation.TriggerTime, string(observation.Operate), observation.Origin, observation.Instance)
			})
			s.Lock()
			s.observeDebugProviders++
			s.Unlock()
		}
		if reporter, ok := provider.(k8s.QueueDepthReporter); ok {
			reporter.SetQueueDepthReporter(s.metrics)
		}
		if reporter, ok := provider.(k8s.MetricsReporter); ok {
			reporter.SetMetricsRecorder(s.metrics)
		}
		if reporter, ok := provider.(consul2.MetricsReporter); ok {
			reporter.SetMetricsRecorder(s.metrics)
		}
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

func (s *Server) dialDiscoveryClient(ctx context.Context) (*discoverycenter.Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, discoveryDialTimeout)
	defer cancel()
	return discoverycenter.Dial(dialCtx, s.cfg.GrpcAddr, s.logger, s.metrics)
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
		dial = s.dialDiscoveryClient
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
		s.logger.Errorf("connect fail: %s", err)
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

func closeNacosAdmin(admin nacos.NacosClusterAdmin, timeout time.Duration) error {
	if admin == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return admin.Close(ctx)
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
	s.logger.Info("stop and start providers begin..")
	defer s.logger.Info("stop and start providers stopped..")
	if err = s.stopProviders(); err != nil {
		return err
	}
	if err = s.startProviders(); err != nil {
		err = errors.WithMessage(err, "start providers")
		s.logger.Errorf(err.Error())
	}
	return err
}

// initializeProvidersFromConfig binds InitializeProviders to a resolved
// config, preserving the legacy InitializeProviders seam with injected
// configuration.
func initializeProvidersFromConfig(cfg infraconfig.Config, logger ports.Logger, notifier ports.Notifier) func(context.Context, worker.Worker) ([]providers.Provider, error) {
	return func(ctx context.Context, w worker.Worker) ([]providers.Provider, error) {
		return InitializeProvidersWithDeps(ctx, w, cfg, logger, notifier)
	}
}

// InitializeProviders creates the providers declared by cfg. It is kept as
// a package-level function (with the config passed in) so it remains a
// testable seam.
func InitializeProviders(ctx context.Context, w worker.Worker, cfg infraconfig.Config) (prs []providers.Provider, err error) {
	return initializeProvidersWithDeps(ctx, w, cfg, nil, nil)
}

// InitializeProvidersWithDeps is the production composition seam. Provider
// logging and notices are explicit ports; the legacy InitializeProviders
// wrapper remains for callers/tests that still exercise the historical global
// configuration path.
func InitializeProvidersWithDeps(ctx context.Context, w worker.Worker, cfg infraconfig.Config, logger ports.Logger, notifier ports.Notifier) (prs []providers.Provider, err error) {
	return initializeProvidersWithDeps(ctx, w, cfg, logger, notifier)
}

func initializeProvidersWithDeps(ctx context.Context, w worker.Worker, cfg infraconfig.Config, logger ports.Logger, notifier ports.Notifier) (prs []providers.Provider, err error) {
	if len(cfg.Providers) == 0 {
		err = errors.New("empty provider names for initializing")
		return nil, err
	}
	// The nacos-reconcile mode the providers' compares run in (dsca-3
	// §3.3): true exactly when the reconcile source designates the nacos
	// sink — including the implicit source used by the Nacos-only graph. The
	// sink wiring and provider diff semantics therefore agree even when the
	// operator omits --reconcile-source.
	nacosConfigured := cfg.NacosAddr != "" || len(cfg.NacosServerList) > 0
	reconcileNacos := cfg.ReconcileSource == nacos.SinkName ||
		(cfg.ReconcileSource == "" && nacosConfigured && !cfg.EnableAtlasCompatibility)
	prs = []providers.Provider{}
	for _, pname := range cfg.Providers {
		switch pname {
		case providers.ProviderK8s:
			// create k8s provider and pass the worker to the providers
			var k8sProvider providers.Provider
			k8sProvider, err = k8s.NewK8SProviderWithDeps(ctx, w, cfg.PushAllInterval, cfg.KubeConfigPath, logger, notifier, cfg.PushAppCodes)
			if err != nil {
				err = errors.WithMessagef(err, "new k8s provider")
				return nil, err
			}
			if reconcileNacos {
				if sw, ok := k8sProvider.(k8s.NacosReconcileSwitch); ok {
					sw.SetNacosReconcileSource(true)
				}
			}
			prs = append(prs, k8sProvider)
		case providers.ProviderEcs:
			var consulProvider providers.Provider
			if len(cfg.ConsulAddress) == 0 {
				err = errors.New("the consul server address is not configured")
				return nil, err
			}
			consulProvider, err = consul2.NewConsulProviderWithDeps(ctx, w, cfg.PushAllInterval, cfg.ConsulAddress, logger, notifier)
			if err != nil {
				err = errors.WithMessagef(err, "new consul provider")
				return nil, err
			}
			if reconcileNacos {
				if sw, ok := consulProvider.(consul2.NacosReconcileSwitch); ok {
					sw.SetNacosReconcileSource(true)
				}
			}
			prs = append(prs, consulProvider)
		default:
			err = errors.New(fmt.Sprintf("invalid provider name: %s", pname))
			return nil, err
		}
	}

	return prs, err
}

package internel

import (
    "context"
    "fmt"
    "github.com/pkg/errors"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/metrics"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers"
    consul2 "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers/consul"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers/k8s"
    worker "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/worker"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/tools"
    "golang.org/x/sync/errgroup"
    "sync"
)

// Distribute Core Server configuration providers provider
type Server struct {

    // When server stop need to call this funcs
    stopProviderFunc context.CancelFunc
    stopElectorFunc  context.CancelFunc

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
        return nil, err
    }
    // init Server
    return &Server{
        stopElectorFunc:  ecancel,
        stopProviderFunc: nil,
        Providers:        nil,
        elector:          elector,
        leaderChCh:       leaderChanges,
        stop:             make(chan struct{}),
        promesvr:         metrics.NewPrometheusServer(),
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
            // if the leader status is set repeatedly, ignore the change
            if s.isLeader == isLeader {
                continue
            }
            // if current leader is true, but changes to false, then stop the worker
            if !isLeader && isLeader != s.isLeader {
                log.Logger.Warn("i am losing the leader state")
                s.isLeader = isLeader
                // if the current node is not the leader, stop the work of the worker (the election work will continue)
                // s.stopWorkerFunc()
                s.stopProviders()
                continue
            }
            // if current leader is false, but changes to true, then start the worker again
            if !s.isLeader && isLeader != s.isLeader {
                log.Logger.Info("i successfully competed for the leader")
                // set current sate
                s.isLeader = isLeader
                // if is leader again, create worker again
                go s.stopAndStartProviders()
            }
            continue
        case <-s.stop:
            // stop background context
            s.stopElectorFunc()
            s.stopProviderFunc()
            // break the loop
            isBreak = true
        }
    }

    //

}

// Stop server and release resources
func (s *Server) Stop() {
    s.stop <- struct{}{}
    log.Logger.Info("internel server stop background context")
}

func (s *Server) stopProviders() (err error) {
    // stop the previous worker
    log.Logger.Infof("stop providers activly, the stopWorkerFunc will be called and providers will be clear ")
    if s.stopProviderFunc != nil {
        // Use a protection mechanism to execute stopProviderFunc
        tools.WithRecover(s.stopProviderFunc)
        s.stopProviderFunc = nil
    }
    s.Providers = nil

    return err
}

func (s *Server) startProviders() (err error) {
    // start new worker
    // init cancel context
    wctx, wcancel := context.WithCancel(context.Background())
    s.stopProviderFunc = wcancel
    // new error group
    eg := errgroup.Group{}
    // create and start the worker
    w := worker.NewResourceWorker(wctx)

    prs := []providers.Provider{}
    if prs, err = InitializeProviders(wctx, w); err != nil {
        return err
    }
    // assign providers
    s.Providers = prs

    // start and wait
    for _, p := range s.Providers {
        p := p
        eg.Go(func() error {
            // 注意：我们期望是，一个 providers 退出，则另外其他 providers 都退出
            defer tools.WithRecover(s.stopProviderFunc)
            return p.Run()
        })

    }
    // wait to stop
    err = eg.Wait()

    return err
}

// stopAndStartProviders will stop providers and then start new providers
// If an error occurs when stopping providers, ignore it.
func (s *Server) stopAndStartProviders() (err error) {
    log.Logger.Info("stop and start providers begin..")
    s.Lock()
    defer func() {
        s.Unlock()
        log.Logger.Info("stop and start providers stopped..")
    }()
    // stop the previous providers
    if err = s.stopProviders(); err != nil {
        // do nothing
    }
    // start new providers
    if err = s.startProviders(); err != nil {
        err = errors.WithMessage(err, "stop and start providers")
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

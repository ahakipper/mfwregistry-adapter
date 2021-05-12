package core

import (
    "context"
    "fmt"
    "github.com/pkg/errors"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/metrics"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers"
    consul2 "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers/consul"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers/k8s"
    worker "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
    "golang.org/x/sync/errgroup"
    "sync"
)

// Distribute Core Server configuration providers provider
type Server struct {

    // When server stop need to call this funcs
    stopWorkerFunc  context.CancelFunc
    stopElectorFunc context.CancelFunc

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
    // init cancel context
    wctx, wcancel := context.WithCancel(context.Background())
    // create worker
    w := worker.NewResourceWorker(wctx)
    var prs []providers.Provider
    if prs, err = InitializeProviders(wctx, w); err != nil {
        err = errors.WithMessagef(err, "new server")
        return nil, err
    }
    return &Server{
        stopElectorFunc: ecancel,
        stopWorkerFunc:  wcancel,
        Providers:       prs,
        elector:         elector,
        leaderChCh:      leaderChanges,
        stop:            make(chan struct{}),
        promesvr:        metrics.NewPrometheusServer(),
    }, nil
}

// Run server
func (s *Server) Run() {
    // start and process leader election
    log.Logger.Info("trying to become to master through election")
    go s.elector.ElectWait()
    // start prome and pprof http server
    go s.promesvr.Start()

    for {
        breaked := false
        if breaked {
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
                log.Logger.Warn("i am lossing the leader state")
                s.isLeader = isLeader
                // if the current node is not the leader, stop the work of the worker (the election work will continue)
                s.stopWorkerFunc()
                continue
            }
            // if current leader is false, but changes to true, then start the worker agian
            if !s.isLeader && isLeader != s.isLeader {
                log.Logger.Info("i successfully competed for the leader")
                // set current sate
                s.isLeader = isLeader
                // if is leader agin, create worker again
                go s.stopAndStartWroker()
            }
            continue
        case <-s.stop:
            // stop background context
            s.stopElectorFunc()
            s.stopWorkerFunc()
            // break the loop
            breaked = true
        }
    }

    //

}

// Stop server and release resources
func (s *Server) Stop() {
    s.stop <- struct{}{}
    log.Logger.Info("core server stop background context")
}

func (s *Server) stopAndStartWroker() (err error) {
    s.Lock()
    defer s.Unlock()
    log.Logger.Info("stop and create worker")

    // providers check
    if len(config.Providers) == 0 {
        err = errors.New("there is none providers configured")
        panic(err)
    } else {
        log.Logger.Infof("configured providers %v", config.Providers)
    }

    // stop the previous worker
    if s.stopWorkerFunc != nil {
        s.stopWorkerFunc()
        s.stopWorkerFunc = nil
    }
    s.Providers = nil

    // init cancel context
    wctx, wcancel := context.WithCancel(context.Background())
    s.stopWorkerFunc = wcancel
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
        eg.Go(func() error {
            return p.Run()
        })

    }
    err = eg.Wait()

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
            consulProvider, err = consul2.NewConsulProvider(ctx, w, config.PushAllInterval, config.KubeConfigPath)
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

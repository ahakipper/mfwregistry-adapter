package core

import (
    "context"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/resource"
    worker "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
    "sync"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/metrics"
)

// Distribute Core Server configuration resource provider
type Server struct {

    // When server stop need to call this funcs
    stopWorkerFunc  context.CancelFunc
    stopElectorFunc context.CancelFunc

    // k8s resource provider
    K8sProvider resource.K8SProvider

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

    // new master elector
    ectx, ecancel := context.WithCancel(context.Background())
    // this channel must have a buffer, otherwise, the operation of leader change notification of the elector that sendting to
    // the channel may be blocked.
    leaderChanges := make(chan bool, 2048)
    elector, err := worker.NewElector(ectx, leaderChanges)

    if err != nil {
        return nil, err
    }
    // init cancel context
    wctx, wcancel := context.WithCancel(context.Background())
    // create worker
    w := worker.NewResourceWorker(wctx)
    // pass the worker to the providers
    k8sProvider, err := resource.NewK8SProvider(wctx, w, config.PushAllInterval, config.KubeConfigPath)
    if err != nil {
        return nil, err
    }
    return &Server{
        stopElectorFunc: ecancel,
        stopWorkerFunc:  wcancel,
        K8sProvider:     k8sProvider,
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

    // stop the previous worker
    if s.stopWorkerFunc != nil {
        s.stopWorkerFunc()
        s.stopWorkerFunc = nil
    }
    s.K8sProvider = nil

    // init cancel context
    wctx, wcancel := context.WithCancel(context.Background())

    // create and start the worker
    w := worker.NewResourceWorker(wctx)
    var k8sProvider resource.K8SProvider
    if k8sProvider, err = resource.NewK8SProvider(wctx, w, config.PushAllInterval, config.KubeConfigPath); err != nil {
        return err
    }
    //
    s.stopWorkerFunc = wcancel
    s.K8sProvider = k8sProvider
    // start
    k8sProvider.Start()

    return
}

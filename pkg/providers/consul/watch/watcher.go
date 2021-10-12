package watch

import (
    "context"
    "github.com/hashicorp/consul/api"
    consulwatch "github.com/hashicorp/consul/api/watch"
    "github.com/pkg/errors"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "golang.org/x/sync/errgroup"
)

type Watcher struct {
    client       *api.Client             // Consul api client
    address      string                  // Consul address
    watchPlans   []WatchPlan             // Consul watch plans
    eventHandler consulwatch.HandlerFunc // Handler that will be called when watch events triggered
}

type WatchType string
type WatchPlan func() (*consulwatch.Plan, error)

const (
    TypeServices = "services"
    TypeNodes    = "nodes"
    TypeChecks   = "checks"
)

// WatchTypes is the consul watch types.
// Set the watch type as a global variable to facilitate testing
var WatchTypes = []WatchType{TypeServices, TypeNodes, TypeChecks}
var watchPlans []*consulwatch.Plan

func NewWatcher(address string, handler consulwatch.HandlerFunc) (w *Watcher, err error) {
    if address == "" || handler == nil {
        err = errors.New("none consul client or handler")
        return nil, err
    }
    w = &Watcher{
        address:      address,
        eventHandler: handler,
    }
    w.constructWatchPlan()

    return w, nil
}

func (w *Watcher) constructWatchPlan() {
    wps := []WatchPlan{}
    if len(WatchTypes) > 0 {
        for _, wt := range WatchTypes {
            switch wt {
            case TypeServices:
                wps = append(wps, w.buildConsulServicePlan)
            case TypeNodes:
                wps = append(wps, w.buildConsulNodesPlan)
            case TypeChecks:
                wps = append(wps, w.buildConsulChecksPlan)
            }
        }
    }
    w.watchPlans = wps
}

func (w *Watcher) Watch(ctx context.Context) (err error) {
    // init consul build plans
    plans := w.watchPlans
    if len(plans) == 0 {
        err = errors.New("watch none consul plans")
        return err
    }
    // start watch
    planStopCh := make(chan struct{})
    var eg errgroup.Group
    for _, plan := range plans {
        var perr error
        var watchplan *consulwatch.Plan
        watchplan, perr = plan()
        if perr != nil {
            perr = errors.WithMessage(perr, "build consul plan failed")
            err = perr
            log.Logger.Error(perr.Error())
            break
        }
        eg.Go(func() error {
            go func() {
                select {
                case <-ctx.Done():
                    watchplan.Stop()
                    log.Logger.Info(ctx.Err().Error())
                case <-planStopCh:
                    log.Logger.Info("stop watching consul")
                }
            }()
            // watchplan Run will hang until it exits
            if perr = watchplan.Run(w.address); perr != nil {
                perr = errors.WithMessage(perr, "watch consul failed")
                log.Logger.Error(perr.Error())
            }
            close(planStopCh)
            return err
        })
    }
    err = eg.Wait()

    return err
}

func (w *Watcher) buildConsulNodesPlan() (*consulwatch.Plan, error) {
    watchConfig := map[string]interface{}{
        "type": "nodes",
    }
    plan, err := consulwatch.Parse(watchConfig)
    if err != nil {
        return nil, err
    }
    plan.Handler = w.eventHandler
    return plan, nil
}

func (w *Watcher) buildConsulChecksPlan() (*consulwatch.Plan, error) {
    watchConfig := map[string]interface{}{
        "type": "checks",
    }
    plan, err := consulwatch.Parse(watchConfig)
    if err != nil {
        return nil, err
    }
    plan.Handler = w.eventHandler
    return plan, nil
}

func (w *Watcher) buildConsulServicePlan() (*consulwatch.Plan, error) {
    watchConfig := map[string]interface{}{
        "type":    "service",
        "service": "redis",
    }
    plan, err := consulwatch.Parse(watchConfig)
    if err != nil {
        return nil, err
    }
    plan.Handler = w.eventHandler
    return plan, nil
}

package consul

import (
    "context"
    "github.com/hashicorp/consul/api"
    "github.com/panjf2000/ants/v2"
    "github.com/pkg/errors"
    "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/metrics"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/unit"
    "sync"
    "time"
)

// K8S provider implement
type consul struct {
    clientFactory ConsulClientFactory        // consul api factory
    monitor       Monitor                    // monitor
    ctx           context.Context            // context
    worker        worker.Worker              // the role of worker is to synchronize provider changes to the discovery center
    stopped       bool                       // whether the current provider has stopped
    sync.Mutex                               // lock mutex
    interval      int                        // the time interval for full synchronization. default 7200s(2h)
    filters       []providers.InstanceFilter // filters is a collection of functions used to filter invalid instances
    cache         providers.CacheIterface    // consul endpoint cache
    pool          *ants.Pool                 // goroutine pool
    initDone      bool
}

// NewConsulProvider creates consul provider
func NewConsulProvider(ctx context.Context, worker worker.Worker, pushInterval int, addrs []string) (provider providers.Provider, err error) {
    if ctx == nil || len(addrs) == 0 || worker == nil {
        err = errors.New("params invalid")
        return nil, err
    }
    var cf ConsulClientFactory
    if cf, err = NeweClientFacotorySimple(addrs); err != nil {
        return nil, err
    }
    var monitor Monitor
    if monitor, err = NewConsulMonitor(cf); err != nil {
        return nil, err
    }
    consulProvider := &consul{
        ctx:           ctx,
        monitor:       monitor,
        worker:        worker,
        clientFactory: cf,
        cache:         providers.NewCache(8),
    }
    // Create pool for sending instance events to MfwRegistry
    p, _ := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime))
    consulProvider.pool = p
    // Init instance fiter
    consulProvider.filters = providers.InitInstanceFilters()

    // Watch the change events to refresh local caches
    // monitor.AppendServiceHandler(provider.ServiceChanged)
    monitor.AppendInstanceHandler(consulProvider.InstanceChanged)

    //return &controller, err
    return consulProvider, nil
}

func (c *consul) Run() (err error) {
    log.Logger.Infof("start to run ecs provider")
    // Perform full instances synchronization periodically
    go c.ProcessIntervalFullPush()
    // Perform instances comparison for single synchronization one by one. Note: this operation will only be executed once.
    go c.CompareAndFlush()
    // monitor will hang
    err = c.monitor.Start(c.ctx)
    if err != nil {
        err = errors.WithMessage(err, "consul provider stopped")
        log.Logger.Errorf(err.Error())
    }
    log.Logger.Info("consul providers worker stopped")

    return err
}

// syncInstance will sync consul endpoints to MfwRegistry.
// It will get all the services tagged as "microservice", and convert consul endpoint model to MfwRegistry Model.
// Then compare the new instances list with the instances we cached before so that we can generate the instances which are
// added、updated and deleted.
func (c *consul) syncInstance() (err error) {
    c.Lock()
    defer c.Unlock()
    oldCache := c.cache
    newCache := providers.NewCache(8)
    // Init cache
    c.cache = providers.NewCache(8)
    // Get all services from consul
    currentInss := c.GetAll()
    // Here, we assume that the consul data is impossible to be empty. Once it is empty,
    // no operation is performed except for return an error.
    if currentInss == nil || len(currentInss) == 0 {
        err = errors.New("get empty consul endpoints")
        log.Logger.Warnf(err.Error())
        return err
    }
    for _, ins := range currentInss {
        newCache.ReplaceOrInsert(ins)
    }
    // Compare to generate events
    addEvents, updateEvents, deleteEvents := c.extractDiff(oldCache, newCache)
    //pp.Println(map[string][]*sv.Instance{"add": addEvents, "update": updateEvents, "delete": deleteEvents})
    // push events
    c.EventsSync(addEvents, updateEvents, deleteEvents)
    // Update cache
    c.cache = newCache

    return err
}

func (c *consul) toInstance(endpoints []*api.ServiceEntry) (inss []*sv.Instance) {
    // get pod info from k8s robot
    inss = []*sv.Instance{}
    if len(endpoints) > 0 {
        for _, ep := range endpoints {
            if ins, err := convertInstance(ep); err != nil {
                log.Logger.Errorf(err.Error())
                continue
            } else {
                inss = append(inss, ins)
            }
        }
    }

    return inss
}

func (c *consul) GetAll() (result []*v2.Instance) {
    // Get all services from consul
    var err error
    var consulServices map[string][]string
    consulServices, err = c.monitor.GetServices()
    if err != nil {
        err = errors.WithMessage(err, "get services from consul")
        log.Logger.Errorf(err.Error())
        return nil
    }
    // Process new cache
    if len(consulServices) > 0 {
        result = []*sv.Instance{}
        for serviceName := range consulServices {
            // get endpoints of a service from consul
            var endpoints []*api.ServiceEntry
            endpoints, err = c.monitor.GetServiceEntries(serviceName, nil)
            if err != nil {
                err = errors.WithMessage(err, "get service endpoints from consul")
                log.Logger.Errorf(err.Error())
                return nil
            }
            if instances := c.toInstance(endpoints); instances != nil {
                for _, ins := range instances {
                    result = append(result, ins)
                }
            }
        }
    }
    log.Logger.Infof("consul getall size: %d", len(result))

    return result
}

func (c *consul) InstanceChanged(instance *api.CatalogService) (err error) {
    err = c.syncInstance()
    return err
}

func (c *consul) ServiceChanged(instances []*api.CatalogService) (err error) {
    err = c.syncInstance()
    return err
}

func (c *consul) extractDiff(old, new providers.CacheIterface) (add []*v2.Instance, update []*sv.Instance, del []*sv.Instance) {
    // pp.Println(len(old.List()), len(new.List()))
    add = []*sv.Instance{}
    update = []*sv.Instance{}
    del = []*sv.Instance{}
    if old == nil && new != nil {
        for _, ins := range new.List() {
            add = append(add, ins)
        }
        return
    } else if old != nil && new == nil {
        return
    } else if old != nil && new != nil {
        // add & update events
        for _, newIns := range new.List() {
            // new cache has the instance in old cache
            if oldIns := old.Get(newIns.InstanceId); oldIns != nil {
                // update events
                if newIns.Reversion > oldIns.Reversion {
                    if c.VerifyInstance(newIns) {
                        newIns.Status = providers.InstanceStatusOnline
                        newIns.Enabled = true
                        newIns.State = providers.InstanceStateRunning
                        update = append(update, newIns)
                    } else {
                        log.Logger.Warnf("verify instance failed, appcode: %s, instanceid: %s", newIns.AppCode, newIns.InstanceId)
                    }
                }
            } else {
                // add events
                if c.VerifyInstance(newIns) {
                    newIns.Status = providers.InstanceStatusOnline
                    newIns.Enabled = true
                    newIns.State = providers.InstanceStateRunning
                    add = append(add, newIns)
                } else {
                    log.Logger.Warnf("verify instance failed, appcode: %s, instanceid: %s", newIns.AppCode, newIns.InstanceId)
                }
            }
        }
        // delete events
        for _, oldIns := range old.List() {
            // old cache has the instance which not in new cache
            if newIns := new.Get(oldIns.InstanceId); newIns == nil {
                // delete events
                oldIns.Status = providers.InstanceStatusUnhealthy
                oldIns.Enabled = false
                oldIns.State = providers.InstanceStateProbing
                del = append(del, oldIns)
            }
        }
    }

    return
}

// VerifyInstance checks wether the instance is valid
func (c *consul) VerifyInstance(ins *sv.Instance) bool {
    if c.filters != nil && len(c.filters) > 0 {
        for _, f := range c.filters {
            if !f(ins) {
                return false
            }
        }
    }

    return true
}

// EventsSync sync the event to the finder
func (c *consul) EventsSync(add, update, del []*sv.Instance) {
    if len(add) > 0 {
        for _, ins := range add {
            c.eventSync(ins, time.Now().Unix())
        }
    }
    if len(update) > 0 {
        for _, ins := range update {
            c.eventSync(ins, time.Now().Unix())
        }
    }
    if len(del) > 0 {
        for _, ins := range del {
            c.eventSync(ins, time.Now().Unix())
        }
    }
}

// eventSync sync the event to the finder
func (c *consul) eventSync(ins *sv.Instance, triggerTime int64) {
    c.worker.Handle(&worker.Event{
        Trigger: triggerTime,
        Data:    []*sv.Instance{ins},
        Operate: worker.OperateTypeSync,
    })
}

// ProcessIntervalFullPush will sync all Instances of the current Provider to the MfwRegistry.
// Note: The current synchronization behavior is not to directly call the SyncAll method of MfwRegistry,
// but to perform instances comparison and do instance synchronization one by one using Method CompareAndFlush.
// TODO: 各个 Provider 方法重复，后期需要优化
func (c *consul) ProcessIntervalFullPush() {
    var interval time.Duration = providers.FullPushInterval
    if c.interval != 0 {
        interval = time.Duration(c.interval) * time.Second
    }
    ticker := time.NewTicker(interval)
    for {
        select {
        case <-ticker.C:
            before := time.Now()
            c.CompareAndFlush()
            after := time.Now()
            offset := after.Sub(before).Milliseconds()
            metrics.SyncAllEcsDurationsHistogram.Observe(float64(offset))
            log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
        case <-c.ctx.Done():
            ticker.Stop()
            return
        }
    }
}

// CompareAndFlush compare and find diff instances then flush
func (c *consul) CompareAndFlush() {
    c.Lock()
    defer c.Unlock()
    log.Logger.Infof("trying to compare and find diff instances then flush")
    // Here, we assume that the consul data is impossible to be empty. Once it is empty,
    // no operation is performed.
    if all := c.GetAll(); all != nil && len(all) > 0 {
        // 处理缓存
        c.cache.Clear()
        onlineCount := 0
        for _, item := range all {
            c.cache.ReplaceOrInsert(item)
            if item.Status == 1 {
                onlineCount++
            }
        }
        // 对比差异并增量同步
        registryList, err := c.worker.GetAll(providers.InstanceStatusOnline, providers.ProviderEcs)
        if err != nil {
            err = errors.WithMessage(err, "get all instances from mfwregistry")
            log.Logger.Errorf(err.Error())
            return
        }
        if registryList == nil || registryList.Instance == nil || len(registryList.Instance) == 0 {
            for _, ins := range all {
                c.buildAndSendEvent(ins)
            }
            return
        }
        remoteInstances := providers.ListToMap(registryList.GetInstance())
        // pp.Println(remoteInstances)
        currentProviderInstances := providers.ListToMap(all)
        log.Logger.Infof("mfwregistry online ecs instances size :%d  consul online instance size :%d  total :%d", len(remoteInstances), onlineCount, len(currentProviderInstances))

        for consulKey, consulIns := range currentProviderInstances {
            // For these instances in both Provider and MfwRegistry, if the information in Provider is newer, push is performed.
            if servIns, exist := remoteInstances[consulKey]; exist {
                diff := false
                // If K8s instance Version > Finder instance version
                if consulIns.Reversion > servIns.Reversion {
                    diff = true
                } else if consulIns.Reversion == servIns.Reversion {
                    // If env-type not equal
                    if consulIns.EnvType != servIns.EnvType ||
                        consulIns.EnvGroup != servIns.EnvGroup ||
                        consulIns.Status != servIns.Status ||
                        consulIns.Ip != servIns.Ip ||
                        consulIns.Idc != servIns.Idc ||
                        consulIns.Cluster != servIns.Cluster ||
                        consulIns.Enabled != servIns.Enabled ||
                        consulIns.AppCode != servIns.AppCode ||
                        consulIns.Cpu != servIns.Cpu {
                        diff = true
                    }
                }
                if diff {
                    log.Logger.Infof("the instance: %s of appcode: %s is newer, trigger a push.", consulIns.InstanceId, consulIns.AppCode)
                    c.buildAndSendEvent(consulIns)
                }
                delete(currentProviderInstances, consulKey)
                delete(remoteInstances, consulKey)
            } else {
                // For these instances in both Provider but not in MfwRegistry, the instance should be added to MfwRegistry.
                log.Logger.Infof("consul match much id : %s, status: %d", consulIns.InstanceId, consulIns.Status)
                if consulIns.Status == 1 {
                    c.buildAndSendEvent(consulIns)
                }
                delete(currentProviderInstances, consulKey)
            }
        }
        // The instances remaining in the MfwRegistry variable（remoteInstances） are either old or not in the Provider instance list.
        // In this case, we should delete it from MfwRegistry (that is, set it to Status=2 and push it).
        if len(remoteInstances) > 0 {
            log.Logger.Infof("process mfwregistry instance deleting. instance size: %d", len(remoteInstances))
            for _, servIns := range remoteInstances {
                servIns.Status = 2
                log.Logger.Infof("mfwregistry server has the old instance, set its Status filed as 2, and trigger a push. instance: %v", servIns)
                c.buildAndSendEvent(servIns)
            }
        }
    }
}

func (c *consul) buildAndSendEvent(instance *sv.Instance) {
    // if instance status is 0 , don't send event
    c.pool.Submit(func() {
        if instance.Status == 0 {
            return
        }
        ins := make([]*sv.Instance, 1)
        ins[0] = instance
        triggerTime := time.Now().Unix()
        event := &worker.Event{
            Trigger: triggerTime,
            Data:    ins,
            Operate: worker.OperateTypeSync,
        }
        c.worker.Handle(event)
    })
}

// withExpiryDuration sets up the interval time of cleaning up goroutines.
func withExpiryDuration(expiryDuration time.Duration) ants.Option {
    return func(opts *ants.Options) {
        opts.ExpiryDuration = expiryDuration
    }
}

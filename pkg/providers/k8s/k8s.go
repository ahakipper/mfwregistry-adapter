package k8s

import (
    "context"
    "github.com/panjf2000/ants/v2"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/metrics"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/worker"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/tools/unit"
    client "gitlab.mfwdev.com/servicemesh/robot"
    v1 "k8s.io/api/core/v1"
    "strings"
    "sync"
    "time"
)

// K8S provider implement
type k8s struct {
    robot      client.Robot               // the K8s multi-cluster aggregator
    ctx        context.Context            // context
    worker     worker.Worker              // the role of worker is to synchronize provider changes to the discovery center
    stopped    bool                       // whether the current provider has stopped
    sync.Mutex                            // lock mutex
    interval   int                        // the time interval for full synchronization. default 600s(10m)
    filters    []providers.InstanceFilter // finters is a collection of functions used to filter invalid instances
    cache      providers.CacheIterface    // pod cache
    pool       *ants.Pool                 // goroutine pool
}

// NewK8SProvider Init k8s provider
func NewK8SProvider(ctx context.Context, worker worker.Worker, pushInterval int, configPath []string) (provider providers.Provider, err error) {
    clusters := make([]client.Cluster, len(configPath))
    for idx, path := range configPath {
        clusters[idx] = client.Cluster{
            ConfigPath: path,
            Resources: []client.RN{
                {
                    client.Pods,
                    "",
                },
            },
        }
    }
    var kr client.Robot
    kr, err = client.NewRobot(clusters, false)
    if err != nil {
        // If walk here, server init failed
        return nil, err
    }

    // init k8s obj
    k := &k8s{
        robot:    kr,
        ctx:      ctx,
        worker:   worker,
        interval: pushInterval,
        cache:    providers.NewCache(2),
    }
    p, _ := ants.NewPool(providers.PoolBenchSize, withExpiryDuration(time.Second*providers.PoolExpireTime))
    k.pool = p
    k.filters = providers.InitInstanceFilters()

    provider = k

    return
}

// Run starts to monitor k8s cluster pod changes
func (k *k8s) Run() (err error) {
    log.Logger.Infof("start to run k8s provider")
    k.monitor()
    log.Logger.Info("k8s providers worker stopped")

    return nil
}

// monitor k8s pod changes
// Perform full instances synchronization periodically
// Perform instances comparison for single synchronization one by one. Note: this operation will only be executed once.
func (k *k8s) monitor() {
    go k.robot.Run()
    for {
        if k.robot.HasSynced() {
            break
        }
    }
    go k.ProcessIntervalFullPush()
    go k.CompareAndFlush()
    defer k.robot.Stop()
    // fork a goroutine to monitor pod change
    go func() {
        for {
            // get pod changes from k8s client
            obj, err := k.robot.Pop()
            if err != nil {
                if k.stopped {
                    break
                }
                log.Logger.Error("K8S watch error: ", err)
                time.Sleep(1 * time.Second)
                continue
            }
            // trigger time
            triggerTime := obj.CreateAt.Unix()
            // instance format
            var ins *sv.Instance
            if ins = k.pod2Instance(obj); ins == nil {
                continue
            }
            // rsync
            k.pool.Submit(func() {
                k.eventSync(ins, triggerTime)
            })
            k.robot.Finish(obj)
        }
    }()

    // wait to stop
    select {
    case <-k.ctx.Done():
        k.stopped = true
        break
    }

    log.Logger.Info("exit the k8s monitor")
}

//
func (k *k8s) ProcessCache(event client.Event, ins *sv.Instance) {
    switch event {
    case client.EventAdd:
        k.cache.ReplaceOrInsert(ins)
    case client.EventUpdate:
        k.cache.ReplaceOrInsert(ins)
    case client.EventDelete:
        k.cache.ReplaceOrInsert(ins)
    }
}

// VerifyInstance checks wether the instance is valid
func (k *k8s) VerifyInstance(ins *sv.Instance) error {
    if k.filters != nil && len(k.filters) > 0 {
        for _, f := range k.filters {
            if err := f(ins); err != nil {
                return err
            }
        }
    }

    return nil
}

// eventSync sync the event to the finder
func (k *k8s) eventSync(ins *sv.Instance, triggerTime int64) {
    k.worker.Handle(&worker.Event{
        Trigger: triggerTime,
        Data:    []*sv.Instance{ins},
        Operate: worker.OperateTypeSync,
    })
}

//
func (k *k8s) pod2Instance(obj client.QueueObject) (ins *sv.Instance) {
    // get pod info from k8s robot
    items, ok := k.robot.GetByKey(client.Pods, obj.Key)
    if ok && len(items) > 0 {
        pod := items[0].(*v1.Pod)
        instance := formatInstance(&obj, pod)
        if ver := k.VerifyInstance(instance); ver != nil {
            log.Logger.Warnf("invalid instance, instanceid: %s, reason: %s", instance.InstanceId, ver.Error())
            return nil
        }
        cacheInstance := k.cache.Get(instance.InstanceId)
        if cacheInstance == nil || k.hasInstanceDiff(cacheInstance, instance) {
            // put all exist instance to cache, purpose for get cache don't make npe
            k.ProcessCache(obj.Event, instance)
            ins = instance
        }
    } else {
        // If we cannot get the instance data, it means that this may be a DELETE event. At this point, the data in the
        // K8s robot no longer exists. However, in this scenario, the consumer of the instance needs
        // not only status = 2 but also the complete field data of the instance. Therefore, we have to fetch it from the cache.
        switch obj.Event {
        case client.EventAdd:
            fallthrough
            // log error
        case client.EventUpdate:
            // log error
        case client.EventDelete:
            instanceId := k.obj2InstanceId(obj)
            log.Logger.Infof("delete event, instanceid: %s", instanceId)
            if instance := k.cache.Get(instanceId); instance != nil {
                if instance.Status != providers.InstanceStatusOffline {
                    // set instance status
                    instance.Status = providers.InstanceStatusOffline
                    // delete cache
                    k.ProcessCache(client.EventDelete, instance)
                    ins = instance
                }
            }
        }
    }

    return ins
}

//
func (k *k8s) hasInstanceDiff(old, new *sv.Instance) (diff bool) {
    // If K8s instance Version > Finder instance version
    if old.Status == new.Status && old.Status == 3 { // if instance status is offline, we need not treat the change as difference
        diff = false
    } else if new.Reversion > old.Reversion {
        diff = true
    } else if new.EnvType != old.EnvType || new.State != old.State || new.Status != old.Status ||
        new.EnvGroup != old.EnvGroup || new.InstanceId != old.InstanceId || new.Ip != old.Ip ||
        new.Cpu != old.Cpu || new.Memory != old.Memory {
        diff = true
    }

    return diff
}

func (k *k8s) obj2InstanceId(obj client.QueueObject) string {
    if obj.Key != "" {
        keys := strings.Split(obj.Key, "/")
        if keys != nil && len(keys) >= 2 {
            return keys[1]
        }
    }

    return ""
}

// flush all instances
func (k *k8s) flushInstances() {
    if all := k.GetAll(); all != nil && len(all) > 0 {
        // flush the original cache and fill it
        before := time.Now()
        k.cache.Clear()
        for _, ins := range all {
            k.cache.ReplaceOrInsert(ins)
        }
        log.Logger.Infof("flush k8s cache spend time: %s", unit.RelTime(before, time.Now(), "", ""))
        // push all
        event := &worker.Event{
            Trigger: time.Now().Unix(),
            Data:    all,
            Operate: worker.OperateTypeSyncAll}
        k.worker.Handle(event)
    }
}

// CompareAndFlush compare and find diff instances then flush
func (k *k8s) CompareAndFlush() {
    if all := k.GetAll(); all != nil && len(all) > 0 {
        // 处理缓存
        k.cache.Clear()
        onlineCount := 0
        for _, item := range all {
            k.cache.ReplaceOrInsert(item)
            if item.Status == 1 {
                onlineCount++
            }
        }
        // 对比差异并增量同步
        list, err := k.worker.GetAll(providers.InstanceStatusOnline, providers.ProviderK8s)
        if err != nil {
            log.Logger.Errorf("get all instances from atlas failed")
        }
        if list == nil || list.Instance == nil || len(list.Instance) == 0 {
            for _, ins := range all {
                k.buildAndSendEvent(ins)
            }
            return
        } else if len(config.PushAppCodes) > 0 {
            instances := []*sv.Instance{}
            for _, ins := range list.Instance {
                for _, appcode := range config.PushAppCodes {
                    if appcode == ins.AppCode {
                        instances = append(instances, ins)
                    }
                }
            }
            list.Instance = instances
        }
        servMap := providers.ListToMap(list.GetInstance())
        k8sMap := providers.ListToMap(all)
        log.Logger.Infof("mfwregistry online k8s instances size :%d  k8s online instance size :%d  total :%d", len(servMap), onlineCount, len(k8sMap))
        for k8sKey, k8sIns := range k8sMap {
            if servIns, exist := servMap[k8sKey]; exist {
                diff := k.hasInstanceDiff(servIns, k8sIns)
                if diff {
                    k.buildAndSendEvent(k8sIns)
                }
                delete(k8sMap, k8sKey)
                delete(servMap, k8sKey)
            } else {
                log.Logger.Infof("k8s match much id: %v , status : %v \n", k8sIns.InstanceId, k8sIns.Status)
                if k8sIns.Status == 1 {
                    k.buildAndSendEvent(k8sIns)
                }
                delete(k8sMap, k8sKey)
            }
        }
        if len(servMap) > 0 {
            log.Logger.Infof("atlas server pre delete instance size :%d \n", len(servMap))
            for _, servIns := range servMap {
                servIns.Status = 2
                k.buildAndSendEvent(servIns)
            }
        }
    }
}

func (k *k8s) buildAndSendEvent(instance *sv.Instance) {
    // if instance status is 0 , don't send event
    k.pool.Submit(func() {
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
        k.worker.Handle(event)
    })
}

func (k *k8s) GetAll() (result []*sv.Instance) {
    items := k.robot.List(client.Pods)
    for _, item := range items {
        pod := item.(*v1.Pod)
        instance := formatInstance(nil, pod)
        if instance == nil {
            continue
        }
        if ver := k.VerifyInstance(instance); ver == nil {
            if result == nil {
                result = []*sv.Instance{}
            }
            result = append(result, instance)
        } else {
            log.Logger.Warnf("invalid instance, instanceid: %s, reason: %s", instance.InstanceId, ver.Error())
        }
    }
    log.Logger.Infof("k8s get all size: %d", len(result))

    return
}

// ProcessIntervalFullPush will sync all Instances of the current Provider to the MfwRegistry.
// Note: The current synchronization behavior is not to directly call the SyncAll method of MfwRegistry,
// but to perform instances comparison and do instance synchronization one by one using Method CompareAndFlush.
func (k *k8s) ProcessIntervalFullPush() {
    interval := providers.FullPushInterval
    if k.interval != 0 {
        interval = time.Duration(k.interval) * time.Second
    }
    ticker := time.NewTicker(interval)
    for {
        select {
        case <-ticker.C:
            before := time.Now()
            k.CompareAndFlush()
            after := time.Now()
            offset := after.Sub(before).Milliseconds()
            metrics.SyncAllK8sDurationsHistogram.Observe(float64(offset))
            log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
        case <-k.ctx.Done():
            ticker.Stop()
            return
        }
    }
}

// withExpiryDuration sets up the interval time of cleaning up goroutines.
func withExpiryDuration(expiryDuration time.Duration) ants.Option {
    return func(opts *ants.Options) {
        opts.ExpiryDuration = expiryDuration
    }
}

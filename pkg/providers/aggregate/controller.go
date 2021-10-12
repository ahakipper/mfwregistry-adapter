package aggregate

import (
    "context"
    "github.com/panjf2000/ants/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers"
)

var Providers []providers.Provider

func RegisterAggregateProvider(provider providers.Provider) {
    if provider != nil {
        Providers = append(Providers, provider)
    }
}

// Controller
// The purpose of Controller is to aggregate all providers and perform uniform behaviors.
// In this way, each provider does not need to execute unified repetitive logic separately. For example: CompareAndFlush.
type Controller struct {
    interval int                        // the time interval for full synchronization. default 600s(10m)
    filters  []providers.InstanceFilter // filters is a collection of functions used to filter invalid instances
    cache    providers.CacheIterface    // pod cache
    pool     *ants.Pool                 // goroutine pool
    ctx      context.Context
}

func NewAggregateController() *Controller {
    return &Controller{}
}

//func (ag *Controller) processIntervalFullPush() {
//    interval := providers.FullPushInterval
//    if ag.interval != 0 {
//        interval = time.Duration(ag.interval) * time.Second
//    }
//    ticker := time.NewTicker(time.Second * time.Duration(interval))
//    for {
//        select {
//        case <-ticker.C:
//            before := time.Now()
//            ag.compareAndFlush()
//            after := time.Now()
//            offset := after.Sub(before).Milliseconds()
//            metrics.SyncAllDurationsHistogram.Observe(float64(offset))
//            log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
//        case <-ag.ctx.Done():
//            ticker.Stop()
//            return
//        }
//    }
//}
//
//// compare and find diff instances then flush
//func (ag *Controller) compareAndFlush() {
//    if all := ag.GetAll(); all != nil && len(all) > 0 {
//        // 处理缓存
//        k.cache.Clear()
//        onlineCount := 0
//        for _, item := range all {
//            k.cache.ReplaceOrInsert(item)
//            if item.Status == 1 {
//                onlineCount++
//            }
//        }
//        // 对比差异并增量同步
//        list, err := k.worker.GetAll(providers.InstanceStatus, providers.ProviderK8s)
//        if err != nil {
//            log.Logger.Errorf("get all instances from atlas failed")
//        }
//        if list == nil || list.Instance == nil || len(list.Instance) == 0 {
//            for _, ins := range all {
//                k.buildAndSendEvent(ins)
//            }
//            return
//        }
//        servMap := providers.ListToMap(list.GetInstance())
//        k8sMap := providers.ListToMap(all)
//        log.Logger.Infof("atlas online instance size :%d  k8s online instance size :%d  total :%d", len(servMap), onlineCount, len(k8sMap))
//        for k8sKey, k8sIns := range k8sMap {
//            if servIns, exist := servMap[k8sKey]; exist {
//                diff := false
//                // If K8s instance Version > Finder instance version
//                if k8sIns.Reversion > servIns.Reversion {
//                    diff = true
//                }
//                // If env-type not equal
//                if k8sIns.EnvType != servIns.EnvType {
//                    diff = true
//                }
//                // If env-group not equal
//                if k8sIns.EnvGroup != servIns.EnvGroup {
//                    diff = true
//                }
//                if diff {
//                    k.buildAndSendEvent(k8sIns)
//                }
//                delete(k8sMap, k8sKey)
//                delete(servMap, k8sKey)
//            } else {
//                log.Logger.Infof("k8s match much id : %v , status : %v \n", k8sIns.InstanceId, k8sIns.Status)
//                if k8sIns.Status == 1 {
//                    k.buildAndSendEvent(k8sIns)
//                }
//                delete(k8sMap, k8sKey)
//            }
//        }
//        if len(servMap) > 0 {
//            log.Logger.Infof("atlas server pre delete instance size :%d \n", len(servMap))
//            for _, servIns := range servMap {
//                servIns.Status = 2
//                k.buildAndSendEvent(servIns)
//            }
//        }
//    }
//}
//
//func (ag *Controller) buildAndSendEvent(instance *sv.Instance) {
//    // if instance status is 0 , don't send event
//    k.pool.Submit(func() {
//        if instance.Status == 0 {
//            return
//        }
//        ins := make([]*sv.Instance, 1)
//        ins[0] = instance
//        triggerTime := time.Now().Unix()
//        event := &worker.Event{
//            Trigger: triggerTime,
//            Data:    ins,
//            Operate: worker.OperateTypeSync,
//        }
//        k.worker.Handle(event)
//    })
//}

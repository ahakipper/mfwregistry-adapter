package resource

import (
    "context"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/tools/unit"
    client "gitlab.mfwdev.com/servicemesh/robot"
    v1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/api/resource"
    "regexp"
    "strconv"
    "strings"
    "sync"
    "time"
)

// K8S provider implement
type k8s struct {
    robot      client.Robot    // the K8s multi-cluster aggregator
    ctx        context.Context // context
    worker     worker.Worker   // the role of worker is to synchronize provider changes to the discovery center
    stopped    bool            // whether the current provider has stopped
    sync.Mutex                 // lock mutex
    interval   int             // the time interval for full synchronization. default 600s(10m)
}

const (
    InstanceOnline   = "online"
    InstanceOffline  = "offline"
    InstancePrepared = "prepared"
    InstanceFailed   = "failed"
)

type RuntimeConfig struct {
    Cpu          float32           `json:"cpu"`    // cpu size
    Memory       int32             `json:"memory"` // memory size
    Image        map[string]string `json:"image"`
    Environments map[string]string `json:"environments"`
}

// Init k8s provider
func NewK8SProvider(ctx context.Context, worker worker.Worker, pushInterval int, configPath ...string) (provider K8SProvider, err error) {
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
    kr, err = client.NewRobot(clusters...)
    if err != nil {
        // If walk here, server init failed
        return nil, err
    }

    provider = &k8s{
        robot:    kr,
        ctx:      ctx,
        worker:   worker,
        interval: pushInterval,
    }

    return
}

// Monitor k8s cluster pod changes
func (k *k8s) Start() {
    go k.robot.Run()

    // make sure all task done
    k.monitor()

    log.Logger.Info("k8s resource worker stopped")
}

// monitor k8s pod changes
func (k *k8s) monitor() {
    // first we should flush all the instances
    k.flushInstances()
    // synchronize periodically
    go k.processIntervalFullPush()
    // monitor instance changes
    for {
        select {
        case <-k.ctx.Done():
            k.stopped = true
            k.robot.Stop()
            goto monitorEnd
        default:
            // get pod changes from k8s client
            obj, err := k.robot.Pop()
            if err != nil {
                log.Logger.Error("K8S watch error: ", err)
                return
            }
            //
            triggerTime := obj.CreateAt.Unix()
            switch obj.Event {
            case client.EventAdd:
                k.eventAdd(obj.Key, triggerTime)
            case client.EventUpdate:
                k.eventUpdate(obj.Key, triggerTime)
            case client.EventDelete:
                k.eventDelete(obj.Key, triggerTime)
            }

            k.robot.Finish(obj)
        }
    }

monitorEnd:
    log.Logger.Info("exit the k8s monitor")
}

// eventAdd get pod info by key, find an instance from mysql where health_state != Running
// and update this instance by updateInstance
func (k *k8s) eventAdd(key string, triggerTime int64) {
    // get pod info from monitor
    items, ok := k.robot.GetByKey(client.Pods, key)
    if ok && len(items) > 0 {
        pod := items[0].(*v1.Pod)
        instance := formatInstance(pod)
        // callback
        k.worker.Handle(&worker.Event{
            Trigger: triggerTime,
            Data:    instance,
            Operate: worker.OperateTypeADD,
        })
    } else {
        // log error
    }
}

// eventUpdate update instance by updateInstance
func (k *k8s) eventUpdate(key string, triggerTime int64) {
    // get pod info from monitor
    items, ok := k.robot.GetByKey(client.Pods, key)
    if ok {
        pod := items[0].(*v1.Pod)
        // populate pod info to instance
        if pod == nil {
            log.Logger.Errorf("time [%d] update error due to nil instance", triggerTime)
            return
        }
        instance := formatInstance(pod)
        instance = instance
        // callback
        k.worker.Handle(&worker.Event{
            Trigger: triggerTime,
            Data:    instance,
            Operate: worker.OperateTypeUPDATE,
        })
    }
}

// delete instance by pod name in mysql
func (k *k8s) eventDelete(key string, triggerTime int64) {
    instance := &sv.Instance{
        InstanceId: strings.Split(key, "/")[1],
        Enabled:    false,
        State:      InstanceOffline,
    }
    // callback
    k.worker.Handle(&worker.Event{
        Trigger: triggerTime,
        Data:    instance,
        Operate: worker.OperateTypeDELETE,
    })
}

// delete instance by pod name in mysql
func (k *k8s) flushInstances() {
    if all := k.GetAll(); all != nil && len(all) > 0 {
        for _, ins := range all {
            event := &worker.Event{Trigger: time.Now().Unix(), Data: ins, Operate: worker.OperateTypeADD}
            k.worker.Handle(event)
        }
    }
}

func (k *k8s) GetAll() (result []*sv.Instance) {
    items := k.robot.List(client.Pods)
    for _, item := range items {
        pod := item.(*v1.Pod)
        instance := formatInstance(pod)
        if result == nil {
            result = []*sv.Instance{}
        }
        result = append(result, instance)
    }

    return
}

func (k *k8s) processIntervalFullPush() {
    interval := 600
    if k.interval != 0 {
        interval = k.interval
    }
    ticker := time.NewTicker(time.Second * time.Duration(interval))
    for {
        select {
        case <-ticker.C:
            before := time.Now()
            k.flushInstances()
            log.Logger.Infof("the synchronization operation is completed periodically, interval: %d, time spend: %s", interval, unit.RelTime(before, time.Now(), "", ""))
        case <-k.ctx.Done():
            ticker.Stop()
            return
        }
    }
}

func formatInstance(pod *v1.Pod) (ins *sv.Instance) {
    if pod == nil {
        return ins
    }
    // labels
    labels := pod.Labels
    var runtimeConfig *RuntimeConfig
    var envType string
    var ports []*sv.PortInfo
    var cpu float32
    var memory int32
    var envs map[string]string
    var images = make(map[string]string)
    // A pod may contains more than one containers
    for _, container := range pod.Spec.Containers {

        // only get the  container env info that named "application"
        if container.Name == "application" {
            envs = make(map[string]string, len(container.Env))
            for _, env := range container.Env {
                envs[env.Name] = env.Value
            }

        }
        images[container.Name] = container.Image
        for _, e := range container.Env {
            if e.Name == "K8S_CLUSTER_TYPE" {
                envType = e.Value
            }
        }
        // merge containers cpu and memory limit value
        cpu = cpu + formatCpuSize(container.Resources.Limits.Cpu())
        memory = memory + formatMemorySize(container.Resources.Limits.Memory())
        // format container port info
        for _, p := range container.Ports {
            ports = append(ports, &sv.PortInfo{
                Name:     p.Name,
                Protocol: string(p.Protocol),
                Port:     p.ContainerPort,
            })
        }
    }
    // cpu and memroy
    runtimeConfig = &RuntimeConfig{
        Cpu:          cpu,
        Memory:       memory,
        Image:        images,
        Environments: envs,
    }
    // format state and appcode
    state := formatState(pod)
    appCode := formatAppCode(pod)
    // filter the invalid pod
    if appCode == "" {
        return
    }
    envType = formatEnvType(pod, envType)
    // enable state
    enabled := formatContainerEnabled(pod)
    // reversion
    // pay attention. The reversion field is the resource version of the instance, which is strictly self-increasing.
    // The push behavior of data may not be able to reach the opposite end in an orderly manner under a complex network environment.
    // Therefore, when the opposite end receives data, it needs to compare the reversion value with the existing value.
    // If the value is larger, it can be stored in the database. Otherwise, refuse to update.
    reversion, _ := strconv.ParseInt(pod.ObjectMeta.ResourceVersion, 10, 64)

    // convert pod to instance
    ins = &sv.Instance{
        InstanceId:  pod.Name,
        Ports:       ports,
        AppCode:     appCode,
        EnvType:     envType,
        EnvGroup:    labels["env-group"],
        Version:     labels["version"],
        HealthState: "passing",
        Ip:          pod.Status.PodIP,
        Enabled:     enabled,
        State:       state,
        Provider:    "k8s",
        Hostname:    pod.Name,
        Cpu:         runtimeConfig.Cpu,
        Memory:      runtimeConfig.Memory,
        Image:       runtimeConfig.Image,
        Idc:         labels["version"],
        Reversion:   reversion,
    }

    return
}

func formatAppCode(pod *v1.Pod) (appCode string) {
    labels := pod.Labels
    if code, ok := labels["app-code"]; ok {
        appCode = code
    } else if code, ok := labels["cadvisor-app"]; ok {
        appCode = code
    } else {
        appCode = pod.Namespace + "-" + labels["name"]
    }
    return
}

func formatEnvType(pod *v1.Pod, envType string) string {
    labels := pod.Labels
    if env, ok := labels["env-type"]; ok && envType == "" {
        envType = env
    } else if env, ok := labels["K8S_CLUSTER_TYPE"]; ok && envType == "" {
        envType = env
    }
    envType = strings.ToLower(envType)
    if envType == "online" {
        envType = "product"
    }
    return envType
}

func formatContainerEnabled(pod *v1.Pod, ) (enabled bool) {
    if pod != nil && pod.Status.Phase == v1.PodRunning {
        var ready = true
        for _, c := range pod.Status.ContainerStatuses {
            if c.Ready == false || c.State.Running == nil {
                ready = false
            }
        }
        if ready == true {
            enabled = true
        }
    }

    return
}

// 1000m = 1
func formatCpuSize(r *resource.Quantity) (count float32) {
    if cpuInt, ok := r.AsInt64(); !ok {
        cpuStr := r.String()
        reg := regexp.MustCompile(`\d+`)
        c, err := strconv.Atoi(string(reg.Find([]byte(cpuStr))))
        if err != nil {
            log.Logger.Error("format cpu error: cpu=", cpuStr)
        }
        count = float32(c) / 1000
    } else {
        count = float32(cpuInt)
    }
    return
}

// formatMemorySize is responsible for formatting the memory value of the instance
func formatMemorySize(r *resource.Quantity) (memory int32) {
    memoryInt, _ := r.AsInt64()
    memoryStr := r.String()
    if strings.Contains(memoryStr, "i") {
        memory = int32(memoryInt / 1024 / 1024)
    } else {
        memory = int32(memoryInt / 1000 / 1000)
    }
    return
}

// formatState is responsible for formatting the state of the instance
func formatState(pod *v1.Pod) (state string) {

    if formatContainerEnabled(pod) {
        state = InstanceOnline
    } else {
        switch pod.Status.Phase {
        case v1.PodPending:
            state = InstancePrepared
        case v1.PodUnknown:
        case v1.PodFailed:
            state = InstanceFailed
        }
    }
    return
}

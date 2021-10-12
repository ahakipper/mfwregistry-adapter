package k8s

import (
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers"
    client "gitlab.mfwdev.com/servicemesh/robot"
    v1 "k8s.io/api/core/v1"
    "k8s.io/apimachinery/pkg/api/resource"
    "regexp"
    "strconv"
    "strings"
)

// TODO obj param optimize
func formatInstance(obj *client.QueueObject, pod *v1.Pod) (ins *sv.Instance) {
    if pod == nil {
        return ins
    }
    // labels
    labels := pod.Labels
    var runtimeConfig *providers.RuntimeConfig
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
    }
    // format container port
    ports = formatAppPort(pod)

    // cpu and memroy
    runtimeConfig = &providers.RuntimeConfig{
        Cpu:          cpu,
        Memory:       memory,
        Image:        images,
        Environments: envs,
    }
    // format state and appcode
    appCode := formatAppCode(pod)
    // filter the invalid pod
    if appCode == "" {
        return
    }
    // filter appcodes
    if config.PushAppCodes != nil {
        for _, code := range config.PushAppCodes {
            if appCode != code {
                return nil
            }
        }
    }
    state := formatState(pod)
    status := formatStatus(obj, pod)
    envType = formatEnvType(pod, envType)
    idc := formatIDC(pod)
    cluster := formatCluster(pod)
    // enable state
    enabled := formatContainerEnabled(pod)
    // reversion
    // pay attention. The reversion field is the providers version of the instance, which is strictly self-increasing.
    // The push behavior of data may not be able to reach the opposite end in an orderly manner under a complex network environment.
    // Therefore, when the opposite end receives data, it needs to compare the reversion value with the existing value.
    // If the value is larger, it can be stored in the database. Otherwise, refuse to update.
    reversion, _ := strconv.ParseInt(pod.ObjectMeta.ResourceVersion, 10, 64)

    // envgroup
    envGroup := ""
    if eg, ok := labels["env-group"]; ok && eg != "" {
        envGroup = eg
    }

    // envCode
    envCode := envType + "#" + envGroup

    // set application.name
    label := formatLableInfo(pod, labels, runtimeConfig.Environments)

    // convert pod to instance
    ins = &sv.Instance{
        InstanceId: pod.Name,
        Ports:      ports,
        AppCode:    appCode,
        EnvCode:    envCode,
        EnvType:    envType,
        EnvGroup:   envGroup,
        Version:    labels["version"],
        Ip:         pod.Status.PodIP,
        Enabled:    enabled,
        State:      state,
        Provider:   "k8s",
        Hostname:   pod.Name,
        Cpu:        runtimeConfig.Cpu,
        Memory:     runtimeConfig.Memory,
        Image:      runtimeConfig.Image,
        Idc:        idc,
        Cluster:    cluster,
        Reversion:  reversion,
        Status:     status,
        Label:      label,
    }

    return
}

// format lable info
func formatLableInfo(pod *v1.Pod, originLables map[string]string, envs map[string]string) (lablel map[string]string) {
    if lablel == nil {
        lablel = make(map[string]string)
    }
    // for Java SDK
    if envs != nil && len(envs) > 0 {
        if san, exist := envs[providers.InstanceSpringApplicationName]; exist {
            lablel[providers.InstanceCompatibilityLabelEnvSan] = san
        }
    }
    // for namespace
    lablel[providers.InstanceCompatibilityLabelAosNamespace] = ""
    if pod != nil {
        lablel[providers.InstanceCompatibilityLabelAosNamespace] = pod.Namespace
    }
    if originLables != nil && len(originLables) > 0 {
        // for specific label
        lablel[providers.InstanceCompatibilityLabelAosApp] = ""
        if lapp, exist := originLables["app"]; exist {
            lablel[providers.InstanceCompatibilityLabelAosApp] = lapp
        }
        // for destination rule and virtual service
        var drHost string
        // in case of FengXiao
        if _, exist := originLables["deploy-id"]; exist {
            drHost = lablel[providers.InstanceCompatibilityLabelAosApp] + "." + lablel[providers.InstanceCompatibilityLabelAosNamespace]
        } else {
            // in case of AosMicroservice
            drHost = lablel[providers.InstanceCompatibilityLabelAosApp]
        }
        lablel[providers.InstanceCompatibilityLabelAosDrHost] = drHost
        // in case of WebIDE
        if mark, exist := originLables["mark"]; exist {
            lablel[providers.InstanceCompatibilityLabelAosMark] = mark
        }
    }

    return
}

// formatAppPort format K8s container port to instance port info
func formatAppPort(pod *v1.Pod) (ports []*sv.PortInfo) {
    ports = []*sv.PortInfo{}
    if pod.Spec.Containers != nil && len(pod.Spec.Containers) > 0 {
        // TODO temp setting
        ports = append(ports, &sv.PortInfo{
            Name:     "dubbo" + "-" + strconv.FormatInt(7096, 10),
            Protocol: providers.ProtoDubbo,
            Port:     7096,
        })
        for _, container := range pod.Spec.Containers {
            if container.Ports != nil && len(container.Ports) > 0 {
                for _, p := range container.Ports {
                    proto := ""
                    if p.Protocol == v1.ProtocolTCP {
                        if strings.HasPrefix(p.Name, "http") {
                            proto = providers.ProtoHTTP
                        } else if strings.HasPrefix(p.Name, "grpc") {
                            proto = providers.ProtoGRPC
                        }
                    }
                    ports = append(ports, &sv.PortInfo{
                        Name:     p.Name,
                        Protocol: proto,
                        Port:     p.ContainerPort,
                    })

                }
            }
        }
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

// formatInstanceStatus convert instance status
func formatStatus(obj *client.QueueObject, pod *v1.Pod) (status int32) {
    status = providers.InstanceStatusOffline
    if pod == nil {
        status = providers.InstanceStatusOffline
    } else if obj != nil && obj.Event == client.EventDelete {
        status = providers.InstanceStatusOffline
    } else if !pod.DeletionTimestamp.IsZero() {
        status = providers.InstanceStatusOffline
    } else {
        if pod.Status.Phase == v1.PodRunning {
            var ready = true
            for _, c := range pod.Status.ContainerStatuses {
                if c.Ready == false || c.State.Running == nil {
                    ready = false
                }
            }
            if ready == true {
                status = providers.InstanceStatusOnline
            } else {
                status = providers.InstanceStatusUnhealthy
            }
        } else if pod.Status.Phase == v1.PodFailed {
            if pod.Status.Reason == "Evicted" {
                status = providers.InstanceStatusUnhealthy
            }
        } else if pod.Status.Phase == v1.PodPending {
            status = providers.InstanceStatusUnhealthy
        }
    }

    return
}

func formatEnvType(pod *v1.Pod, envType string) string {
    labels := pod.Labels
    if env, ok := labels["env-type"]; ok && envType != "" {
        envType = env
    } else if env, ok := labels["K8S_CLUSTER_TYPE"]; ok && envType == "" {
        envType = env
    }
    envType = strings.ToLower(envType)
    if envType == "online" {
        envType = providers.EnvProduct
    }
    return envType
}

func formatContainerEnabled(pod *v1.Pod) (enabled bool) {
    if pod != nil && pod.Status.Phase == v1.PodRunning {
        var ready = true
        for _, c := range pod.Status.ContainerStatuses {
            if c.Ready == false || c.State.Running == nil {
                ready = false
            }
        }
        if pod.DeletionTimestamp != nil {
            ready = false
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
    if pod != nil {
        if !pod.DeletionTimestamp.IsZero() { // the pod is deleted
            state = providers.InstanceStateTerminated
        } else if pod.Status.Phase != v1.PodRunning {
            switch pod.Status.Phase {
            case v1.PodPending:
                state = providers.InstanceStatePending
            case v1.PodUnknown:
                state = providers.InstanceStateUnknown
            case v1.PodFailed:
                // For the eviction scenario, the pod stage status is Failed, but the reason is Evicted, we treat it specially
                if pod.Status.Reason == "Evicted" {
                    state = providers.InstanceStateEvicted
                } else {
                    state = providers.InstanceStateFailed
                }
            // PodSucceeded means that all the containers of the pod has exited with exit code 0, and we mark it as terminated.
            case v1.PodSucceeded:
                state = providers.InstanceStateTerminated
            }
        } else {
            already := true
            for _, cs := range pod.Status.ContainerStatuses {
                if !cs.Ready {
                    already = false
                    if cs.State.Waiting != nil && cs.LastTerminationState.Terminated != nil &&
                        cs.State.Waiting.Reason == "CrashLoopBackOff" &&
                        cs.LastTerminationState.Terminated.Reason == "Error" {
                        state = providers.InstanceStateError
                    } else if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
                        state = providers.InstanceStateCrash
                    }
                }
            }
            if already {
                state = providers.InstanceStateRunning
            } else {
                if state == "" {
                    state = providers.InstanceStateProbing
                } else {
                    state = providers.InstanceStateUnknown
                }
            }
        }
    }

    if state == "" {
        state = providers.InstanceStateUnknown
    }

    return state
}

// formatIDC is responsible for formatting the IDC attr of the instance
func formatIDC(pod *v1.Pod) string {
    // get idc from labels first
    if len(pod.Labels) > 0 {
        if idc, ok := pod.Labels["idc"]; ok {
            return idc
        }
    }
    // get idc from env
    envs := make(map[string]string)
    for _, container := range pod.Spec.Containers {
        // only get the  container env info that named "application"
        if container.Name == "application" {
            for _, env := range container.Env {
                envs[env.Name] = env.Value
            }
        }
    }
    if len(envs) > 0 {
        if cluster, ok := envs["APP_IDC"]; ok {
            return cluster
        }
    }

    return ""
}

// formatIDC is responsible for formatting the K8s Cluster attr of the instance
func formatCluster(pod *v1.Pod) string {
    // get cluster from labels first
    if len(pod.Labels) > 0 {
        if idc, ok := pod.Labels["cluster"]; ok {
            return idc
        }
    }
    // get idc from env
    envs := make(map[string]string)
    for _, container := range pod.Spec.Containers {
        // only get the  container env info that named "application"
        if container.Name == "application" {
            for _, env := range container.Env {
                envs[env.Name] = env.Value
            }
        }
    }
    if len(envs) > 0 {
        if cluster, ok := envs["K8S_CLUSTER_NAME"]; ok {
            return cluster
        }
    }

    return ""
}

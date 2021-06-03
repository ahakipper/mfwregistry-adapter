package providers

import (
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "time"
)

const (
    InstanceOnline   = "online"
    InstanceOffline  = "offline"
    InstancePrepared = "prepared"
    InstanceFailed   = "failed"
    InstanceStatus   = 1 // 0下线 1上线 -1全量
)

const (
    ProtoHTTP      = "http"
    ProtoGRPC      = "grpc"
    ProtoWebSocket = "websocket"
    ProtoDubbo     = "dubbo"
    PoolBenchSize  = 100
    PoolExpireTime = 100
)

const (
    ProviderK8s = "k8s"
    ProviderEcs = "ecs"
)

const (
    InstanceCompatibilityLabelAosNamespace = "compatibility:aos_namespace"
    InstanceCompatibilityLabelAosApp       = "compatibility:aos_app"     // Aos and Fengxiao had incompatibility issues with the app field in the label of the generated Pod from the beginning.
    InstanceCompatibilityLabelAosDrHost    = "compatibility:aos_dr_host" // Used for gateways to generate DestinationRule rules adapted to Aos microservices.
    InstanceCompatibilityLabelAosMark      = "compatibility:aos_mark"    // Used for Aos WebIDE
    InstanceCompatibilityLabelEnvSan       = "env:san"
    InstanceSpringApplicationName          = "spring.application.name"
)

const (
    FullPushInterval = 7200 * time.Second
)

type RuntimeConfig struct {
    Cpu          float32           `json:"cpu"`    // cpu size
    Memory       int32             `json:"memory"` // memory size
    Image        map[string]string `json:"image"`
    Environments map[string]string `json:"environments"`
}

type InstanceFilter func(ins *sv.Instance) bool

func ListToMap(ins []*sv.Instance) (m map[string]*sv.Instance) {
    m = make(map[string]*sv.Instance)
    for _, value := range ins {
        m[value.InstanceId] = value
    }
    return
}

func InitInstanceFilters() (filters []InstanceFilter) {
    filters = []InstanceFilter{}
    // Init a default instance filter
    filters = append(filters, func(ins *sv.Instance) bool {
        if ins == nil {
            return false
        }
        // Validate some fields that must not be empty.
        // Note: Do not valid ins.Version as it may truly be empty.
        if ins.AppCode == "" || ins.EnvType == "" || ins.Ip == "" || ins.Reversion == 0 {
            return false
        }
        return true
    })

    return filters
}

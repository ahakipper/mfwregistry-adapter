package providers

import (
    "fmt"
    "github.com/pkg/errors"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "time"
)

const (
    InstanceStatePending    = "pending"    // Instance not scheduled
    InstanceStateStarting   = "starting"   // Instance is starting
    InstanceStateProbing    = "probing"    //
    InstanceStateOOM        = "oom"        // The instance is OOM Killed. This state may exist for a very short time
    InstanceStateCrash      = "crash"      // Instance exited without code 0
    InstanceStateRunning    = "running"    // Instance is running
    InstanceStateError      = "error"      // Instance can not be stated. for instance: the start command path is incorrect
    InstanceStateFailed     = "failed"     // Instance can not be created due to system error, such as: K8s kubelet cni configuration invalid.
    InstanceStateTerminated = "terminated" // Instance is deleted.
    InstanceStateUnknown    = "unknown"    //
)

const (
    InstanceStatusUnknown   = 0
    InstanceStatusOnline    = 1 // Instance is online
    InstanceStatusUnhealthy = 2 //
    InstanceStatusOffline   = 3 // Instance is deleted
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
    EnvDev     = "dev"
    EnvTest    = "test"
    EnvStaging = "staging"
    EnvProduct = "product"
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
    FullPushInterval = 21600 * time.Second
)

type RuntimeConfig struct {
    Cpu          float32           `json:"cpu"`    // cpu size
    Memory       int32             `json:"memory"` // memory size
    Image        map[string]string `json:"image"`
    Environments map[string]string `json:"environments"`
}

type InstanceFilter func(ins *sv.Instance) error

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
    filters = append(filters, func(ins *sv.Instance) error {
        if ins == nil {
            return errors.New("nil resource instance")
        }
        // Validate some fields that must not be empty.
        // Note: Do not valid ins.Version as it may truly be empty.
        if ins.AppCode == "" {
            return errors.New("instance has nil appcode")
        }
        if ins.EnvType == "" {
            return errors.New("instance has nil env type")
        }
        if ins.Ip == "" {
            return errors.New("instance has nil ip")
        }
        if ins.Reversion == 0 {
            return errors.New("instance has nil reversion")
        }
        if ins.Status == InstanceStatusUnknown {
            return errors.New(fmt.Sprintf("instance has status unknown value: %d，may be the format process need to be performed", ins.Status))
        }

        return nil
    })

    return filters
}

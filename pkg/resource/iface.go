package resource

import (
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

// K8s resource provider
type K8SProvider interface {
    // Monitor k8s pod changes
    Start()

    // Monitor k8s pod changes
    GetAll() []*sv.Instance
}

// K8s resource provider
type KVMProvider interface {
    // Register when start deploy need to call
    Register(appCode, envType, envGroup string) error

    // UnRegister when deploy ended need to call
    UnRegister(appCode, envType, envGroup string)

    // Recycle resource
    Recycle()
}

package providers

import (
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
)

// Provider is a interface
type Provider interface {
    // Run starts to monitor s subject
    // The Run action should hang utils the monitor stopped.
    Run() error

    // GetAll get all the instances from current provider
    GetAll() []*sv.Instance
}

// K8s providers provider
type KVMProvider interface {
    // Register when start deploy need to call
    Register(appCode, envType, envGroup string) error

    // UnRegister when deploy ended need to call
    UnRegister(appCode, envType, envGroup string)

    // Recycle providers
    Recycle()
}

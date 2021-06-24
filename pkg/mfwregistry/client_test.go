package mfwregistry

import (
    "github.com/k0kubun/pp"
    sv "gitlab.mfwdev.com/mtech/beehive-proto/api/service/v2"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers"
    "testing"
)

var atlasDevAddr = "172.18.27.63:50051"

func TestClient_Sync(t *testing.T) {
    config.GrpcAddr = atlasDevAddr
    log.LoggerInit()
    client, err := NewInstance()
    if err != nil {
        t.Error(err)
    }
    // You need to increase the reversion value before performing the sync operation,
    // otherwise the synchronization will not take effect.
    // For MfwRegistry, only the reversion greater than its existing value of the instance can be updated.
    instances := []*sv.Instance{&sv.Instance{
        AppCode:    "test-test",
        InstanceId: "350461-test-mtest-98ff7cd8-rd2j9",
        Provider:   "ecs",
        Ip:         "172.18.18.18",
        Ports: []*sv.PortInfo{
            &sv.PortInfo{
                Name:     "http0",
                Protocol: "http",
                Port:     80,
            },
        },
        Enabled:   true,
        Status:    1,
        EnvCode:   providers.EnvDev,
        EnvGroup:  "inter111",
        Reversion: 1240,
    }}
    r, err := client.Sync(instances)
    if err != nil {
        t.Error(err.Error())
    }
    r = r
    // pp.Println(r.Code, r.Msg)
}

func TestClient_GetAllOfProviderK8s(t *testing.T) {
    config.GrpcAddr = atlasDevAddr
    log.LoggerInit()
    client, err := NewInstance()
    if err != nil {
        t.Error(err)
    }
    r, err := client.GetAll(1, "k8s")
    if err != nil {
        t.Error(err.Error())
    }
    r = r
    pp.Println(len(r.Instance))
}

func TestClient_GetAllOfProviderEcs(t *testing.T) {
    config.GrpcAddr = atlasDevAddr
    log.LoggerInit()
    client, err := NewInstance()
    if err != nil {
        t.Error(err)
    }
    r, err := client.GetAll(1, "ecs")
    r = r
    if err != nil {
        t.Error(err.Error())
    }
    pp.Println(len(r.Instance))
}

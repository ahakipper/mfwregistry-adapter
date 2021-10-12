package consul

import (
    "context"
    "github.com/hashicorp/consul/api"
    "github.com/k0kubun/pp"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/providers"
    "testing"
)

func init() {
    log.LoggerInit()
}

func TestRegisterService(t *testing.T) {
    var err error
    config := api.DefaultConfig()
    config.Address = "http://172.16.129.37:8520"
    client, err := api.NewClient(config)
    hostName := "knode172-16-129-37"
    service := &api.AgentServiceRegistration{
        Name: "redis",
        Port: 12311,
        Tags: []string{"microservice"},
        Meta: map[string]string{
            "ports":      "[{\"name\":\"http0\",\"protocal\":\"http\",\"port\":80}]",
            "appCode":    "test-mtest",
            "envType":    providers.EnvDev,
            "envGroup":   "inter1231112",
            "instanceId": hostName,
            "cluster":    "",
            "version":    "88722",
            "hostname":   hostName,
            "other_1":    "other_1",
            "other_2":    "other_2",
            "other_3":    "other_3",
        },
        Check: &api.AgentServiceCheck{
            HTTP:     "http://127.0.0.1:8120",
            Interval: "10s",
            Timeout:  "10s",
        }}

    if err = client.Agent().ServiceRegister(service); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
}

func TestDeRegisterService(t *testing.T) {
    var err error
    config := api.DefaultConfig()
    var client *api.Client
    config.Address = "172.16.129.146:8520"
    client, err = api.NewClient(config)
    serviceId := "testiterationc-msp"
    if err = client.Agent().ServiceDeregister(serviceId); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
}

func TestGetServices(t *testing.T) {
    var err error
    config := api.DefaultConfig()
    var client *api.Client
    config.Address = "172.16.129.38:8520"
    client, err = api.NewClient(config)
    serviceId := "testiterationc-msp"
    queryOptions := api.QueryOptions{
        WaitTime: blockQueryWaitTime,
    }
    var endpoints []*api.ServiceEntry
    if endpoints, _, err = client.Health().Service(serviceId, tagMicroservice, true, &queryOptions); err == nil {
        pp.Println(endpoints)
    } else {
        t.Error(err.Error())
        t.FailNow()
    }
}

func TestNewConsulMonitor(t *testing.T) {
    var err error
    var cf ConsulClientFactory
    if cf, err = NeweClientFacotorySimple([]string{"172.16.129.3:8520", "172.16.129.2:8520"}); err != nil {
        t.Error(err)
        t.FailNow()
    }
    var monitor Monitor
    if monitor, err = NewConsulMonitor(cf); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
    ctx, cancel := context.WithCancel(context.Background())
    _ = cancel
    if err = monitor.Start(ctx); err != nil {
        t.Error(err)
        t.FailNow()
    }
}

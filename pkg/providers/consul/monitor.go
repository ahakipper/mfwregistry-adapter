package consul

import (
    "context"
    "github.com/hashicorp/consul/api"
    consulwatch "github.com/hashicorp/consul/api/watch"
    "github.com/pkg/errors"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers/consul/watch"
    "golang.org/x/sync/errgroup"
    "time"
)

// Monitor handles service and instance changes
type Monitor interface {
    Start(ctx context.Context) error
    GetServices() (services map[string][]string, err error)
    GetServiceEntries(name string, q *api.QueryOptions) (endpoints []*api.ServiceEntry, err error)
    AppendServiceHandler(ServiceHandler)
    AppendInstanceHandler(InstanceHandler)
}

// InstanceHandler processes service instance change events
type InstanceHandler func(instance *api.CatalogService) error

// ServiceHandler processes service change events
type ServiceHandler func(instances []*api.CatalogService) error

type consulMonitor struct {
    // clientFactory is a consul api factory, used to return a valid consul client.
    // The main reason is that consul's client does not have load balancing capabilities and the ability to automatically select effective target nodes.
    clientFactory    ConsulClientFactory
    addr             string
    watcher          *watch.Watcher
    handler          consulwatch.HandlerFunc
    instanceHandlers []InstanceHandler
    serviceHandlers  []ServiceHandler
    stop             chan struct{}
}

const (
    refreshIdleTime    time.Duration = 50 * time.Millisecond
    periodicCheckTime  time.Duration = 50 * time.Millisecond
    blockQueryWaitTime time.Duration = 5 * time.Second

    tagMicroservice string = "microservice"
)

// NewConsulMonitor watches for changes in Consul services and CatalogServices
func NewConsulMonitor(clientF ConsulClientFactory) (monitor Monitor, err error) {
    if clientF == nil {
        err = errors.New("new consul monitor with nil consul client")
        return nil, err
    }
    cm := &consulMonitor{
        clientFactory:    clientF,
        instanceHandlers: make([]InstanceHandler, 0),
        serviceHandlers:  make([]ServiceHandler, 0),
        stop:             make(chan struct{}),
    }

    return cm, err
}

func (m *consulMonitor) Start(ctx context.Context) (err error) {
    change := make(chan struct{}, 64)
    defer close(change)

    eg := errgroup.Group{}
    eg.Go(func() error {
        defer log.Logger.Info("consul monitor watching action stopped")
        return m.watchConsul(ctx, change)
    })
    eg.Go(func() error {
        defer log.Logger.Info("consul monitor update record action stopped")
        return m.updateRecord(ctx, change)
    })
    err = eg.Wait()

    return err
}

// watchConsul starts to watch consul and process service & node & health change
func (m *consulMonitor) watchConsul(ctx context.Context, change chan struct{}) (err error) {
    var consulWaitIndex uint64
    var client *api.Client
    for {
        select {
        case <-ctx.Done():
            return
        default:
            queryOptions := api.QueryOptions{
                WaitIndex: consulWaitIndex,
                WaitTime:  blockQueryWaitTime,
            }
            // Get a valid consul client from the factory.
            var queryMeta *api.QueryMeta
            if client, err = m.clientFactory.ConsulClientFactory(); err != nil {
                log.Logger.Errorf(errors.WithMessage(err, "get consul client").Error())
                continue
            }
            // This Consul REST API will block until service changes or timeout
            // var svcs map[string][]string
            // fmt.Println(r.url.String() + r.url.Fragment,p.StatusCode)
            _, queryMeta, err = client.Health().State("any", &queryOptions)
            if err != nil {
                log.Logger.Warnf("could not fetch services: %s", err.Error())
            } else if consulWaitIndex != queryMeta.LastIndex {
                consulWaitIndex = queryMeta.LastIndex
                change <- struct{}{}
            }
            time.Sleep(periodicCheckTime)
        }
    }
}

func (m *consulMonitor) updateRecord(ctx context.Context, change <-chan struct{}) (err error) {
    lastChange := int64(0)
    ticker := time.NewTicker(periodicCheckTime)
    for {
        select {
        case <-ctx.Done():
            ticker.Stop()
            return err
        case <-ticker.C:
            currentTime := time.Now().Unix()
            if lastChange > 0 && currentTime-lastChange > int64(refreshIdleTime.Seconds()) {
                log.Logger.Infof("consul service changed")
                // m.updateServiceRecord()
                m.updateInstanceRecord()
                lastChange = int64(0)
            }
        case <-change:
            lastChange = time.Now().Unix()
        }
    }
}

func (m *consulMonitor) updateServiceRecord() {
    // This is only a work-around solution currently
    // Since Handler functions generally act as a refresher
    // regardless of the input, thus passing in meaningless
    // input should make functionalities work
    //TODO
    var obj []*api.CatalogService
    for _, f := range m.serviceHandlers {
        go func(handler ServiceHandler) {
            if err := handler(obj); err != nil {
                log.Logger.Warnf("Error executing service handler function: %v", err)
            }
        }(f)
    }
}

func (m *consulMonitor) updateInstanceRecord() {
    // This is only a work-around solution currently
    // Since Handler functions generally act as a refresher
    // regardless of the input, thus passing in meaningless
    // input should make functionalities work
    // TODO
    obj := &api.CatalogService{}
    for _, f := range m.instanceHandlers {
        go func(handler InstanceHandler) {
            if err := handler(obj); err != nil {
                log.Logger.Warnf("Error executing instance handler function: %v", err)
            }
        }(f)
    }
}

func (m *consulMonitor) AppendServiceHandler(h ServiceHandler) {
    m.serviceHandlers = append(m.serviceHandlers, h)
}

func (m *consulMonitor) AppendInstanceHandler(h InstanceHandler) {
    m.instanceHandlers = append(m.instanceHandlers, h)
}

func (c *consulMonitor) GetServices() (services map[string][]string, err error) {
    var client *api.Client
    if client, err = c.clientFactory.ConsulClientFactory(); err != nil {
        log.Logger.Errorf(errors.WithMessage(err, "get consul client").Error())
        return nil, err
    }
    data, _, err := client.Catalog().Services(nil)
    if err != nil {
        log.Logger.Warnf("Could not retrieve services from consul: %v", err)
        return nil, err
    }

    return data, nil
}

func (m *consulMonitor) GetServiceEntries(name string, q *api.QueryOptions) (endpoints []*api.ServiceEntry, err error) {
    var client *api.Client
    if client, err = m.clientFactory.ConsulClientFactory(); err != nil {
        log.Logger.Errorf(errors.WithMessage(err, "get consul client").Error())
        return nil, err
    }
    // filter the endpoint not tagged with "microservice". referer here: https://wiki.mafengwo.cn/x/mM3_Aw
    endpoints, _, err = client.Health().Service(name, tagMicroservice, false, q)
    if err != nil {
        log.Logger.Warnf("Could not retrieve service catalog from consul: %v", err)
        return nil, err
    }

    return endpoints, nil
}

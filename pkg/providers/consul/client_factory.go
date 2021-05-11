package consul

import (
    "github.com/hashicorp/consul/api"
    "github.com/pkg/errors"
    "sync"
)

type ConsulClientFactory interface {
    ConsulClientFactory() (*api.Client, error)
}

var DefaultConsulClientFactory ConsulClientFactory

type ClientFactorySimple struct {
    addrs   []string
    clients map[string]*api.Client
    sync.RWMutex
}

func NeweClientFacotorySimple(addrs []string) (*ClientFactorySimple, error) {
    if len(addrs) == 0 {
        return nil, errors.New("none consul addresses")
    }
    return &ClientFactorySimple{
        addrs:   addrs,
        clients: map[string]*api.Client{},
    }, nil
}

// ConsulClientFactory will return a valid consul client and save it in the internal map.
// Here you need to known is that consul client is based on HTTP, and it closes the connection at the end of the call internally,
// so we don't need to care about connection closure.
func (cfs *ClientFactorySimple) ConsulClientFactory() (client *api.Client, err error) {
    cfs.Lock()
    defer cfs.Unlock()
    if len(cfs.addrs) == 0 {
        err = errors.New("none consul addresses")
        return nil, err
    }
    // Try to get valid client from cache.
    if cfs.clients != nil && len(cfs.clients) > 0 {
        for _, c := range cfs.clients {
            var leader string
            if leader, err = c.Status().Leader(); err == nil && leader != "" {
                client = c
                break
            }
        }
    }
    // If there is no valid client in the cache, initialize one and save it.
    if client == nil {
        for _, addr := range cfs.addrs {
            conf := api.DefaultConfig()
            conf.Address = addr
            var c *api.Client
            c, err = api.NewClient(conf)
            var leader string
            if leader, err = c.Status().Leader(); err == nil && leader != "" {
                client = c
                cfs.clients[addr] = client
                break
            }
        }
        if err != nil {
            err = errors.WithMessage(err, "there is none valid consul address")
        }
    }
    //
    if client == nil {
        err = errors.New("none valid consul client")
    }

    return client, err
}

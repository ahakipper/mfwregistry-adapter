package consul

import (
    "context"
    "spotter/pkg/providers"
    "spotter/pkg/worker"
    "testing"
)

func TestNewConsulProvider(t *testing.T) {
    var err error
    // init cancel context
    wctx, _ := context.WithCancel(context.Background())
    // create and start the worker
    w := worker.NewResourceFackWorker(wctx)
    var provider providers.Provider
    if provider, err = NewConsulProvider(wctx, w, 300, []string{"http://10.72.73.172:8520", "http://10.72.73.172:8520"}); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
    if err = provider.Run(); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
}

package consul

import (
    "context"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/providers"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/worker"
    "testing"
)

func TestNewConsulProvider(t *testing.T) {
    var err error
    // init cancel context
    wctx, _ := context.WithCancel(context.Background())
    // create and start the worker
    w := worker.NewResourceFackWorker(wctx)
    var provider providers.Provider
    if provider, err = NewConsulProvider(wctx, w, 300, []string{"http://172.16.129.3:8520", "http://172.16.129.2:8520"}); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
    if err = provider.Run(); err != nil {
        t.Error(err.Error())
        t.FailNow()
    }
}

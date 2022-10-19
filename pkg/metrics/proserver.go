package metrics

import (
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "gitlab.mfwdev.com/paas/mfwregistry-adapter/pkg/log"
    "math/rand"
    "net/http"
    _ "net/http/pprof"
    "time"
)

type PrometheusService struct {
    Addr string
}

var srv *http.Server

func NewPrometheusServer(addr string) *PrometheusService {
    return &PrometheusService{
        Addr: addr,
    }
}

func (s *PrometheusService) Start() {
    //s.mock()
    srv = &http.Server{Addr: s.Addr}
    log.Logger.Info("prometheus server start ...")
    http.Handle("/metrics", promhttp.Handler())
    log.Logger.Fatal(srv.ListenAndServe())
}

func (s *PrometheusService) Stop() {
    srv.Shutdown(nil)
}

func (s *PrometheusService) mock() {
    go func() {
        for {
            v := rand.NormFloat64()
            log.Logger.Infof("cost time :%v", v)
            SyncOnceDurationsHistogram.Observe(v)
            time.Sleep(700 * time.Millisecond)
        }
    }()
}

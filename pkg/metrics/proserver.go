package metrics

import (
	"log"
	"math/rand"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"time"
	_ "net/http/pprof"
)

type PrometheusService struct {}

var srv *http.Server

func NewPrometheusServer() *PrometheusService{
	return &PrometheusService{}
}

func (s *PrometheusService) Start() {
	//s.mock()
	srv = &http.Server{Addr: ":8090"}
	log.Println("prometheus server start ...")
	http.Handle("/metrics",promhttp.Handler())
	log.Fatal(srv.ListenAndServe())
}

func (s *PrometheusService) Stop() {
	srv.Shutdown(nil)
}

func (s *PrometheusService) mock() {
	go func() {
		for {
			v := rand.NormFloat64()
			log.Printf("cost time :%v",v)
			SyncOnceDurationsHistogram.Observe(v)
			time.Sleep(700 * time.Millisecond)
		}
	}()
}

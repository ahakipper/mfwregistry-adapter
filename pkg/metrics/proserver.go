package metrics

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	legacycompat "spotter/internal/infra/legacycompat"
	"spotter/internal/ports"
	"time"
)

type PrometheusService struct {
	Addr   string
	Logger ports.Logger
}

var srv *http.Server

func NewPrometheusServer(addr string) *PrometheusService {
	return &PrometheusService{
		Addr:   addr,
		Logger: legacycompat.Logger(),
	}
}

// NewPrometheusServerWithLogger is the explicit dependency-injection seam.
// NewPrometheusServer remains a compatibility wrapper for legacy callers.
func NewPrometheusServerWithLogger(addr string, logger ports.Logger) *PrometheusService {
	if logger == nil {
		logger = ports.NopLogger{}
	}
	return &PrometheusService{Addr: addr, Logger: logger}
}

func (s *PrometheusService) Start() {
	if s == nil {
		return
	}
	if s.Logger == nil {
		s.Logger = ports.NopLogger{}
	}
	//s.mock()
	srv = &http.Server{Addr: s.Addr}
	s.Logger.Info("prometheus server start ...")
	http.Handle("/metrics", promhttp.Handler())
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.Logger.Errorf("prometheus server stopped: %s", err)
	}
}

func (s *PrometheusService) Stop() {
	if srv != nil {
		_ = srv.Shutdown(nil)
	}
}

func (s *PrometheusService) mock() {
	go func() {
		for {
			v := rand.NormFloat64()
			if s.Logger != nil {
				s.Logger.Infof("cost time :%v", v)
			}
			SyncOnceDurationsHistogram.Observe(v)
			time.Sleep(700 * time.Millisecond)
		}
	}()
}

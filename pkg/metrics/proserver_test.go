package metrics

import (
	"net"
	"net/http"
	"sync"
	"testing"
)

func TestPrometheusServiceStopBeforeStartIsNilSafe(t *testing.T) {
	service := NewPrometheusServerWithLogger("127.0.0.1:0", nil)
	service.Stop()
	service.Stop()
}

func TestPrometheusServiceStopIsConcurrentAndIdempotent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.NewServeMux()}
	serveDone := make(chan struct{})
	go func() {
		_ = server.Serve(listener)
		close(serveDone)
	}()

	service := NewPrometheusServerWithLogger(listener.Addr().String(), nil)
	service.mu.Lock()
	service.srv = server
	service.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); service.Stop() }()
	}
	wg.Wait()
	select {
	case <-serveDone:
	default:
		t.Fatal("Prometheus server is still serving after concurrent Stop calls")
	}
	service.Stop()
}

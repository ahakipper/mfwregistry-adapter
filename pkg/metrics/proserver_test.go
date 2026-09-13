package metrics

import (
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
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
	case <-time.After(time.Second):
		t.Fatal("Prometheus server is still serving after concurrent Stop calls")
	}
	service.Stop()
}

func TestPrometheusServiceConcurrentInstancesRegisterMetricsOnce(t *testing.T) {
	one := NewPrometheusServerWithLogger("127.0.0.1:0", nil)
	two := NewPrometheusServerWithLogger("127.0.0.1:0", nil)
	firstDone := make(chan struct{})
	secondDone := make(chan struct{})
	go func() { one.Start(); close(firstDone) }()
	go func() { two.Start(); close(secondDone) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		one.mu.Lock()
		oneStarted := one.srv != nil
		one.mu.Unlock()
		two.mu.Lock()
		twoStarted := two.srv != nil
		two.mu.Unlock()
		if oneStarted && twoStarted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	one.Stop()
	two.Stop()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Prometheus server did not stop")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second Prometheus server did not stop")
	}
}

//go:build observe
// +build observe

package observe

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDockerErrorClassificationDistinguishesNotFound(t *testing.T) {
	if !isContainerNotFound(errors.New("docker inspect failed"), "Error: No such object: dsca-observe-nacos") {
		t.Fatal("No such object was not classified as container-not-found")
	}
	if !isContainerNotFound(errors.New("docker image inspect failed"), "Error: No such image: nacos/nacos-server:v2.1.0") {
		t.Fatal("No such image was not classified as image-not-found")
	}
	if isContainerNotFound(errors.New("context deadline exceeded"), "daemon unavailable") {
		t.Fatal("daemon error was misclassified as container-not-found")
	}
}

func TestNacosPortMappings(t *testing.T) {
	p, err := nacosPortMappings(28848)
	if err != nil || p.http != 28848 || p.grpc != 29848 || p.control != 29849 {
		t.Fatalf("mapping=%+v err=%v", p, err)
	}
	for _, port := range []int{0, 1023, 64535, 65535} {
		if _, err := nacosPortMappings(port); err == nil {
			t.Fatalf("port %d accepted", port)
		}
	}
}

func TestObserveCommandHelpersHonorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if _, err := runCommandContext(ctx, "sleep", "30"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("runCommandContext error = %v, want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("canceled command took %s", elapsed)
	}

	driver := newChurnDriver("/nonexistent", []string{"obs-app-0"}, "obs")
	if _, err := driver.kubectlStdinContext(ctx, "", "version"); err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("kubectlStdinContext error = %v, want context canceled", err)
	}
}

func TestObserveHealthHelpersRejectInvalidBoundsAndCancellation(t *testing.T) {
	child := &spotterChild{}
	if err := child.waitForHealthy(0); err == nil {
		t.Fatal("waitForHealthy(0) returned nil")
	}
	if err := nacosHealthWait("127.0.0.1:1", 0); err == nil {
		t.Fatal("nacosHealthWait(0) returned nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := nacosHealthWaitContext(ctx, "127.0.0.1:1"); err == nil {
		t.Fatal("canceled nacosHealthWaitContext returned nil")
	}
}

func TestObserveChildKillIsIdempotent(t *testing.T) {
	child := &spotterChild{}
	child.kill()
	child.kill()
}

func TestChurnApplyBatchRejectsInvalidInputs(t *testing.T) {
	driver := newChurnDriver("/tmp/kubeconfig", nil, "obs")
	if _, err := driver.applyBatch(1, 1, time.Now()); err == nil {
		t.Fatal("applyBatch with no app codes returned nil")
	}
	driver = newChurnDriver("/tmp/kubeconfig", []string{"obs-app-0"}, "obs")
	if _, err := driver.applyBatch(1, 0, time.Now()); err == nil {
		t.Fatal("applyBatch with zero batch size returned nil")
	}
}

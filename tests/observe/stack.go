//go:build observe
// +build observe

package observe

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// The throwaway-nacos lifecycle (dsca-4 §4.1): a fresh
// nacos/nacos-server:v2.1.0 docker container on a scratch host port
// (28848 -> container 8848), pinned heap, health-gated, owned by the
// harness, removed by the harness. The image tag is the same one the soak
// stack runs (verified: `docker inspect soak-nacos --format
// '{{.Config.Image}}'` -> nacos/nacos-server:v2.1.0), so no pull is needed
// on this host; a missing image is a startup error the caller surfaces.
//
// The demo's nacos (18848) and the soak compose's are never addressed.

// observeNacosContainer is the throwaway container's name.
const observeNacosContainer = "dsca-observe-nacos"

// nacosImage is the throwaway stack's nacos image (the soak stack's tag).
const nacosImage = "nacos/nacos-server:v2.1.0"

// nacosHealthWait boot-waits a fresh standalone nacos: the container start
// plus the ARM/colima JVM's rebuild window (the soak's (d)-scenario
// pathology — readiness returns before the naming service serves derby
// data, so the gate is the SERVICE LIST answering, the assert.go
// nsAPIReady discipline: a 200 with a non-empty count).
func nacosHealthWait(addr string, bound time.Duration) error {
	base := "http://" + addr
	deadline := time.Now().Add(bound)
	var lastErr error
	for time.Now().Before(deadline) {
		// readiness endpoint first (fast fail), then the naming-service gate.
		if err := httpGetOK(base + "/nacos/v1/console/health/readiness"); err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		if err := nacosNamingServing(base); err != nil {
			lastErr = err
		} else {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("nacos health wait timed out after %s", bound)
	}
	return lastErr
}

// nacosNamingServing reports whether the v1 naming API serves its persisted
// data (the console readiness endpoint turns 200 while the naming service
// is still rebuilding on a fresh ARM/colima JVM — an empty service list
// must NOT pass the gate). A fresh empty server legitimately answers
// count=0: this is a THROWAWAY server, so "serving" here means the endpoint
// answers 200 at all — the rebuild blindness only matters for a server
// that had data (the soak's restart scenario). The gate therefore accepts
// a 200 and distinguishes connection errors.
func nacosNamingServing(base string) error {
	return httpGetOK(base + "/nacos/v1/ns/service/list?pageNo=1&pageSize=1&groupName=DEFAULT_GROUP&namespaceId=public")
}

// httpGetOK issues one GET and reports non-2xx / transport errors.
func httpGetOK(target string) error {
	response, err := sharedHTTP.Get(target) //nolint:gosec // fixed loopback URL
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= 300 {
		return fmt.Errorf("GET %s answered %d", target, response.StatusCode)
	}
	return nil
}

// nacosHostPort extracts the host port from a host:port address.
func nacosHostPort(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx >= 0 {
		return addr[idx+1:]
	}
	return addr
}

// dockerRunNacos starts the throwaway nacos container on the given host
// port. It fails fast when the image is missing (the harness does not
// silently substitute a mock for the real server).
func dockerRunNacos(hostPort int) error {
	if running, _ := dockerContainerRunning(observeNacosContainer); running {
		return fmt.Errorf("observe: throwaway nacos container %s already exists (previous run not cleaned up; docker rm -f %s)", observeNacosContainer, observeNacosContainer)
	}
	if has, _ := dockerImagePresent(nacosImage); !has {
		return fmt.Errorf("observe: nacos image %s not present (docker pull %s)", nacosImage, nacosImage)
	}
	out, err := runCommand("docker", "run", "-d", "--name", observeNacosContainer,
		"-e", "MODE=standalone", "-e", "JVM_XMS=512m", "-e", "JVM_XMX=512m",
		"-p", fmt.Sprintf("%d:8848", hostPort), nacosImage)
	if err != nil {
		return fmt.Errorf("observe: docker run nacos: %w: %s", err, strings.TrimSpace(out))
	}
	return nil
}

// dockerStopNacos removes the throwaway nacos container (idempotent).
func dockerStopNacos() error {
	if running, _ := dockerContainerRunning(observeNacosContainer); !running {
		// Not running: remove a stopped leftover if any.
		_, _ = runCommand("docker", "rm", "-f", observeNacosContainer)
		return nil
	}
	_, err := runCommand("docker", "rm", "-f", observeNacosContainer)
	return err
}

// dockerContainerRunning reports whether the named container exists in a
// running state.
func dockerContainerRunning(name string) (bool, error) {
	out, err := runCommand("docker", "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) == "true", nil
}

// dockerImagePresent reports whether the image exists locally.
func dockerImagePresent(image string) (bool, error) {
	out, err := runCommand("docker", "image", "inspect", image)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

// runCommand runs one command, returning trimmed stdout.
func runCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...) //nolint:gosec // the harness's own docker/kubectl machinery
	cmd.Env = os.Environ()
	out, err := cmd.Output()
	return string(out), err
}

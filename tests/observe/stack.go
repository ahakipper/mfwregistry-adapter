//go:build observe
// +build observe

package observe

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// The throwaway-Nacos lifecycle (dsca-4 §4.1): a fresh
// nacos/nacos-server:v3.2.4-slim docker container on a scratch host port
// (28848 -> container 8848), pinned heap, health-gated, owned by the
// harness, removed by the harness. The image is pinned to the verified
// ARM64 digest below; a missing image is a startup error the caller surfaces.
// Observe readiness uses the Nacos 3 naming gRPC listener, not the removed
// Nacos 2 /nacos/v1 HTTP endpoints.
//
// The demo's nacos (18848) and the soak compose's are never addressed.

// observeNacosContainer is the throwaway container's name.
const observeNacosContainer = "dsca-observe-nacos"

// nacosImage is the target Observe image. The digest is the ARM64
// linux/arm64 manifest verified on the local Apple Silicon host.
const nacosImage = "nacos/nacos-server:v3.2.4-slim"
const nacosImageDigest = "sha256:2a6d445d567b04c81404a3569309b07bfaf077216dbc3a92c0f56c9113034fb5"
const nacosPlatform = "linux/arm64"

// nacosHealthWait boot-waits a fresh standalone Nacos 3 instance. The gate is
// the naming gRPC listener (container 9848, host port +1000), not a Nacos 2
// /nacos/v1 HTTP endpoint. SDK-level read/write readiness is performed after
// this transport gate by the observe view.
func nacosHealthWait(addr string, bound time.Duration) error {
	if bound <= 0 {
		return fmt.Errorf("nacos health wait bound must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	return nacosHealthWaitContext(ctx, addr)
}

func nacosHealthWaitContext(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var lastErr error
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := nacosNamingServingContext(ctx, addr); err != nil {
			lastErr = err
		} else {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return fmt.Errorf("nacos health wait timed out: %w (last error: %v)", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

// nacosNamingServing reports whether the Nacos 3 naming gRPC listener is
// accepting connections. It is intentionally only a transport gate; callers
// must use the official SDK for naming reads and writes.
func nacosNamingServing(addr string) error {
	return nacosNamingServingContext(context.Background(), addr)
}

func nacosNamingServingContext(ctx context.Context, addr string) error {
	endpoint, err := nacosGRPCEndpoint(addr)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return fmt.Errorf("nacos naming gRPC %s: %w", endpoint, err)
	}
	return conn.Close()
}

// nacosGRPCEndpoint maps the configured Nacos HTTP port to the Nacos 3
// naming gRPC port. Nacos's documented default is HTTP+1000 (8848→9848).
func nacosGRPCEndpoint(addr string) (string, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("nacos address %q must be host:port: %w", addr, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 64535 {
		return "", fmt.Errorf("nacos address %q has invalid HTTP port", addr)
	}
	return net.JoinHostPort(host, strconv.Itoa(port+1000)), nil
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
	ports, err := nacosPortMappings(hostPort)
	if err != nil {
		return err
	}
	image := nacosImage
	if override := strings.TrimSpace(os.Getenv("OBS_NACOS_IMAGE")); override != "" {
		image = override
	}
	running, inspectErr := dockerContainerRunning(observeNacosContainer)
	if inspectErr != nil {
		return fmt.Errorf("observe: docker inspect before run: %w", inspectErr)
	}
	if running {
		return fmt.Errorf("observe: throwaway nacos container %s already exists (previous run not cleaned up; docker rm -f %s)", observeNacosContainer, observeNacosContainer)
	}
	has, imageErr := dockerImagePresent(image)
	if imageErr != nil {
		return fmt.Errorf("observe: docker image inspect: %w", imageErr)
	}
	if !has {
		return fmt.Errorf("observe: nacos image %s not present (docker pull %s)", image, image)
	}
	canonical, err := validateNacosImage(image)
	if err != nil {
		return err
	}
	out, err := runCommand("docker", "run", "-d", "--name", observeNacosContainer,
		"--platform", nacosPlatform,
		"-e", "MODE=standalone", "-e", "NACOS_AUTH_ENABLE=false",
		"-e", "NACOS_AUTH_IDENTITY_KEY=spotter-observe", "-e", "NACOS_AUTH_IDENTITY_VALUE=spotter-observe",
		"-e", "NACOS_AUTH_TOKEN=c3BvdHRlci1vYnNlcnZlLW5hY29zMy10b2tlbi0yMDI2MDkxNA==",
		"-e", "JVM_XMS=512m", "-e", "JVM_XMX=512m",
		"-p", fmt.Sprintf("%d:8848", hostPort), "-p", fmt.Sprintf("%d:9848", ports.grpc), "-p", fmt.Sprintf("%d:9849", ports.control), canonical)
	if err != nil {
		return fmt.Errorf("observe: docker run nacos: %w: %s", err, strings.TrimSpace(out))
	}
	return nil
}

type nacosPorts struct{ http, grpc, control int }

func nacosPortMappings(hostPort int) (nacosPorts, error) {
	if hostPort < 1024 || hostPort > 64534 {
		return nacosPorts{}, fmt.Errorf("observe: EnvError invalid Nacos host port %d", hostPort)
	}
	return nacosPorts{http: hostPort, grpc: hostPort + 1000, control: hostPort + 1001}, nil
}

func validateNacosImage(image string) (string, error) {
	out, err := runCommand("docker", "image", "inspect", "--format", "{{.Architecture}} {{.RepoDigests}}", image)
	if err != nil {
		return "", fmt.Errorf("observe: EnvError inspect Nacos image architecture/digest: %w", err)
	}
	parts := strings.Fields(strings.NewReplacer("[", "", "]", "", ",", "").Replace(out))
	if len(parts) < 2 || parts[0] != "arm64" {
		return "", fmt.Errorf("observe: EnvError Nacos image %s must prove arm64 and digest %s (got %q)", image, nacosImageDigest, strings.TrimSpace(out))
	}
	for _, digest := range parts[1:] {
		if strings.HasSuffix(digest, "@"+nacosImageDigest) {
			return digest, nil
		}
	}
	return "", fmt.Errorf("observe: EnvError Nacos image %s missing digest %s", image, nacosImageDigest)
}

// dockerStopNacos removes the throwaway nacos container (idempotent).
func dockerStopNacos() error {
	running, inspectErr := dockerContainerRunning(observeNacosContainer)
	if inspectErr != nil {
		return fmt.Errorf("observe: docker inspect during teardown (residual_unknown=true): %w", inspectErr)
	}
	if !running {
		// Not running: remove a stopped leftover if any.
		out, err := runCommand("docker", "rm", "-f", observeNacosContainer)
		if err != nil && !isContainerNotFound(err, out) {
			return fmt.Errorf("observe: docker rm stopped Nacos container (residual_unknown=true): %w", err)
		}
	} else {
		if out, err := runCommand("docker", "rm", "-f", observeNacosContainer); err != nil {
			return fmt.Errorf("observe: docker rm Nacos container (residual_unknown=true): %w", err)
		} else {
			_ = out
		}
	}
	exists, err := dockerContainerExists(observeNacosContainer)
	if err != nil {
		return fmt.Errorf("observe: docker residual inspect (residual_unknown=true): %w", err)
	}
	if exists {
		return fmt.Errorf("observe: Nacos container still exists after rm (residual_unknown=true)")
	}
	return nil
}

// dockerContainerRunning reports whether the named container exists in a
// running state.
func dockerContainerRunning(name string) (bool, error) {
	out, err := runCommand("docker", "inspect", "--format", "{{.State.Running}}", name)
	if err != nil {
		if isContainerNotFound(err, out) {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(out) == "true", nil
}

func dockerContainerExists(name string) (bool, error) {
	out, err := runCommand("docker", "inspect", "--format", "{{.Id}}", name)
	if err != nil {
		if isContainerNotFound(err, out) {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

// dockerImagePresent reports whether the image exists locally.
func dockerImagePresent(image string) (bool, error) {
	out, err := runCommand("docker", "image", "inspect", image)
	if err != nil {
		if isContainerNotFound(err, out) {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

// runCommand runs one command, returning trimmed stdout.
func runCommand(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), observeCommandTimeout)
	defer cancel()
	return runCommandContext(ctx, name, args...)
}

func runCommandContext(ctx context.Context, name string, args ...string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // the harness's own docker/kubectl machinery
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil && ctx.Err() != nil {
		return string(out), fmt.Errorf("%s timed out: %w", name, ctx.Err())
	}
	if err != nil {
		return string(out), fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(out)))
	}
	return string(out), err
}

func isContainerNotFound(err error, output string) bool {
	text := strings.ToLower(errString(err) + " " + output)
	return strings.Contains(text, "no such container") || strings.Contains(text, "no such object") || strings.Contains(text, "no such image")
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

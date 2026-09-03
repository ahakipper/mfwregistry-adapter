// Package etcdmock provides an in-process embedded etcd server for tests.
//
// It is the etcd equivalent of consulmock/discoverymock: nothing in
// production imports this package, so the embedded server (and its
// go.etcd.io/etcd/server/v3 dependency) never reaches the production
// binary.
package etcdmock

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

const (
	// defaultName is the embedded member name used when WithName is not
	// supplied.
	defaultName = "etcdmock"

	// defaultLogLevel keeps the embedded server quiet in test output; the
	// server still logs genuine errors.
	defaultLogLevel = "error"

	// readyTimeout bounds how long Start waits for the single-member raft
	// to elect itself and open the client endpoint. A fresh member on a
	// loopback port is normally ready in well under a second; the bound
	// only guards pathological environments.
	readyTimeout = 10 * time.Second
)

// Server wraps one embedded etcd member listening on loopback-only dynamic
// ports. All exported methods are safe for concurrent use, and Close is
// idempotent.
type Server struct {
	mu sync.Mutex

	server *embed.Etcd

	// dir is the member's data directory; Close removes it.
	dir string

	// clientEndpoints holds the advertise client URLs in the exact form
	// clientv3.Config.Endpoints accepts.
	clientEndpoints []string

	closeOnce sync.Once
}

// Option configures Start.
type Option func(*options)

// WithName sets the embedded member name (also its raft member identity).
// The empty string keeps the default.
func WithName(name string) Option {
	return func(o *options) {
		if name != "" {
			o.name = name
		}
	}
}

// WithLogLevel sets the embedded server log level (debug, info, warn,
// error). The empty string keeps the default.
func WithLogLevel(level string) Option {
	return func(o *options) {
		if level != "" {
			o.logLevel = level
		}
	}
}

type options struct {
	name     string
	logLevel string
}

// Start boots ONE embedded etcd member on dynamically allocated loopback
// ports (client and peer get distinct ports) with a unique temporary data
// directory. It blocks until the member is ready to serve client requests
// and fails fast (cleaning up after itself) on boot or readiness failure.
//
// Callers must eventually call Close, which stops the server AND removes
// the data directory.
func Start(opts ...Option) (*Server, error) {
	o := &options{name: defaultName, logLevel: defaultLogLevel}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}

	clientURL, peerURL, err := freeLoopbackURLs()
	if err != nil {
		return nil, err
	}

	dir, err := os.MkdirTemp("", "etcdmock-")
	if err != nil {
		return nil, fmt.Errorf("etcdmock: create data dir: %w", err)
	}

	// The embed defaults are kept deliberately: TickMs=100 and
	// ElectionMs=1000 give a single-member cluster a ~1s election and clamp
	// granted lease TTLs to MinLeaseTTL = ceil(1.5s) = 2s, which keeps
	// lease-expiry failover fast without any non-default raft tuning.
	cfg := embed.NewConfig()
	cfg.Name = o.name
	cfg.Dir = dir
	cfg.LogLevel = o.logLevel
	// The server unconditionally logs benign error-level lines when it is
	// closed ("http: Server closed", "use of closed network connection"),
	// which pollutes test output. Routing the server log into the data
	// directory keeps the console clean AND removes the log together with
	// the data dir on Close.
	cfg.LogOutputs = []string{filepath.Join(dir, "embed.log")}
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, peerURL.String())

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		// StartEtcd closes the partially started server itself on error,
		// leaving only the directory behind.
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("etcdmock: start embedded etcd: %w", err)
	}

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(readyTimeout):
		e.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("etcdmock: embedded etcd not ready within %s", readyTimeout)
	}

	return &Server{
		server:          e,
		dir:             dir,
		clientEndpoints: []string{clientURL.String()},
	}, nil
}

// ClientEndpoints returns the advertise client URLs of the embedded member,
// ready to pass as clientv3.Config.Endpoints.
func (s *Server) ClientEndpoints() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.clientEndpoints...)
}

// Dir returns the member's data directory path. Close removes it, so after
// Close the path no longer exists.
func (s *Server) Dir() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir
}

// Close stops the embedded server and removes its data directory. It is
// safe to call any number of times and from multiple goroutines; every call
// after the first is a no-op.
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		server := s.server
		dir := s.dir
		s.mu.Unlock()

		if server != nil {
			// Etcd.Close is itself idempotent and blocks until every
			// server goroutine has exited.
			server.Close()
		}
		if dir != "" {
			_ = os.RemoveAll(dir)
		}
	})
}

// freeLoopbackURLs picks two free loopback TCP ports (one for the client
// listener, one for the peer listener) by briefly binding port 0 and
// reusing the assigned port numbers. The ports must differ, which the
// retry guarantees.
func freeLoopbackURLs() (client, peer url.URL, err error) {
	for attempt := 0; attempt < 8; attempt++ {
		client, err = freeLoopbackURL()
		if err != nil {
			return url.URL{}, url.URL{}, err
		}
		peer, err = freeLoopbackURL()
		if err != nil {
			return url.URL{}, url.URL{}, err
		}
		if client.Host != peer.Host {
			return client, peer, nil
		}
	}
	return url.URL{}, url.URL{}, errors.New("etcdmock: could not allocate two distinct loopback ports")
}

func freeLoopbackURL() (url.URL, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return url.URL{}, fmt.Errorf("etcdmock: reserve loopback port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return url.URL{}, fmt.Errorf("etcdmock: release reserved port: %w", err)
	}
	return url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", port)}, nil
}

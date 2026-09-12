// Package notice wires the appcenter notice client to the ports.Notifier
// interface.
//
// Notify mirrors pkg/notice.Notice: it sends an EMERGENCY text notice
// asynchronously (in a goroutine) and logs send failures through an injected
// logger instead of the pkg/log global. LocalIP mirrors pkg/notice.GetLocalIP.
package notice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"spotter/internal/ports"
	"spotter/pkg/notice/appcenternotice"
)

// Message is the transport-neutral notice contract.  The adapter deliberately
// does not prescribe an appcenter request body: the production appcenter API
// contract is deployment-owned and is not part of this repository.
type Message struct {
	AppCode string
	Env     string
	Title   string
	Content string
	Level   string
	Type    string
}

// RequestBuilder translates a Message into the deployment's appcenter HTTP
// request.  Keeping this callback explicit prevents this package from
// inventing a private payload schema while still providing timeout, retry,
// authentication and observability around the actual request.
type RequestBuilder func(ctx context.Context, endpoint string, message Message) (*http.Request, error)

// HTTPConfig configures the appcenter HTTP transport.  Endpoint, one of the
// authentication values and a finite timeout are mandatory.  A request
// builder is mandatory because the private appcenter payload contract is not
// known to this repository.
type HTTPConfig struct {
	Endpoint     string
	AppCode      string
	AuthToken    string
	AuthKey      string
	Env          string
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
	BuildRequest RequestBuilder
}

// HTTPNotifier is a production transport adapter.  Notify preserves the
// historical non-blocking port while send failures are counted and logged.
type HTTPNotifier struct {
	cfg       HTTPConfig
	client    *http.Client
	logger    ports.Logger
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup
	succeeded atomic.Uint64
	failed    atomic.Uint64
}

var _ ports.Notifier = (*HTTPNotifier)(nil)

// NewHTTP constructs an appcenter notifier only when the complete transport
// contract is available.  Callers should use NewFailClosed when the endpoint
// is not configured rather than silently falling back to log-only delivery.
func NewHTTP(cfg HTTPConfig, logger ports.Logger) (*HTTPNotifier, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("appcenter endpoint is required")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("invalid appcenter endpoint %q", cfg.Endpoint)
	}
	if strings.TrimSpace(cfg.AuthToken) == "" && strings.TrimSpace(cfg.AuthKey) == "" {
		return nil, errors.New("appcenter authentication is required")
	}
	if cfg.Timeout <= 0 {
		return nil, errors.New("appcenter timeout must be positive")
	}
	if cfg.MaxRetries < 0 {
		return nil, errors.New("appcenter max retries cannot be negative")
	}
	if cfg.RetryBackoff < 0 {
		return nil, errors.New("appcenter retry backoff cannot be negative")
	}
	if cfg.BuildRequest == nil {
		return nil, errors.New("appcenter request builder is required; payload schema is deployment-owned")
	}
	if logger == nil {
		logger = ports.NopLogger{}
	}
	return &HTTPNotifier{cfg: cfg, client: &http.Client{}, logger: logger}, nil
}

// Notify submits a notice asynchronously.  Every failure increments the
// failure counter and is emitted through the injected logger; no failure is
// converted into a false success.
func (n *HTTPNotifier) Notify(title, content string) {
	if n == nil {
		return
	}
	message := Message{AppCode: n.cfg.AppCode, Env: n.cfg.Env, Title: title, Content: content, Level: "emergency", Type: "text"}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	n.wg.Add(1)
	n.mu.Unlock()
	go func() {
		defer n.wg.Done()
		if err := n.send(message); err != nil {
			n.failed.Add(1)
			n.logger.Errorf("appcenter notice delivery failed: %s", err)
			return
		}
		n.succeeded.Add(1)
	}()
}

// Close prevents new asynchronous deliveries and waits for all notices
// already accepted by Notify to finish. The bounded request timeout makes
// this safe for process shutdown while preserving the non-blocking Notify
// port for callers.
func (n *HTTPNotifier) Close() error {
	if n == nil {
		return nil
	}
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	n.wg.Wait()
	return nil
}

// FailureCount and SuccessCount expose delivery observability without
// coupling callers to a metrics backend.
func (n *HTTPNotifier) FailureCount() uint64 {
	if n == nil {
		return 0
	}
	return n.failed.Load()
}
func (n *HTTPNotifier) SuccessCount() uint64 {
	if n == nil {
		return 0
	}
	return n.succeeded.Load()
}

func (n *HTTPNotifier) send(message Message) error {
	var lastErr error
	for attempt := 0; attempt <= n.cfg.MaxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.Timeout)
		req, err := n.cfg.BuildRequest(ctx, n.cfg.Endpoint, message)
		if err == nil && req != nil {
			req = req.WithContext(ctx)
			if n.cfg.AuthToken != "" {
				req.Header.Set("Authorization", "Bearer "+n.cfg.AuthToken)
			}
			if n.cfg.AuthKey != "" {
				req.Header.Set("X-Appcenter-Key", n.cfg.AuthKey)
			}
			resp, doErr := n.client.Do(req)
			if doErr == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					cancel()
					return nil
				}
				lastErr = fmt.Errorf("appcenter returned HTTP %d", resp.StatusCode)
				if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
					cancel()
					return lastErr
				}
			} else {
				lastErr = doErr
			}
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = errors.New("appcenter request builder returned nil request")
		}
		cancel()
		if attempt < n.cfg.MaxRetries && n.cfg.RetryBackoff > 0 {
			timer := time.NewTimer(n.cfg.RetryBackoff)
			<-timer.C
		}
	}
	return lastErr
}

// FailClosedNotifier makes missing deployment credentials visible while
// keeping nil-safe lifecycle behavior.  It never claims that a notice was
// delivered; composition should replace it with HTTPNotifier once the
// endpoint contract is supplied.
type FailClosedNotifier struct {
	reason string
	logger ports.Logger
	failed atomic.Uint64
}

var _ ports.Notifier = (*FailClosedNotifier)(nil)

func NewFailClosed(reason string, logger ports.Logger) *FailClosedNotifier {
	if logger == nil {
		logger = ports.NopLogger{}
	}
	if strings.TrimSpace(reason) == "" {
		reason = "appcenter notifier is not configured"
	}
	return &FailClosedNotifier{reason: reason, logger: logger}
}

func (n *FailClosedNotifier) Notify(title, content string) {
	if n == nil {
		return
	}
	n.failed.Add(1)
	// Do not log title/content: notification payloads may contain credentials,
	// instance metadata, or other sensitive data. The failure counter and
	// reason are sufficient for operator action.
	n.logger.Errorf("appcenter notice not delivered (fail-closed): %s", n.reason)
}

func (n *FailClosedNotifier) FailureCount() uint64 {
	if n == nil {
		return 0
	}
	return n.failed.Load()
}

// notifier sends notices through the local appcenternotice client.
type notifier struct {
	noticer *appcenternotice.Noticer
	logger  ports.Logger
}

// Compile-time assertion that notifier satisfies the port.
var _ ports.Notifier = (*notifier)(nil)

// New returns a ports.Notifier that sends notices for the given app code,
// key and environment. Send failures are logged to a no-op logger; use
// NewWithLogger to make them observable.
func New(appCode, key, env string) ports.Notifier {
	return NewWithLogger(appCode, key, env, nil)
}

// NewWithLogger returns a ports.Notifier like New, logging send failures
// through logger. A nil logger falls back to ports.NopLogger so the
// returned notifier never dereferences a nil logger.
//
// Note: the underlying appcenternotice.Noticer writes delivered notices
// (and only those) to the pkg/log global logger when it is initialized;
// this cannot be injected without modifying the existing package. When the
// global logger is nil (for example in unit tests), appcenternotice skips
// that step, so NewWithLogger with a fake logger stays fully offline.
func NewWithLogger(appCode, key, env string, logger ports.Logger) ports.Notifier {
	if logger == nil {
		logger = ports.NopLogger{}
	}
	noticer := appcenternotice.NewNoticer()
	noticer = noticer.WithAppCode(appCode).WithKey(key).WithEnv(env)
	return &notifier{
		noticer: noticer,
		logger:  logger,
	}
}

// Notify sends title/content as an EMERGENCY text notice asynchronously,
// exactly like pkg/notice.Notice: the send happens in a goroutine and the
// caller never blocks; a send error is logged via the injected logger.
func (n *notifier) Notify(title, content string) {
	messageLevel := appcenternotice.MESSAGE_LEVEL_EMERGENCY
	messageType := appcenternotice.MESSAGE_TYPE_TEXT
	go func() {
		err := n.noticer.SendNotice(title, content, messageLevel, messageType)
		if err != nil {
			n.logger.Errorf("noticer send notice error:%s", err)
		}
	}()
}

// LocalIP returns the current node IP, exactly like
// pkg/notice.GetLocalIP: the first non-loopback, globally unicast address
// of the network interfaces, or an error when the interfaces cannot be
// listed.
func LocalIP() (string, error) {
	ip, err := getLocalIP()
	return ip, err
}

// getLocalIP is a copy of the GetLocalIP logic in pkg/notice/notice.go.
func getLocalIP() (ip string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range addrs {
		ipAddr, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ipAddr.IP.IsLoopback() {
			continue
		}
		if !ipAddr.IP.IsGlobalUnicast() {
			continue
		}
		return ipAddr.IP.String(), nil
	}
	return
}

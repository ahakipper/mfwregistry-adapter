// Package notice provides the fail-closed notifier and local-IP resolver used
// by the composition root. External HTTP delivery is intentionally
// absent from the current Spotter release.
package notice

import (
	"net"
	"strings"
	"sync/atomic"

	"spotter/internal/ports"
)

// FailClosedNotifier records attempted notices without claiming external
// delivery. It is the default operational-notice behavior for this release;
// callers may still inject another ports.Notifier explicitly.
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
		reason = "external notice delivery is not configured"
	}
	return &FailClosedNotifier{reason: reason, logger: logger}
}

func (n *FailClosedNotifier) Notify(title, content string) {
	if n == nil {
		return
	}
	n.failed.Add(1)
	// Do not log title/content: notification payloads may contain credentials,
	// instance metadata, or other sensitive data.
	n.logger.Errorf("notice not delivered (fail-closed): %s", n.reason)
}

func (n *FailClosedNotifier) FailureCount() uint64 {
	if n == nil {
		return 0
	}
	return n.failed.Load()
}

// LocalIP returns the first non-loopback, globally unicast address of the
// network interfaces, or an error when the interfaces cannot be listed.
func LocalIP() (string, error) { return getLocalIP() }

func getLocalIP() (ip string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		ipAddr, ok := addr.(*net.IPNet)
		if !ok || ipAddr.IP.IsLoopback() || !ipAddr.IP.IsGlobalUnicast() {
			continue
		}
		return ipAddr.IP.String(), nil
	}
	return "", nil
}

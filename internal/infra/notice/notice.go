// Package notice wires the appcenter notice client to the ports.Notifier
// interface.
//
// Notify mirrors pkg/notice.Notice: it sends an EMERGENCY text notice
// asynchronously (in a goroutine) and logs send failures through an injected
// logger instead of the pkg/log global. LocalIP mirrors pkg/notice.GetLocalIP.
package notice

import (
	"net"

	"spotter/internal/ports"
	"spotter/pkg/notice/appcenternotice"
)

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

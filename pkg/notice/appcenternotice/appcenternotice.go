// Package appcenternotice is a local, self-contained replacement of the former
// private "appcenter-notice/client" module.
//
// It mirrors the surface of the original notice client (Noticer with the
// chainable With* builders and SendNotice) but, instead of calling the internal
// appcenter-notice HTTP service, it writes the notice to the application logger
// and returns nil. This keeps the public API of pkg/notice unchanged while
// removing the dependency on the unreachable private module.
package appcenternotice

import (
	"github.com/ahakipper/spotter/pkg/log"
)

// Level is the severity level of a notice message.
type Level string

// Supported notice levels. MESSAGE_LEVEL_EMERGENCY matches the constant of the
// original appcenter-notice client.
const (
	MESSAGE_LEVEL_EMERGENCY Level = "emergency"
	MESSAGE_LEVEL_NORMAL    Level = "normal"
	MESSAGE_LEVEL_INFO      Level = "info"
)

// Type is the format of a notice message.
type Type string

// Supported notice types. MESSAGE_TYPE_TEXT matches the constant of the
// original appcenter-notice client.
const (
	MESSAGE_TYPE_TEXT     Type = "text"
	MESSAGE_TYPE_MARKDOWN Type = "markdown"
)

// Noticer mirrors the appcenter-notice client Noticer. It accumulates the
// appcode / key / env attributes and delivers notices through the local logger.
type Noticer struct {
	appCode string
	key     string
	env     string
}

// NewNoticer creates an empty Noticer, exactly like the original client.
func NewNoticer() *Noticer {
	return &Noticer{}
}

// WithAppCode sets the appcode attribute and returns the Noticer for chaining.
func (n *Noticer) WithAppCode(s string) *Noticer {
	n.appCode = s
	return n
}

// WithKey sets the key attribute and returns the Noticer for chaining.
func (n *Noticer) WithKey(s string) *Noticer {
	n.key = s
	return n
}

// WithEnv sets the env attribute and returns the Noticer for chaining.
func (n *Noticer) WithEnv(s string) *Noticer {
	n.env = s
	return n
}

// SendNotice delivers a notice. In the original client this call POSTed the
// message to the internal appcenter-notice service; this local implementation
// logs the notice (so it is still observable from the application logs) and
// always succeeds. The zap sugar logger is nil-safe enough for our usage, but
// we guard against a nil logger for safety when the notice package is used
// before logger initialization (e.g. in unit tests).
func (n *Noticer) SendNotice(title, content string, level Level, msgType Type) error {
	if n == nil {
		return nil
	}
	if log.Logger != nil {
		log.Logger.Infof("[notice] appcode: %s, env: %s, level: %s, type: %s, title: %s, content: %s",
			n.appCode, n.env, string(level), string(msgType), title, content)
	}
	return nil
}

// Package legacycompat is the only compatibility boundary that may read the
// pre-composition package globals. Production constructors must receive
// explicit internal/ports collaborators instead.
package legacycompat

import (
	"spotter/config"
	"spotter/internal/ports"
	"spotter/pkg/log"
	"spotter/pkg/notice"
)

// Logger returns the legacy logger when initialized, otherwise a nil-safe
// no-op implementation.
func Logger() ports.Logger {
	if log.Logger != nil {
		return log.Logger
	}
	return ports.NopLogger{}
}

type notifier struct{}

func (notifier) Notify(title, content string) {
	if notice.Noticer != nil {
		notice.Notice(title, content)
	}
}

// Notifier adapts the legacy global notice client to the injected port. A
// missing legacy initialization is deliberately a no-op rather than a panic.
func Notifier() ports.Notifier { return notifier{} }

// PushAppCodes copies the legacy filter list for compatibility wrappers.
func PushAppCodes() []string { return append([]string(nil), config.PushAppCodes...) }

// EtcdConfig is the legacy elector configuration snapshot.
func EtcdConfig() (endpoints []string, cert, key, ca, campaign string) {
	return append([]string(nil), config.EtcdEndpoints...), config.CertFile, config.KeyFile, config.CAFile, config.LockCampaignKey
}

// CampaignKey returns the legacy default campaign key for the deprecated
// empty-key constructor path.
func CampaignKey() string { return config.LockCampaignKey }

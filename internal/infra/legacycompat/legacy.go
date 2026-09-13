// Package legacycompat is the only compatibility boundary that may read the
// pre-composition package globals. Production constructors must receive
// explicit internal/ports collaborators instead.
package legacycompat

import (
	"sync/atomic"

	"spotter/config"
	"spotter/internal/ports"
	"spotter/pkg/log"
	"spotter/pkg/notice"
)

// AccessCounts reports compatibility-boundary access activity. The counters
// are intentionally process-local observability for the global migration
// tests; they do not alter legacy values or runtime behavior.
type AccessCounts struct {
	Reads  uint64
	Writes uint64
}

var compatibilityReads atomic.Uint64
var compatibilityWrites atomic.Uint64

// ResetAccessCounts clears the compatibility-boundary counters.
func ResetAccessCounts() {
	compatibilityReads.Store(0)
	compatibilityWrites.Store(0)
}

// AccessCountsSnapshot returns a point-in-time copy of the compatibility
// boundary counters.
func AccessCountsSnapshot() AccessCounts {
	return AccessCounts{
		Reads:  compatibilityReads.Load(),
		Writes: compatibilityWrites.Load(),
	}
}

// RecordWrite records a write performed by a legacy bridge outside this
// package. It exists so the command-side compatibility assignment can be
// observed without exposing or changing the legacy global values.
func RecordWrite() { compatibilityWrites.Add(1) }

func recordRead() { compatibilityReads.Add(1) }

// Logger returns the legacy logger when initialized, otherwise a nil-safe
// no-op implementation.
func Logger() ports.Logger {
	recordRead()
	if log.Logger != nil {
		return log.Logger
	}
	return ports.NopLogger{}
}

type notifier struct{}

func (notifier) Notify(title, content string) {
	recordRead()
	if notice.Noticer != nil {
		notice.Notice(title, content)
	}
}

// Notifier adapts the legacy global notice client to the injected port. A
// missing legacy initialization is deliberately a no-op rather than a panic.
func Notifier() ports.Notifier { return notifier{} }

// PushAppCodes copies the legacy filter list for compatibility wrappers.
func PushAppCodes() []string {
	recordRead()
	return append([]string(nil), config.PushAppCodes...)
}

// EtcdConfig is the legacy elector configuration snapshot.
func EtcdConfig() (endpoints []string, cert, key, ca, campaign string) {
	recordRead()
	endpoints = append([]string(nil), config.EtcdEndpoints...)
	recordRead()
	cert = config.CertFile
	recordRead()
	key = config.KeyFile
	recordRead()
	ca = config.CAFile
	recordRead()
	campaign = config.LockCampaignKey
	return endpoints, cert, key, ca, campaign
}

// CampaignKey returns the legacy default campaign key for the deprecated
// empty-key constructor path.
func CampaignKey() string {
	recordRead()
	return config.LockCampaignKey
}

// Package composition is the composition root of the adapter.
//
// Build turns a fully resolved infra/config.Config into the concrete
// infrastructure objects (logger, notifier, metrics recorder) the rest of
// the application consumes through the internal/ports interfaces. It makes
// no network connections: etcd and discovery-center clients are still
// created during server startup.
package composition

import (
	"fmt"
	"io"

	infraconfig "spotter/internal/infra/config"
	infralogging "spotter/internal/infra/logging"
	inframetrics "spotter/internal/infra/metrics"
	infranotice "spotter/internal/infra/notice"
	"spotter/internal/ports"
)

// Legacy notice identifiers copied from pkg/notice/notice.go
// (InitNoticeClient). They are kept here so the composition root does not
// depend on the legacy global wiring.
const (
	noticeAppCode = "spotter-mtech"
	noticeKey     = "KZ60vWUzdM65ibQCGn03sPF9c1trlIfA"
)

// Deps carries the collaborators injected into the composition root.
//
// All fields are optional: a nil (or zero) field falls back to the built-in
// default, so tests can inject fakes and only override what they need.
type Deps struct {
	// Logger receives the send failures of the notice adapter. When nil,
	// Build falls back to the logger constructed from the config (or a
	// no-op logger when the config requires no log files).
	Logger ports.Logger
	// LogCloser closes a caller-provided log sink. When nil, Build returns
	// the closer of the logger it constructed.
	LogCloser io.Closer
	// Notifier overrides the constructed notice adapter.
	Notifier ports.Notifier
	// MetricsRecorder overrides the constructed metrics recorder.
	Metrics ports.MetricsRecorder
	// Config is the resolved runtime configuration.
	Config infraconfig.Config
	// LocalIP resolves the current node IP for leader-loss notices. When
	// nil, the default infra notice implementation is used.
	LocalIP func() (string, error)
}

// Runtime is the result of the composition root: the object graph handed
// to the server constructor.
type Runtime struct {
	// Logger is the application logger (ports.Logger).
	Logger ports.Logger
	// LogCloser flushes and releases the log sink when Build constructed
	// the logger. It is nil when deps.Logger was injected without a
	// deps.LogCloser — the injected logger then owns its own lifecycle.
	LogCloser io.Closer
	// Notifier sends operational notices.
	Notifier ports.Notifier
	// Metrics records synchronization metrics.
	Metrics ports.MetricsRecorder
	// Config is the resolved runtime configuration.
	Config infraconfig.Config
	// LocalIP resolves the current node IP (never nil).
	LocalIP func() (string, error)
}

// Build constructs the application object graph from cfg and deps.
//
// It builds the zap-based logging adapter from the log settings of cfg
// (when LogFilePath is set, which is always the case for a config produced
// by infra/config.Load), the notice adapter from the legacy app code, key
// and cfg.Env, and the Prometheus metrics recorder. When deps overrides a
// collaborator, the override wins and the corresponding default is not
// constructed.
func Build(cfg infraconfig.Config, deps Deps) (*Runtime, error) {
	runtime := &Runtime{
		Config:  cfg,
		LocalIP: deps.LocalIP,
	}
	if runtime.LocalIP == nil {
		runtime.LocalIP = infranotice.LocalIP
	}

	// Logging: the composition root owns the log sink. Callers that inject
	// both a Logger and a LogCloser keep full control.
	if deps.Logger != nil {
		runtime.Logger = deps.Logger
		runtime.LogCloser = deps.LogCloser
	} else {
		logger, closer, err := infralogging.New(infralogging.Options{
			FilePath:   cfg.LogFilePath,
			MaxSizeMB:  cfg.LogSize,
			MaxBackups: cfg.LogBackups,
			MaxAgeDays: cfg.LogAge,
			Level:      cfg.LogLevel,
			Encoding:   cfg.LogEncoding,
			ToStd:      cfg.LogToStd,
		})
		if err != nil {
			return nil, fmt.Errorf("build logger: %w", err)
		}
		runtime.Logger = logger
		if deps.LogCloser != nil {
			// The constructed sink would leak; close it before switching.
			_ = closer.Close()
			runtime.LogCloser = deps.LogCloser
		} else {
			runtime.LogCloser = closer
		}
	}

	// Notice: send failures are reported through the runtime logger.
	runtime.Notifier = deps.Notifier
	if runtime.Notifier == nil {
		runtime.Notifier = infranotice.NewWithLogger(noticeAppCode, noticeKey, cfg.Env, runtime.Logger)
	}

	// Metrics: the Prometheus recorder observing the package-level
	// collectors; the HTTP endpoint is started by the server.
	runtime.Metrics = deps.Metrics
	if runtime.Metrics == nil {
		runtime.Metrics = inframetrics.New()
	}

	return runtime, nil
}

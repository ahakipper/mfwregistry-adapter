package worker

import (
	"errors"
	"strings"

	"spotter/internal/domain/instance"
)

// This file holds the shared error contract of plan §5.1, introduced in F3
// and consumed by F4's FanoutSink: the per-sink failure aggregate and the
// sinkFanout seam (FanoutSink.PushTo) the retry queue drives. F4 adds the
// real FanoutSink to this file; the types below are its stable contract.

// SinkFailure names one sink's failure inside a fan-out push.
type SinkFailure struct {
	Sink string // sink name, e.g. "atlas", "nacos"
	Err  error
}

// FanoutError aggregates the per-sink failures of one fan-out call. A plain
// (non-Fanout) error means "all sinks failed / no fan-out happened".
type FanoutError []SinkFailure

// Error joins the failures' messages, each prefixed with its sink's name.
func (e FanoutError) Error() string {
	parts := make([]string, 0, len(e))
	for _, failure := range e {
		parts = append(parts, failure.Sink+": "+failure.Err.Error())
	}
	return strings.Join(parts, "; ")
}

// FailedSinks returns the names of the failed sinks in failure order.
func (e FanoutError) FailedSinks() []string {
	sinks := make([]string, 0, len(e))
	for _, failure := range e {
		sinks = append(sinks, failure.Sink)
	}
	return sinks
}

// failedSinkNames resolves which sinks a push error queues retries for: the
// named failures of a FanoutError (detected with errors.As so adapter-side
// wrapping cannot hide the per-sink breakdown — never a type assertion), or
// every known sink for a non-fanout error, the conservative backward-
// compatible fallback of plan §5.2.
func failedSinkNames(err error, known []string) []string {
	if err == nil || len(known) == 0 {
		return nil
	}
	var fanoutErr FanoutError
	if errors.As(err, &fanoutErr) && len(fanoutErr) > 0 {
		return fanoutErr.FailedSinks()
	}
	return known
}

// sinkFanout is the retry seam of plan §6.4: the named-sink surface the
// retry queue drives, exposed by F4's FanoutSink. Sinks() also supplies the
// sink names of the fallback path and the per-sink queue-depth metrics.
type sinkFanout interface {
	// PushTo pushes to exactly one named sink; an unknown name errors.
	PushTo(name string, triggerTime int64, instances []*instance.Instance) error
	// Sinks exposes the registered sink names.
	Sinks() []string
}

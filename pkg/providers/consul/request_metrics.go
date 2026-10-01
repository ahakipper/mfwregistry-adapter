package consul

import (
	"context"
	"errors"
	"net"
)

// normalizeConsulMetricSource accepts only the same small wire-safe alphabet
// used by logical source IDs. This is intentionally stricter than general
// string sanitization: replacing punctuation could turn an endpoint or token
// into a plausible-looking metric label and still leak deployment details.
func normalizeConsulMetricSource(value string) string {
	if value == "" || len(value) > 64 {
		return "unknown"
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return "unknown"
		}
	}
	return value
}

// classifyConsulRequestError converts an SDK/API error into the bounded
// request outcome set used by the request metrics. Context cancellation wins
// when the caller has already cancelled; net.Error and wrapped context
// deadlines are reported as timeout where the transport exposes that fact.
func classifyConsulRequestError(err error, ctx context.Context) string {
	if err == nil {
		return "success"
	}
	if ctx != nil {
		switch ctx.Err() {
		case context.Canceled:
			return "cancel"
		case context.DeadlineExceeded:
			return "timeout"
		}
	}
	if errors.Is(err, context.Canceled) {
		return "cancel"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	return "error"
}

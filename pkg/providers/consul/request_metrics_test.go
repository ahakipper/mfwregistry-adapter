package consul

import (
	"context"
	"testing"
)

func TestClassifyConsulRequestErrorUsesBoundedTimeoutAndCancelOutcomes(t *testing.T) {
	if got := classifyConsulRequestError(context.DeadlineExceeded, nil); got != "timeout" {
		t.Fatalf("deadline outcome = %q, want timeout", got)
	}
	if got := classifyConsulRequestError(context.Canceled, nil); got != "cancel" {
		t.Fatalf("cancel outcome = %q, want cancel", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := classifyConsulRequestError(context.Canceled, ctx); got != "cancel" {
		t.Fatalf("cancelled context outcome = %q, want cancel", got)
	}
}

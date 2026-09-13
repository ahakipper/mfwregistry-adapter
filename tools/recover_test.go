package tools

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
)

func TestWithRecoverUsesExplicitHandlersAndKeepsValue(t *testing.T) {
	var mu sync.Mutex
	var got interface{}
	WithRecover(func() { panic(errors.New("boom")) }, func(value interface{}) {
		mu.Lock()
		got = value
		mu.Unlock()
	})
	mu.Lock()
	defer mu.Unlock()
	if got == nil || got.(error).Error() != "boom" {
		t.Fatalf("explicit handler value = %#v, want boom", got)
	}
}

func TestDefaultPanicHandlerLogsBoundedSafeValue(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(previous)
	WithRecover(func() { panic("password=do-not-log " + strings.Repeat("x", 400)) })
	if !strings.Contains(output.String(), "recovered panic") {
		t.Fatalf("default recovery log = %q, want recovered marker", output.String())
	}
	if strings.Contains(output.String(), "do-not-log") || len(output.String()) > 400 {
		t.Fatalf("default recovery leaked/unbounded value: %q", output.String())
	}
}

func TestWithRecoverNilFunctionAndPanickingHandlerDoNotEscape(t *testing.T) {
	WithRecover(nil, func(interface{}) { panic("handler panic") })
	WithRecover(func() { panic("plain panic") }, nil)
}

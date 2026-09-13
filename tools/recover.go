package tools

import (
	"fmt"
	"log"
	"strings"
)

const maxRecoveredPanicText = 256

// defaultPanicHandler is deliberately best-effort: recovery is a last-resort
// observability path and must never turn an already recovered panic into a
// process crash. It emits a bounded, newline-free value while redacting
// values that look like credentials or bearer material.
var defaultPanicHandler = func(errValue interface{}) {
	defer func() { _ = recover() }()
	text := fmt.Sprintf("%v", errValue)
	if containsSensitivePanicText(text) {
		text = "<redacted sensitive panic value>"
	}
	text = strings.ReplaceAll(strings.ReplaceAll(text, "\n", "\\n"), "\r", "\\r")
	if len(text) > maxRecoveredPanicText {
		text = text[:maxRecoveredPanicText] + "..."
	}
	log.Printf("recovered panic: %s", text)
}

func containsSensitivePanicText(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"password", "passwd", "secret", "token", "authorization", "credential", "private_key"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

type PanicHandler func(errStr interface{})

func WithRecover(fn func(), panicHandlers ...PanicHandler) {
	defer func() {
		var handlers []PanicHandler
		if panicHandlers != nil && len(panicHandlers) > 0 {
			handlers = panicHandlers
		} else {
			handlers = []PanicHandler{defaultPanicHandler}
		}
		if e := recover(); e != nil {
			for _, h := range handlers {
				if h == nil {
					continue
				}
				func(handler PanicHandler) {
					defer func() { _ = recover() }()
					handler(e)
				}(h)
			}
		}
	}()
	// call
	fn()
}

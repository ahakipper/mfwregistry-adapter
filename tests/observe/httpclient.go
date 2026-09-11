//go:build observe
// +build observe

package observe

import (
	"net/http"
	"time"
)

// sharedHTTP is the observe package's shared HTTP client (10s timeout, the
// soak observer's discipline).
var sharedHTTP = &http.Client{Timeout: 10 * time.Second}

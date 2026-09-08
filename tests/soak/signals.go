//go:build soak
// +build soak

package soak

import (
	"os"
	"os/signal"
	"syscall"
)

// notifySignals registers INT/TERM notifications (the harness's cleanup
// hook for interrupted runs).
func notifySignals(c chan<- os.Signal) {
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
}

// stopSignals unregisters.
func stopSignals(c chan<- os.Signal) {
	signal.Stop(c)
}

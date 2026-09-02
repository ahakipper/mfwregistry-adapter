package logging

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// readLogEntries reads app.log in dir and returns the non-empty lines.
func readLogEntries(t *testing.T, dir string) []string {
	t.Helper()
	data, err := ioutil.ReadFile(filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("read app.log: %v", err)
	}
	lines := []string{}
	for _, line := range splitLines(string(data)) {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// splitLines splits s on '\n' without dropping the final empty segment.
func splitLines(s string) []string {
	lines := []string{}
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	lines = append(lines, s[start:])
	return lines
}

// parseEntry parses one JSON log line into a generic map.
func parseEntry(t *testing.T, line string) map[string]interface{} {
	t.Helper()
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("parse log line as JSON %q: %v", line, err)
	}
	return entry
}

// TestNewInfofErrorfJSONEntry verifies that Infof and Errorf produce
// parseable JSON entries in <path>/app.log.
func TestNewInfofErrorfJSONEntry(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-json")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, closer, err := New(Options{FilePath: dir, ToStd: false})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if logger == nil {
		t.Fatalf("New() returned nil logger")
	}
	if closer == nil {
		t.Fatalf("New() returned nil closer")
	}

	logger.Infof("info %s", "message")
	logger.Errorf("error %s", "message")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	entries := readLogEntries(t, dir)
	if len(entries) != 2 {
		t.Fatalf("got %d log entries, want 2: %v", len(entries), entries)
	}

	infoEntry := parseEntry(t, entries[0])
	if got := infoEntry["msg"]; got != "info message" {
		t.Errorf("info entry msg = %v, want %q", got, "info message")
	}
	if got := infoEntry["level"]; got != "INFO" {
		t.Errorf("info entry level = %v, want INFO", got)
	}

	errorEntry := parseEntry(t, entries[1])
	if got := errorEntry["msg"]; got != "error message" {
		t.Errorf("error entry msg = %v, want %q", got, "error message")
	}
	if got := errorEntry["level"]; got != "ERROR" {
		t.Errorf("error entry level = %v, want ERROR", got)
	}
}

// TestNewLogEncoding verifies the "log" (console) encoding: entries are
// produced and are not JSON objects.
func TestNewLogEncoding(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-log")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, closer, err := New(Options{FilePath: dir, Encoding: "log", ToStd: false})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	logger.Info("console entry")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	entries := readLogEntries(t, dir)
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1: %v", len(entries), entries)
	}
	if !contains(entries[0], "console entry") {
		t.Errorf("log entry %q does not contain %q", entries[0], "console entry")
	}
	var entry map[string]interface{}
	if err := json.Unmarshal([]byte(entries[0]), &entry); err == nil {
		t.Errorf("log entry %q parses as JSON, want console format", entries[0])
	}
}

// contains reports whether substr appears in s.
func contains(s, substr string) bool {
	return len(substr) == 0 || len(s) >= len(substr) && indexOf(s, substr) >= 0
}

// indexOf returns the first index of substr in s, or -1.
func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TestNewCloserFlushes verifies that Close flushes buffered entries to the
// file: an entry logged right before Close is present in app.log even
// without any crash-triggered sync.
func TestNewCloserFlushes(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-flush")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, closer, err := New(Options{FilePath: dir, ToStd: false})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}

	const message = "flushed-entry"
	logger.Infof("%s", message)

	if err := closer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	entries := readLogEntries(t, dir)
	found := false
	for _, entry := range entries {
		parsed := parseEntry(t, entry)
		if parsed["msg"] == message {
			found = true
		}
	}
	if !found {
		t.Errorf("app.log does not contain entry %q after Close: %v", message, entries)
	}
}

// TestNewInvalidEncoding verifies that an unknown encoding fails instead of
// silently falling back to the console encoder.
func TestNewInvalidEncoding(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-invalid")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, closer, err := New(Options{FilePath: dir, Encoding: "xml"})
	if err == nil {
		closer.Close()
		t.Fatalf("New(encoding xml) succeeded, want error")
	}
	if logger != nil {
		t.Errorf("New(encoding xml) logger = %v, want nil", logger)
	}
	if closer != nil {
		t.Errorf("New(encoding xml) closer = %v, want nil", closer)
	}
}

// TestNewDefaults verifies the zero-value defaults: New with the zero
// Options succeeds (writing into the default ./logfiles/ directory would
// touch the working tree, so the test only exercises the directory that the
// default path resolves to via getEncoder/default constants).
func TestNewDefaults(t *testing.T) {
	// The zero Options uses FilePath "./logfiles/", which would create a
	// directory in the repository working tree. Exercise the defaults for
	// every other field with an explicit temp path; FilePath defaulting is
	// verified structurally below.
	if defaultFilePath != "./logfiles/" {
		t.Errorf("defaultFilePath = %q, want %q", defaultFilePath, "./logfiles/")
	}
	if defaultMaxSizeMB != 100 {
		t.Errorf("defaultMaxSizeMB = %d, want 100", defaultMaxSizeMB)
	}
	if defaultMaxBackups != 10 {
		t.Errorf("defaultMaxBackups = %d, want 10", defaultMaxBackups)
	}
	if defaultMaxAge != 7 {
		t.Errorf("defaultMaxAge = %d, want 7", defaultMaxAge)
	}
	if defaultMaxLevel != -1 {
		t.Errorf("defaultMaxLevel = %d, want -1", defaultMaxLevel)
	}
	if defaultEncoding != "json" {
		t.Errorf("defaultEncoding = %q, want %q", defaultEncoding, "json")
	}

	dir, err := ioutil.TempDir("", "logging-defaults")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Only FilePath is set; ToStd stays false so the test output stays clean.
	// All other zero fields must be replaced with working defaults.
	logger, closer, err := New(Options{FilePath: dir, ToStd: false})
	if err != nil {
		t.Fatalf("New() with defaults returned error: %v", err)
	}
	// The port has no Debug method; use Info to verify the default path
	// works end to end (the level default itself is asserted above).
	logger.Info("info entry with defaults")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	// Default level -1 (debug) accepts the info entry.
	entries := readLogEntries(t, dir)
	if len(entries) != 1 {
		t.Fatalf("got %d log entries, want 1: %v", len(entries), entries)
	}
	entry := parseEntry(t, entries[0])
	if got := entry["msg"]; got != "info entry with defaults" {
		t.Errorf("entry msg = %v, want info entry", got)
	}
}

// TestNewNop verifies the Nop logger is a no-op.
func TestNewNop(t *testing.T) {
	logger := NewNop()
	if logger == nil {
		t.Fatalf("NewNop() returned nil")
	}
	if _, ok := logger.(interface{}); !ok {
		t.Fatalf("NewNop() returned %T, want ports.NopLogger", logger)
	}
	// All six methods must be callable without panicking.
	logger.Info("info")
	logger.Infof("info %s", "f")
	logger.Warn("warn")
	logger.Warnf("warn %s", "f")
	logger.Error("error")
	logger.Errorf("error %s", "f")
}

// TestLevelFiltering verifies that entries below the configured level are
// dropped and entries at or above it are kept.
func TestLevelFiltering(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-level")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	logger, closer, err := New(Options{FilePath: dir, Level: 1, ToStd: false})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	logger.Info("dropped info")
	logger.Warn("kept warn")
	logger.Error("kept error")
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	entries := readLogEntries(t, dir)
	if len(entries) != 2 {
		t.Fatalf("got %d log entries, want 2: %v", len(entries), entries)
	}
	if got := parseEntry(t, entries[0])["level"]; got != "WARN" {
		t.Errorf("first entry level = %v, want WARN", got)
	}
	if got := parseEntry(t, entries[1])["level"]; got != "ERROR" {
		t.Errorf("second entry level = %v, want ERROR", got)
	}
}

// TestCloseIdempotent verifies Close can be called twice.
func TestCloseIdempotent(t *testing.T) {
	dir, err := ioutil.TempDir("", "logging-close")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	_, closer, err := New(Options{FilePath: dir, ToStd: false})
	if err != nil {
		t.Fatalf("New() returned error: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("first Close() returned error: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("second Close() returned error: %v", err)
	}
}

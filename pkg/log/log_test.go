package log

import (
	"os"
	"path/filepath"
	"spotter/config"
	"testing"
)

func TestLegacyLoggerInitUsesConfiguredLegacyValues(t *testing.T) {
	oldPath, oldLevel, oldEncoding, oldStd := config.LogFilePath, config.LogLevel, config.LogEncoding, config.LogToStd
	oldLogger := Logger
	t.Cleanup(func() {
		Logger = oldLogger
		config.LogFilePath, config.LogLevel, config.LogEncoding, config.LogToStd = oldPath, oldLevel, oldEncoding, oldStd
	})
	dir := t.TempDir()
	config.LogFilePath, config.LogLevel, config.LogEncoding, config.LogToStd = filepath.Join(dir, ""), 0, "json", false
	config.LogBackups, config.LogSize, config.LogAge = 1, 1, 1
	if err := LoggerInit(); err != nil {
		t.Fatalf("LoggerInit: %v", err)
	}
	if Logger == nil {
		t.Fatal("LoggerInit did not create a logger")
	}
	Logger.Info("legacy-contract")
	if err := Logger.Sync(); err != nil && !os.IsPermission(err) {
		t.Fatalf("Logger.Sync: %v", err)
	}
}

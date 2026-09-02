package composition

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	infraconfig "spotter/internal/infra/config"
	inframetrics "spotter/internal/infra/metrics"
	"spotter/internal/ports"
	"spotter/internal/testkit/fakes"
)

// testConfig returns a config whose log settings point at a temp directory,
// so Build exercises the real logging adapter without touching the working
// tree.
func testConfig(t *testing.T) infraconfig.Config {
	t.Helper()
	dir, err := ioutil.TempDir("", "composition-root")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return infraconfig.Config{
		Env:         "test",
		LogFilePath: dir,
		LogSize:     100,
		LogLevel:    -1,
		LogBackups:  10,
		LogAge:      7,
		LogEncoding: "json",
		LogToStd:    false,
		Providers:   []string{"k8s"},
	}
}

// TestBuildDefaults verifies that Build with no overrides returns a runtime
// whose pieces are non-nil, Nop-safe and closable to a real temp log file.
func TestBuildDefaults(t *testing.T) {
	cfg := testConfig(t)

	rt, err := Build(cfg, Deps{})
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	if rt == nil {
		t.Fatal("Build() returned nil runtime")
	}
	if rt.Logger == nil {
		t.Error("Logger = nil, want non-nil")
	}
	if rt.Notifier == nil {
		t.Error("Notifier = nil, want non-nil")
	}
	if rt.Metrics == nil {
		t.Error("Metrics = nil, want non-nil")
	}
	if rt.LocalIP == nil {
		t.Error("LocalIP = nil, want non-nil")
	}
	if rt.LogCloser == nil {
		t.Fatal("LogCloser = nil, want non-nil")
	}
	if !reflect.DeepEqual(rt.Config, cfg) {
		t.Errorf("Config = %+v, want %+v", rt.Config, cfg)
	}

	// All port methods must be callable without panicking (Nop-safe).
	rt.Logger.Info("info")
	rt.Logger.Infof("info %s", "f")
	rt.Logger.Warn("warn")
	rt.Logger.Warnf("warn %s", "f")
	rt.Logger.Error("error")
	rt.Logger.Errorf("error %s", "f")
	rt.Metrics.ObserveSyncOnceDuration(time.Second)
	rt.Metrics.ObserveSyncAllDuration("k8s", time.Second)
	rt.Metrics.SetSyncErrorQueueDepth(1)
	rt.Metrics.MarkSyncOnce()
	rt.Notifier.Notify("title", "content")

	if err := rt.LogCloser.Close(); err != nil {
		t.Fatalf("LogCloser.Close() returned error: %v", err)
	}
	// The log sink must have been created inside the configured path.
	if _, err := os.Stat(filepath.Join(cfg.LogFilePath, "app.log")); err != nil {
		t.Errorf("app.log missing after Close: %v", err)
	}
}

// TestBuildEnvPropagated verifies the config env is carried into the
// runtime (the notice adapter is built with it).
func TestBuildEnvPropagated(t *testing.T) {
	cfg := testConfig(t)
	cfg.Env = "product"

	rt, err := Build(cfg, Deps{})
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	defer rt.LogCloser.Close()

	if rt.Config.Env != "product" {
		t.Errorf("Config.Env = %q, want %q", rt.Config.Env, "product")
	}
	if rt.Notifier == nil {
		t.Fatal("Notifier = nil, want the appcenter notice adapter")
	}
}

// TestBuildOverrides verifies that every Deps field wins over the default.
func TestBuildOverrides(t *testing.T) {
	cfg := testConfig(t)

	logger := &fakes.FakeLogger{}
	closer := nopCloser{}
	notifier := &fakes.FakeNotifier{}
	recorder := &fakes.FakeMetricsRecorder{}
	localIP := func() (string, error) { return "10.0.0.1", nil }

	rt, err := Build(cfg, Deps{
		Logger:    logger,
		LogCloser: closer,
		Notifier:  notifier,
		Metrics:   recorder,
		LocalIP:   localIP,
	})
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}

	if rt.Logger != ports.Logger(logger) {
		t.Error("Logger was not overridden")
	}
	if rt.LogCloser != io.Closer(closer) {
		t.Error("LogCloser was not overridden")
	}
	if rt.Notifier != ports.Notifier(notifier) {
		t.Error("Notifier was not overridden")
	}
	if rt.Metrics != ports.MetricsRecorder(recorder) {
		t.Error("Metrics was not overridden")
	}
	ip, err := rt.LocalIP()
	if ip != "10.0.0.1" || err != nil {
		t.Errorf("LocalIP() = (%q, %v), want (%q, nil)", ip, err, "10.0.0.1")
	}
	// The default log sink must have been closed, not leaked.
	if _, err := os.Stat(filepath.Join(cfg.LogFilePath, "app.log")); err == nil {
		t.Error("default log file was created although a logger was injected")
	}
}

// TestBuildUsesMetricsRecorderDefault verifies the default metrics recorder
// is the infra Prometheus recorder.
func TestBuildUsesMetricsRecorderDefault(t *testing.T) {
	cfg := testConfig(t)
	rt, err := Build(cfg, Deps{})
	if err != nil {
		t.Fatalf("Build() returned error: %v", err)
	}
	defer rt.LogCloser.Close()
	if _, ok := rt.Metrics.(*inframetrics.Recorder); !ok {
		t.Errorf("Metrics = %T, want *inframetrics.Recorder", rt.Metrics)
	}
}

// TestBuildInvalidConfig verifies that an invalid log encoding fails the
// build instead of silently falling back.
func TestBuildInvalidConfig(t *testing.T) {
	cfg := testConfig(t)
	cfg.LogEncoding = "xml"

	rt, err := Build(cfg, Deps{})
	if err == nil {
		rt.LogCloser.Close()
		t.Fatal("Build() succeeded with invalid encoding, want error")
	}
	if rt != nil {
		t.Errorf("Build() runtime = %v, want nil", rt)
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

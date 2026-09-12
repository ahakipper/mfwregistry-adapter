// Package cmd pins the flag mapping of the adapter command: cobra flags →
// adapterFlags (tri-state via Flags().Changed) → infraconfig.Load → the
// resolved Config → applyLegacyGlobals (the legacy config globals). These
// tests cover the struct path; flags that map only through cobra command
// arguments outside the struct (none today — every flag the adapter command
// registers reaches adapterFlags) are noted in the test bodies.
package cmd

import (
	"reflect"
	"testing"

	"github.com/spf13/cobra"

	"spotter/config"
	infraconfig "spotter/internal/infra/config"
)

// newAdapterCommand builds a fresh adapter command with the same flags the
// package init registers (a fresh instance keeps flag value state out of the
// package-level command shared with other tests).
func newAdapterCommand() *cobra.Command {
	cmd := &cobra.Command{Use: "adapter"}
	cmd.Flags().StringSliceP("providers", "r", []string{}, "the providers")
	cmd.Flags().BoolP("leader-elect", "t", true, "whether to enable node election")
	cmd.Flags().StringP("env", "e", "test", "the environment")
	cmd.Flags().IntP("push-interval", "i", 21600, "the time interval for full synchronization")
	cmd.Flags().StringP("grpc-addr", "g", "172.16.130.71:50051", "the Atlas grpc address")
	cmd.Flags().BoolP("disable-worker", "w", false, "disable push worker")
	cmd.Flags().StringSliceP("appcodes", "", []string{}, "only push instances of the appcodes")
	cmd.Flags().StringP("metrics-addr", "", ":8090", "the Prometheus metrics address")
	cmd.Flags().StringP("nacos-addr", "", "", "the Nacos OpenAPI address")
	cmd.Flags().StringSlice("nacos-server-list", nil, "comma-separated Nacos server addresses")
	cmd.Flags().String("nacos-namespace", "", "Nacos namespace ID")
	cmd.Flags().String("nacos-group", "", "Nacos group name")
	cmd.Flags().String("nacos-username", "", "Nacos username")
	cmd.Flags().String("nacos-password", "", "Nacos password")
	cmd.Flags().String("nacos-access-token", "", "Nacos access token")
	cmd.Flags().String("nacos-ca-file", "", "Nacos CA file")
	cmd.Flags().String("nacos-server-name", "", "Nacos TLS server name")
	cmd.Flags().Bool("nacos-insecure-skip-verify", false, "skip Nacos TLS verification")
	cmd.Flags().Int("nacos-timeout", 0, "Nacos timeout seconds")
	cmd.Flags().StringSliceP("kubeconfig", "", []string{}, "comma-separated kubeconfig paths")
	cmd.Flags().StringSliceP("consul-addr", "", []string{}, "comma-separated consul addresses")
	cmd.Flags().StringSliceP("etcd-endpoints", "", []string{}, "comma-separated etcd endpoints")
	cmd.Flags().StringP("log-file-path", "p", "./logfiles/", "the log path")
	cmd.Flags().IntP("log-maxsize", "m", 100, "max log size (MB)")
	cmd.Flags().IntP("log-backup-number", "n", 10, "log backup numbers")
	cmd.Flags().IntP("log-level", "l", -1, "-1 debug, 0 info, 1 warnning")
	cmd.Flags().IntP("log-age", "a", 7, "max expired time (day)")
	cmd.Flags().BoolP("log-to-std", "s", true, "whether to output the log to standard output")
	cmd.Flags().StringP("log-encoding", "c", "json", "output log format")
	return cmd
}

// setFlag sets name=value and marks it changed (what a command-line
// occurrence does); a flag only set through its default stays "unchanged".
func setFlag(t *testing.T, cmd *cobra.Command, name, value string) {
	t.Helper()
	if err := cmd.Flags().Set(name, value); err != nil {
		t.Fatalf("Flags().Set(%q, %q) error = %v", name, value, err)
	}
}

// TestAdapterFlagsMapsEveryDemoFlag: build the adapter command with every
// flag the demo invocation used set to a distinctive value, run the mapping
// (adapterFlags), and assert the resulting Flags struct carries them —
// including the tri-state pointer for push-interval, leader-elect and
// log-to-std (an explicit value sets the pointer; a changed=false flag sets
// the pointer to false, not to the default).
func TestAdapterFlagsMapsEveryDemoFlag(t *testing.T) {
	cmd := newAdapterCommand()
	setFlag(t, cmd, "nacos-addr", "127.0.0.1:18848")
	setFlag(t, cmd, "nacos-server-list", "nacos-a:8848,nacos-b:8848")
	setFlag(t, cmd, "nacos-namespace", "tenant-a")
	setFlag(t, cmd, "nacos-group", "blue")
	setFlag(t, cmd, "nacos-username", "operator")
	setFlag(t, cmd, "nacos-password", "secret")
	setFlag(t, cmd, "nacos-access-token", "token")
	setFlag(t, cmd, "nacos-ca-file", "/tmp/nacos-ca.pem")
	setFlag(t, cmd, "nacos-server-name", "nacos.internal")
	setFlag(t, cmd, "nacos-insecure-skip-verify", "true")
	setFlag(t, cmd, "nacos-timeout", "17")
	setFlag(t, cmd, "consul-addr", "127.0.0.1:18500")
	setFlag(t, cmd, "kubeconfig", "/tmp/soak/kubeconfig")
	setFlag(t, cmd, "etcd-endpoints", "127.0.0.1:12379")
	setFlag(t, cmd, "push-interval", "30")
	setFlag(t, cmd, "leader-elect", "false")
	setFlag(t, cmd, "grpc-addr", "127.0.0.1:19999")
	setFlag(t, cmd, "log-file-path", "/tmp/soak/logs")
	setFlag(t, cmd, "log-to-std", "false")
	setFlag(t, cmd, "providers", "k8s,ecs")
	setFlag(t, cmd, "metrics-addr", "127.0.0.1:18090")
	setFlag(t, cmd, "disable-worker", "true")
	setFlag(t, cmd, "appcodes", "pay-user")

	flags := adapterFlags(cmd)

	if flags.NacosAddr != "127.0.0.1:18848" {
		t.Fatalf("NacosAddr = %q, want 127.0.0.1:18848", flags.NacosAddr)
	}
	if !reflect.DeepEqual(flags.NacosServerList, []string{"nacos-a:8848", "nacos-b:8848"}) || flags.NacosNamespace != "tenant-a" || flags.NacosGroup != "blue" ||
		flags.NacosUsername != "operator" || flags.NacosPassword != "secret" || flags.NacosAccessToken != "token" || flags.NacosCAFile != "/tmp/nacos-ca.pem" ||
		flags.NacosServerName != "nacos.internal" || !flags.NacosInsecureSkipVerify || flags.NacosTimeout != 17 {
		t.Fatalf("Nacos options not mapped: %+v", flags)
	}
	if got := flags.ConsulAddrFlag; !reflect.DeepEqual(got, []string{"127.0.0.1:18500"}) {
		t.Fatalf("ConsulAddrFlag = %v, want [127.0.0.1:18500]", got)
	}
	if got := flags.KubeConfigPathFlag; !reflect.DeepEqual(got, []string{"/tmp/soak/kubeconfig"}) {
		t.Fatalf("KubeConfigPathFlag = %v, want [/tmp/soak/kubeconfig]", got)
	}
	if got := flags.EtcdEndpointsFlag; !reflect.DeepEqual(got, []string{"127.0.0.1:12379"}) {
		t.Fatalf("EtcdEndpointsFlag = %v, want [127.0.0.1:12379]", got)
	}
	if flags.GrpcAddr != "127.0.0.1:19999" {
		t.Fatalf("GrpcAddr = %q, want 127.0.0.1:19999", flags.GrpcAddr)
	}
	if flags.LogFilePath != "/tmp/soak/logs" {
		t.Fatalf("LogFilePath = %q, want /tmp/soak/logs", flags.LogFilePath)
	}
	if flags.MetricsAddr != "127.0.0.1:18090" {
		t.Fatalf("MetricsAddr = %q, want 127.0.0.1:18090", flags.MetricsAddr)
	}
	if !flags.DisablePushWorker {
		t.Fatal("DisablePushWorker = false, want true")
	}
	if got := flags.Providers; !reflect.DeepEqual(got, []string{"k8s", "ecs"}) {
		t.Fatalf("Providers = %v, want [k8s ecs]", got)
	}
	if got := flags.PushAppCodes; !reflect.DeepEqual(got, []string{"pay-user"}) {
		t.Fatalf("PushAppCodes = %v, want [pay-user]", got)
	}

	// Tri-state pointers: set through Flags().Changed, so the pointer is
	// non-nil even for an explicit false/zero.
	if flags.PushIntervalPtr == nil || *flags.PushIntervalPtr != 30 {
		t.Fatalf("PushIntervalPtr = %v, want a non-nil pointer to 30", flags.PushIntervalPtr)
	}
	if flags.LeaderElection == nil || *flags.LeaderElection {
		t.Fatalf("LeaderElection = %v, want a non-nil pointer to false", flags.LeaderElection)
	}
	if flags.LogToStdPtr == nil || *flags.LogToStdPtr {
		t.Fatalf("LogToStdPtr = %v, want a non-nil pointer to false", flags.LogToStdPtr)
	}
}

// TestAdapterFlagsUnchangedFlagsStayDefault: flags left at their defaults
// (no command-line occurrence) must keep the tri-state pointers nil — the
// legacy "flag not set" state Load resolves through the default constants —
// and the plain fields at the cobra defaults.
func TestAdapterFlagsUnchangedFlagsStayDefault(t *testing.T) {
	cmd := newAdapterCommand()

	flags := adapterFlags(cmd)

	if flags.PushIntervalPtr != nil {
		t.Fatalf("PushIntervalPtr = %v, want nil (push-interval not changed)", *flags.PushIntervalPtr)
	}
	if flags.LeaderElection != nil {
		t.Fatalf("LeaderElection = %v, want nil (leader-elect not changed)", *flags.LeaderElection)
	}
	if flags.LogToStdPtr != nil {
		t.Fatalf("LogToStdPtr = %v, want nil (log-to-std not changed)", *flags.LogToStdPtr)
	}
	if flags.LogLevelPtr != nil {
		t.Fatalf("LogLevelPtr = %v, want nil (log-level not changed)", *flags.LogLevelPtr)
	}
	if flags.LogSizePtr != nil {
		t.Fatalf("LogSizePtr = %v, want nil (log-maxsize not changed)", *flags.LogSizePtr)
	}
	if flags.PushAllInterval != 21600 {
		t.Fatalf("PushAllInterval = %d, want the cobra default 21600", flags.PushAllInterval)
	}
	if flags.GrpcAddr != "172.16.130.71:50051" {
		t.Fatalf("GrpcAddr = %q, want the cobra default", flags.GrpcAddr)
	}
	if flags.NacosAddr != "" {
		t.Fatalf("NacosAddr = %q, want empty (the disabled default)", flags.NacosAddr)
	}
}

// TestAdapterFlagsExplicitZeroHonored: push-interval=0 and log-level=0 are
// legal explicit values with no default fallback — Flags().Changed must
// carry them through the pointer instead of letting Load's
// intOrDefault coerce them to the defaults.
func TestAdapterFlagsExplicitZeroHonored(t *testing.T) {
	cmd := newAdapterCommand()
	setFlag(t, cmd, "push-interval", "0")
	setFlag(t, cmd, "log-level", "0")

	flags := adapterFlags(cmd)
	if flags.PushIntervalPtr == nil || *flags.PushIntervalPtr != 0 {
		t.Fatalf("PushIntervalPtr = %v, want a non-nil pointer to 0", flags.PushIntervalPtr)
	}
	if flags.LogLevelPtr == nil || *flags.LogLevelPtr != 0 {
		t.Fatalf("LogLevelPtr = %v, want a non-nil pointer to 0", flags.LogLevelPtr)
	}
}

// TestFlagMappingResolvesConfigAndLegacyGlobals: the full struct path —
// adapterFlags → infraconfig.Load → applyLegacyGlobals — with every demo
// flag at a distinctive value. The resolved config and the legacy globals
// must both carry the overrides (the globals are the bridge the un-migrated
// packages read).
func TestFlagMappingResolvesConfigAndLegacyGlobals(t *testing.T) {
	cmd := newAdapterCommand()
	setFlag(t, cmd, "nacos-addr", "127.0.0.1:18848")
	setFlag(t, cmd, "consul-addr", "127.0.0.1:18500")
	setFlag(t, cmd, "kubeconfig", "/tmp/soak/kubeconfig")
	setFlag(t, cmd, "etcd-endpoints", "127.0.0.1:12379")
	setFlag(t, cmd, "push-interval", "30")
	setFlag(t, cmd, "leader-elect", "false")
	setFlag(t, cmd, "grpc-addr", "127.0.0.1:19999")
	setFlag(t, cmd, "log-file-path", "/tmp/soak/logs")
	setFlag(t, cmd, "log-to-std", "false")
	setFlag(t, cmd, "providers", "k8s")

	cfg, err := infraconfig.Load("test", adapterFlags(cmd))
	if err != nil {
		t.Fatalf("Load(test, flags) error = %v", err)
	}

	// The resolved config: every override carried, and the etcd TLS rule of
	// plan §8.4 (non-empty --etcd-endpoints empties the TLS paths).
	if cfg.NacosAddr != "127.0.0.1:18848" {
		t.Fatalf("cfg.NacosAddr = %q, want 127.0.0.1:18848", cfg.NacosAddr)
	}
	if got := cfg.ConsulAddress; !reflect.DeepEqual(got, []string{"127.0.0.1:18500"}) {
		t.Fatalf("cfg.ConsulAddress = %v, want [127.0.0.1:18500]", got)
	}
	if got := cfg.KubeConfigPath; !reflect.DeepEqual(got, []string{"/tmp/soak/kubeconfig"}) {
		t.Fatalf("cfg.KubeConfigPath = %v, want [/tmp/soak/kubeconfig]", got)
	}
	if got := cfg.EtcdEndpoints; !reflect.DeepEqual(got, []string{"127.0.0.1:12379"}) {
		t.Fatalf("cfg.EtcdEndpoints = %v, want [127.0.0.1:12379]", got)
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" || cfg.CAFile != "" {
		t.Fatalf("etcd TLS paths = %q/%q/%q, want all empty (the §8.4 insecure local rule)", cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	}
	if cfg.PushAllInterval != 30 {
		t.Fatalf("cfg.PushAllInterval = %d, want 30", cfg.PushAllInterval)
	}
	if cfg.EnableLeaderElection {
		t.Fatal("cfg.EnableLeaderElection = true, want false (explicit --leader-elect=false)")
	}
	if cfg.GrpcAddr != "127.0.0.1:19999" {
		t.Fatalf("cfg.GrpcAddr = %q, want 127.0.0.1:19999", cfg.GrpcAddr)
	}
	if cfg.LogFilePath != "/tmp/soak/logs" {
		t.Fatalf("cfg.LogFilePath = %q, want /tmp/soak/logs", cfg.LogFilePath)
	}
	if cfg.LogToStd {
		t.Fatal("cfg.LogToStd = true, want false (explicit --log-to-std=false)")
	}

	// applyLegacyGlobals (NOT assignLegacyGlobals: that one also runs
	// LoggerInit/notice init, which touch the filesystem and network-side
	// globals) must mirror the resolved config into the legacy globals.
	applyLegacyGlobals(cfg)
	if got := config.EtcdEndpoints; !reflect.DeepEqual(got, []string{"127.0.0.1:12379"}) {
		t.Fatalf("legacy config.EtcdEndpoints = %v, want [127.0.0.1:12379]", got)
	}
	if config.CertFile != "" || config.KeyFile != "" || config.CAFile != "" {
		t.Fatalf("legacy etcd TLS = %q/%q/%q, want all empty", config.CertFile, config.KeyFile, config.CAFile)
	}
	if got := config.KubeConfigPath; !reflect.DeepEqual(got, []string{"/tmp/soak/kubeconfig"}) {
		t.Fatalf("legacy config.KubeConfigPath = %v, want [/tmp/soak/kubeconfig]", got)
	}
	if got := config.ConsulAddress; !reflect.DeepEqual(got, []string{"127.0.0.1:18500"}) {
		t.Fatalf("legacy config.ConsulAddress = %v, want [127.0.0.1:18500]", got)
	}
	if config.PushAllInterval != 30 {
		t.Fatalf("legacy config.PushAllInterval = %d, want 30", config.PushAllInterval)
	}
	if config.GrpcAddr != "127.0.0.1:19999" {
		t.Fatalf("legacy config.GrpcAddr = %q, want 127.0.0.1:19999", config.GrpcAddr)
	}
	if config.LogFilePath != "/tmp/soak/logs" {
		t.Fatalf("legacy config.LogFilePath = %q, want /tmp/soak/logs", config.LogFilePath)
	}
	if config.LogToStd {
		t.Fatal("legacy config.LogToStd = true, want false")
	}
	if config.EnableLeaderElection {
		t.Fatal("legacy config.EnableLeaderElection = true, want false")
	}
	if config.LockCampaignKey != "/paas/spotter-test" {
		t.Fatalf("legacy config.LockCampaignKey = %q, want the test preset /paas/spotter-test", config.LockCampaignKey)
	}
}

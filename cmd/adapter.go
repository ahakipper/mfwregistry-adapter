// Copyright © 2020 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"spotter/config"
	"spotter/internal"
	"spotter/internal/composition"
	infraconfig "spotter/internal/infra/config"
	"spotter/pkg/log"
	"spotter/pkg/notice"
	"spotter/pkg/providers"
)

// adapterCmd represents the adapter command
var adapterCmd = &cobra.Command{
	Use:   "adapter",
	Short: "Run the instance adapter",
	Long: `The adapter command is the main entry point of spotter.

It watches the configured providers (Kubernetes clusters and Consul servers),
converts the observed pods/endpoints into instance data, and pushes the
resulting instance events (incremental and full) to the discovery center
(Atlas) over gRPC.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("starting adapter")
		// init flags
		flags := adapterFlags(cmd)
		// init noticer
		env, err := cmd.Flags().GetString("env")
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if env != "product" && env != "dev" && env != "test" {
			// the legacy code panicked on an invalid env; keep the exit code
			// (1) and the message shape, but return instead of panicking.
			fmt.Println("invalid env param")
			os.Exit(1)
		}
		// resolve config
		cfg, err := infraconfig.Load(env, flags)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		// build the composition root (logger, notifier, metrics)
		rt, err := composition.Build(cfg, composition.Deps{})
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		// Temporary bridge: the k8s/consul providers, the k8s conversion
		// code and pkg/distribute/election still read the pkg/log global
		// logger and send notices through the pkg/notice global Noticer.
		// Copy the resolved config into the legacy globals and initialize
		// both globals so those packages keep working unchanged until they
		// are migrated to the injected logger/notifier.
		assignLegacyGlobals(cfg)
		// Note on rt.LogCloser: it is not closed explicitly here. Run()
		// blocks forever (the server only exits through process
		// termination), and the legacy code equally relied on process exit
		// to flush the log sink, so this preserves the existing lifecycle
		// behavior.
		_ = rt.LogCloser
		// server init
		server, err := internal.NewServerFromDeps(rt)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		// run
		server.Run()

		// notify signal
		// c := make(chan os.Signal)
		// signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM)
		// // wait for stop
		// quit := <-c
		// log.Logger.Info("receive quit signal: ", quit)
		// server.Stop()
	},
}

func init() {
	rootCmd.AddCommand(adapterCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// adapterCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this
	// command is called directly, e.g.:
	adapterCmd.Flags().StringSliceP("providers", "r", []string{},
		fmt.Sprintf("the providers, e.g: %s, %s. multiple values are separated by commas", providers.ProviderK8s, providers.ProviderEcs),
	)
	adapterCmd.Flags().BoolP("leader-elect", "t", true, "whether to enable node election")
	adapterCmd.Flags().StringP("env", "e", "test", "the environment, e.g: dev, product")
	adapterCmd.Flags().IntP("push-interval", "i", 21600, "the time interval for full synchronization. the unit is seconds")
	adapterCmd.Flags().StringP("grpc-addr", "g", "172.16.130.71:50051", "the Atlas grpc address")
	adapterCmd.Flags().BoolP("disable-worker", "w", false, "disable push worker, just for testing")
	adapterCmd.Flags().StringSliceP("appcodes", "", []string{}, "only push instances of the appcodes, just for testing")
	adapterCmd.Flags().StringP("metrics-addr", "", ":8090", "the Prometheus metrics address")
}

// adapterFlags maps the cobra flags onto the infra config flag struct. The
// flag names and semantics match the legacy initAdapterFlags exactly.
//
// Flags whose zero value is a legal explicit value (log-level 0 = info,
// log-to-std=false, push-interval=0, ...) are mapped through the tri-state
// pointer fields using Flags().Changed, so an explicit zero/false on the
// command line is honored instead of being coerced to the default.
func adapterFlags(cmd *cobra.Command) infraconfig.Flags {
	flags := infraconfig.Flags{
		LogFilePath:       flagString(cmd, "log-file-path"),
		LogSize:           flagInt(cmd, "log-maxsize"),
		LogLevel:          flagInt(cmd, "log-level"),
		LogBackups:        flagInt(cmd, "log-backup-number"),
		LogAge:            flagInt(cmd, "log-age"),
		LogToStd:          flagBool(cmd, "log-to-std"),
		LogEncoding:       flagString(cmd, "log-encoding"),
		PushAllInterval:   flagInt(cmd, "push-interval"),
		GrpcAddr:          flagString(cmd, "grpc-addr"),
		DisablePushWorker: flagBool(cmd, "disable-worker"),
		Providers:         flagStringSlice(cmd, "providers"),
		PushAppCodes:      flagStringSlice(cmd, "appcodes"),
		MetricsAddr:       flagString(cmd, "metrics-addr"),
	}
	if cmd.Flags().Changed("log-maxsize") {
		flags.LogSizePtr = intPtr(flagInt(cmd, "log-maxsize"))
	}
	if cmd.Flags().Changed("log-level") {
		flags.LogLevelPtr = intPtr(flagInt(cmd, "log-level"))
	}
	if cmd.Flags().Changed("log-backup-number") {
		flags.LogBackupsPtr = intPtr(flagInt(cmd, "log-backup-number"))
	}
	if cmd.Flags().Changed("log-age") {
		flags.LogAgePtr = intPtr(flagInt(cmd, "log-age"))
	}
	if cmd.Flags().Changed("log-to-std") {
		flags.LogToStdPtr = boolPtr(flagBool(cmd, "log-to-std"))
	}
	if cmd.Flags().Changed("push-interval") {
		flags.PushIntervalPtr = intPtr(flagInt(cmd, "push-interval"))
	}
	if cmd.Flags().Changed("leader-elect") {
		flags.LeaderElection = boolPtr(flagBool(cmd, "leader-elect"))
	}
	return flags
}

// intPtr and boolPtr box flag values for the tri-state Flags fields.
func intPtr(v int) *int    { return &v }
func boolPtr(v bool) *bool { return &v }

// assignLegacyGlobals copies the resolved configuration into the legacy
// global config package and initializes the pkg/log global logger and the
// pkg/notice global Noticer, for the packages that are not wired through
// the composition root yet (k8s and consul providers, k8s conversion,
// pkg/distribute/election, pkg/metrics proserver).
func assignLegacyGlobals(cfg infraconfig.Config) {
	applyLegacyGlobals(cfg)
	_ = log.LoggerInit()
	// The legacy notice call sites (pkg/providers/k8s, pkg/providers/consul,
	// pkg/distribute/election) send through the pkg/notice global; without
	// this call the global Noticer stays nil and their notices silently
	// no-op.
	notice.InitNoticeClient(cfg.Env)
}

// applyLegacyGlobals mirrors the legacy initAdapterFlags global assignment
// from the resolved config. Etcd endpoints, TLS file paths, kubeconfig
// paths, consul addresses, providers and the campaign key come from the
// environment preset of the resolved config.
func applyLegacyGlobals(cfg infraconfig.Config) {
	config.EtcdEndpoints = cfg.Endpoints.EtcdEndpoints
	config.CertFile = cfg.Endpoints.CertFile
	config.KeyFile = cfg.Endpoints.KeyFile
	config.CAFile = cfg.Endpoints.CAFile
	config.KubeConfigPath = cfg.Endpoints.KubeConfigPath
	config.ConsulAddress = cfg.Endpoints.ConsulAddress
	config.LockCampaignKey = cfg.Endpoints.LockCampaignKey
	// log
	config.LogFilePath = cfg.LogFilePath
	config.LogSize = cfg.LogSize
	config.LogLevel = cfg.LogLevel
	config.LogBackups = cfg.LogBackups
	config.LogAge = cfg.LogAge
	config.LogToStd = cfg.LogToStd
	config.LogEncoding = cfg.LogEncoding
	// push
	config.PushAllInterval = cfg.PushAllInterval
	// grpc
	config.GrpcAddr = cfg.GrpcAddr
	// disable push worker action
	config.DisablePushWorker = cfg.DisablePushWorker
	config.Providers = cfg.Providers
	config.PushAppCodes = cfg.PushAppCodes
	config.EnableLeaderElection = cfg.EnableLeaderElection
	config.MetricsAddr = cfg.MetricsAddr
}

// flag helpers: they read the named flag and fall back to the zero value,
// exactly like the legacy `flag, _ := cmd.Flags().GetX(...)` calls.
func flagString(cmd *cobra.Command, name string) string {
	value, _ := cmd.Flags().GetString(name)
	return value
}

func flagInt(cmd *cobra.Command, name string) int {
	value, _ := cmd.Flags().GetInt(name)
	return value
}

func flagBool(cmd *cobra.Command, name string) bool {
	value, _ := cmd.Flags().GetBool(name)
	return value
}

func flagStringSlice(cmd *cobra.Command, name string) []string {
	value, _ := cmd.Flags().GetStringSlice(name)
	return value
}

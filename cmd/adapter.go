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
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"spotter/internal"
	"spotter/internal/composition"
	infraconfig "spotter/internal/infra/config"
	"spotter/pkg/providers"
)

// adapterRunner is the small command seam used by tests to exercise startup
// composition without entering the long-running server loop.
type adapterRunner interface{ Run() }

// adapterServerStartError marks failures while constructing the running
// server (for example, an unavailable etcd endpoint). The legacy command
// printed these errors and returned successfully; preserving that distinction
// keeps the offline smoke contract while configuration failures remain fatal.
type adapterServerStartError struct {
	err error
}

func (e *adapterServerStartError) Error() string { return e.err.Error() }
func (e *adapterServerStartError) Unwrap() error { return e.err }

func adapterExitCode(err error) int {
	if err == nil {
		return 0
	}
	var startErr *adapterServerStartError
	if errors.As(err, &startErr) {
		return 0
	}
	return 1
}

var (
	loadAdapterConfig   = infraconfig.Load
	buildAdapterRuntime = composition.Build
	newAdapterServer    = func(rt *composition.Runtime) (adapterRunner, error) {
		return internal.NewServerFromDeps(rt)
	}
)

// adapterCmd represents the adapter command
var adapterCmd = &cobra.Command{
	Use:   "adapter",
	Short: "Run the instance adapter",
	Long: `The adapter command is the main entry point of spotter.

It watches the configured providers (Kubernetes clusters and Consul servers),
converts the observed pods/endpoints into instance data, and pushes the
	resulting instance events (incremental and full) to the configured service
	discovery sink (Nacos by default; Atlas only through explicit compatibility
	mode).`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("starting adapter")
		if err := executeAdapter(cmd); err != nil {
			fmt.Println(err.Error())
			if code := adapterExitCode(err); code != 0 {
				os.Exit(code)
			}
			return
		}

		// notify signal
		// c := make(chan os.Signal)
		// signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM)
		// // wait for stop
		// quit := <-c
		// log.Logger.Info("receive quit signal: ", quit)
		// server.Stop()
	},
}

// executeAdapter resolves flags, builds the explicit composition runtime, and
// starts the server. It deliberately has no writes to the deprecated config,
// logger, or notifier globals; compatibility remains available only to legacy
// callers that explicitly invoke their deprecated wrappers.
func executeAdapter(cmd *cobra.Command) error {
	flags := adapterFlags(cmd)
	env, err := cmd.Flags().GetString("env")
	if err != nil {
		return err
	}
	if env != "product" && env != "dev" && env != "test" {
		return fmt.Errorf("invalid env param")
	}
	cfg, err := loadAdapterConfig(env, flags)
	if err != nil {
		return err
	}
	rt, err := buildAdapterRuntime(cfg, composition.Deps{})
	if err != nil {
		return err
	}
	server, err := newAdapterServer(rt)
	if err != nil {
		return &adapterServerStartError{err: err}
	}
	server.Run()
	return nil
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
	adapterCmd.Flags().Bool("atlas-compat", false, "explicitly keep the legacy Atlas sink alongside Nacos")
	adapterCmd.Flags().BoolP("disable-worker", "w", false, "disable push worker, just for testing")
	adapterCmd.Flags().StringSliceP("appcodes", "", []string{}, "only push instances of the appcodes, just for testing")
	adapterCmd.Flags().StringP("metrics-addr", "", ":8090", "the Prometheus metrics address")
	adapterCmd.Flags().String("appcenter-notice-endpoint", "", "appcenter notice endpoint; empty keeps delivery fail-closed")
	adapterCmd.Flags().String("appcenter-notice-auth-token", "", "appcenter notice bearer token")
	adapterCmd.Flags().Int("appcenter-notice-timeout", 0, "appcenter notice timeout seconds; required when endpoint is configured")
	adapterCmd.Flags().Int("appcenter-notice-retries", 0, "appcenter notice maximum retries")
	// --nacos-addr is the additive F5 flag (docs/nacos-sink-plan.md §7.6):
	// empty disables the Nacos sink, so the flag-empty binary behavior is
	// exactly the pre-F5 one. Every flag above is untouched.
	adapterCmd.Flags().StringP("nacos-addr", "", "", "the Nacos OpenAPI address, e.g. 127.0.0.1:18848; empty disables the Nacos sink")
	adapterCmd.Flags().StringSlice("nacos-server-list", nil, "comma-separated Nacos server addresses for failover")
	adapterCmd.Flags().String("nacos-namespace", "", "Nacos namespace ID")
	adapterCmd.Flags().String("nacos-group", "", "Nacos group name")
	adapterCmd.Flags().String("nacos-username", "", "Nacos username")
	adapterCmd.Flags().String("nacos-password", "", "Nacos password")
	adapterCmd.Flags().String("nacos-access-token", "", "Nacos access token")
	adapterCmd.Flags().String("nacos-ca-file", "", "Nacos CA PEM file")
	adapterCmd.Flags().String("nacos-server-name", "", "Nacos TLS server name")
	adapterCmd.Flags().Bool("nacos-insecure-skip-verify", false, "skip Nacos TLS verification")
	adapterCmd.Flags().Int("nacos-timeout", 0, "Nacos request timeout seconds")
	adapterCmd.Flags().String("nacos-transport", "sdk", "Nacos transport: sdk (required in product) or http-compat (test/approved migration rollback only)")
	// --reconcile-source designates the fanout sink whose view the periodic
	// compare reads. Empty resolves to Nacos in the active Nacos-only graph;
	// only explicit --atlas-compat retains the historical Atlas primary.
	adapterCmd.Flags().StringP("reconcile-source", "", "", "reconcile read sink; empty selects Nacos for the default Nacos-only graph")
	// The three additive local-source flags of docs/nacos-sink-plan.md §8.4
	// exist for the local full-stack soak, where the env presets point at
	// unreachable network addresses: they override the preset endpoints for
	// this invocation only, and empty flags keep the preset values verbatim
	// (etcd TLS included). Every flag above is untouched.
	adapterCmd.Flags().StringSliceP("kubeconfig", "", []string{}, "comma-separated kubeconfig paths overriding the preset's KubeConfigPath, e.g. /tmp/soak/kubeconfig; empty keeps the preset")
	adapterCmd.Flags().StringSliceP("consul-addr", "", []string{}, "comma-separated consul addresses overriding the preset, e.g. 127.0.0.1:18500; empty keeps the preset")
	adapterCmd.Flags().StringSliceP("etcd-endpoints", "", []string{}, "comma-separated etcd endpoints overriding the preset, e.g. 127.0.0.1:12379; non-empty resolves the etcd TLS file paths to empty (insecure local mode); empty keeps the preset with TLS")
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
		LogFilePath:              flagString(cmd, "log-file-path"),
		LogSize:                  flagInt(cmd, "log-maxsize"),
		LogLevel:                 flagInt(cmd, "log-level"),
		LogBackups:               flagInt(cmd, "log-backup-number"),
		LogAge:                   flagInt(cmd, "log-age"),
		LogToStd:                 flagBool(cmd, "log-to-std"),
		LogEncoding:              flagString(cmd, "log-encoding"),
		PushAllInterval:          flagInt(cmd, "push-interval"),
		GrpcAddr:                 flagString(cmd, "grpc-addr"),
		EnableAtlasCompatibility: flagBool(cmd, "atlas-compat"),
		DisablePushWorker:        flagBool(cmd, "disable-worker"),
		Providers:                flagStringSlice(cmd, "providers"),
		PushAppCodes:             flagStringSlice(cmd, "appcodes"),
		MetricsAddr:              flagString(cmd, "metrics-addr"),
		AppCenterNoticeEndpoint:  flagString(cmd, "appcenter-notice-endpoint"),
		AppCenterNoticeAuthToken: flagString(cmd, "appcenter-notice-auth-token"),
		AppCenterNoticeTimeout:   flagInt(cmd, "appcenter-notice-timeout"),
		AppCenterNoticeRetries:   flagInt(cmd, "appcenter-notice-retries"),
		NacosAddr:                flagString(cmd, "nacos-addr"), NacosServerList: flagStringSlice(cmd, "nacos-server-list"),
		NacosNamespace: flagString(cmd, "nacos-namespace"), NacosGroup: flagString(cmd, "nacos-group"),
		NacosUsername: flagString(cmd, "nacos-username"), NacosPassword: flagString(cmd, "nacos-password"),
		NacosAccessToken: flagString(cmd, "nacos-access-token"), NacosCAFile: flagString(cmd, "nacos-ca-file"),
		NacosServerName: flagString(cmd, "nacos-server-name"), NacosInsecureSkipVerify: flagBool(cmd, "nacos-insecure-skip-verify"),
		NacosTimeout: flagInt(cmd, "nacos-timeout"), NacosTransport: flagString(cmd, "nacos-transport"),
		ReconcileSource:    flagString(cmd, "reconcile-source"),
		KubeConfigPathFlag: flagStringSlice(cmd, "kubeconfig"),
		ConsulAddrFlag:     flagStringSlice(cmd, "consul-addr"),
		EtcdEndpointsFlag:  flagStringSlice(cmd, "etcd-endpoints"),
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

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
    "github.com/spf13/cobra"
    "spotter/config"
    "spotter/internal"
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
        initAdapterFlags(cmd)
        // init noticer
        env, err := cmd.Flags().GetString("env")
        if env != "product" && env != "dev" && env != "test" {
            panic("invalid env param")
        }
        if err != nil {
            panic(err)
        }
        notice.InitNoticeClient(env)
        if err != nil {
            fmt.Println(err.Error())
        }
        // server init
        server, err := internal.NewServer()
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
    
    // Cobra supports local flags which will only run when this command
    // is called directly, e.g.:
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

func initAdapterFlags(cmd *cobra.Command) {
    // env init
    env, err := cmd.Flags().GetString("env")
    if err != nil {
        panic(err)
    }
    switch env {
    case "product":
        config.InitProd()
    case "dev":
        config.InitDev()
    default:
        config.InitTest()
    }
    // log init
    config.LogFilePath, _ = cmd.Flags().GetString("log-file-path")
    config.LogSize, _ = cmd.Flags().GetInt("log-maxsize")
    config.LogLevel, _ = cmd.Flags().GetInt("log-level")
    config.LogBackups, _ = cmd.Flags().GetInt("log-backup-number")
    config.LogAge, _ = cmd.Flags().GetInt("log-age")
    config.LogToStd, _ = cmd.Flags().GetBool("log-to-std")
    config.LogEncoding, _ = cmd.Flags().GetString("log-encoding")
    if err = log.LoggerInit(); err != nil {
        panic(err)
    }
    // push
    config.PushAllInterval, _ = cmd.Flags().GetInt("push-interval")
    // grpc
    config.GrpcAddr, _ = cmd.Flags().GetString("grpc-addr")
    // disable push worker action
    config.DisablePushWorker, _ = cmd.Flags().GetBool("disable-worker")
    config.Providers, _ = cmd.Flags().GetStringSlice("providers")
    config.PushAppCodes, _ = cmd.Flags().GetStringSlice("appcodes")
    config.EnableLeaderElection, _ = cmd.Flags().GetBool("leader-elect")
    config.MetricsAddr, _ = cmd.Flags().GetString("metrics-addr")
}

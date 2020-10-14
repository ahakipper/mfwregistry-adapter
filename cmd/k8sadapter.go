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
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/core"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/pkg/log"
    "os"
    "os/signal"
    "syscall"

    "github.com/spf13/cobra"
)

// k8sadapterCmd represents the k8sadapter command
var k8sadapterCmd = &cobra.Command{
    Use:   "k8sadapter",
    Short: "A brief description of your command",
    Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
    Run: func(cmd *cobra.Command, args []string) {
        fmt.Println("starting k8sadapter")
        // init flags
        initAdapterFlags(cmd)
        // server init
        server, err := core.NewServer()
        if err != nil {
            panic(err)
        }

        // run
        server.Run()
        // notify signal
        c := make(chan os.Signal)
        signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGUSR1, syscall.SIGUSR2, syscall.SIGTERM)
        // wait for stop
        quit := <-c
        log.Logger.Info("receive quit signal: ", quit)
        server.Stop()
    },
}

func init() {
    rootCmd.AddCommand(k8sadapterCmd)

    // Here you will define your flags and configuration settings.

    // Cobra supports Persistent Flags which will work for this command
    // and all subcommands, e.g.:
    // k8sadapterCmd.PersistentFlags().String("foo", "", "A help for foo")

    // Cobra supports local flags which will only run when this command
    // is called directly, e.g.:
    k8sadapterCmd.Flags().BoolP("leader-elect", "t", true, "whether to enable node election")
    k8sadapterCmd.Flags().StringP("env", "e", "test", "the environment，e.g：test、dev、prod")
}

func initAdapterFlags(cmd *cobra.Command) {
    // env init
    env, err := cmd.Flags().GetString("env")
    if err != nil {
        panic(err)
    }
    switch env {
    case "prod":
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
}

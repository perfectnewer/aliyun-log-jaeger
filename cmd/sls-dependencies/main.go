// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/jaegertracing/jaeger/cmd/env"
	"github.com/jaegertracing/jaeger/cmd/flags"
	"github.com/jaegertracing/jaeger/cmd/sls-dependencies/app"
	"github.com/jaegertracing/jaeger/pkg/config"
	"github.com/jaegertracing/jaeger/pkg/healthcheck"
	pMetrics "github.com/jaegertracing/jaeger/pkg/metrics"
	"github.com/jaegertracing/jaeger/pkg/version"
	aliyunlogStorage "github.com/jaegertracing/jaeger/plugin/storage/aliyunlog"
)

const serviceName = "sls-dependencies"

func main() {
	var signalsChannel = make(chan os.Signal, 0)
	signal.Notify(signalsChannel, os.Interrupt, syscall.SIGTERM)

	aliyunLogFactory := aliyunlogStorage.NewFactory()

	v := viper.New()
	command := &cobra.Command{
		Use:   "sls-dependencies",
		Short: "sls-dependencies the missing dependencies processor for sls. you should use spark-dependencies on other storage backend",
		Long:  `sls-dependencies collect spans from sls logstore, analysis links between services, and stores them for later presentation in UI.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := flags.TryLoadConfigFile(v)
			if err != nil {
				return err
			}

			sFlags := new(flags.SharedFlags).InitFromViper(v)
			logger, err := sFlags.NewLogger(zap.NewProductionConfig())
			if err != nil {
				return err
			}

			FIXME_PORT := 8088
			hc, err := healthcheck.
				New(healthcheck.Unavailable, healthcheck.Logger(logger)).
				Serve(FIXME_PORT)
			if err != nil {
				logger.Fatal("Could not start the health check server.", zap.Error(err))
			}

			mBldr := new(pMetrics.Builder).InitFromViper(v)
			metricsFactory, err := mBldr.CreateMetricsFactory("sls-dependencies")
			if err != nil {
				logger.Fatal("Cannot create metrics factory.", zap.Error(err))
			}

			aliyunLogFactory.InitFromViper(v)
			if err := aliyunLogFactory.Initialize(metricsFactory, logger); err != nil {
				logger.Fatal("Failed to init storage factory", zap.Error(err))
			}

			// FIXME need multi log-project and log storage
			// spanReader, err := aliyunLogFactory.CreateSpanReader()
			// if err != nil {
			// 	logger.Fatal("Failed to create span reader", zap.Error(err))
			// }

			// FIXME TODO remove
			dependencyWriter, err := aliyunLogFactory.CreateDependencyWriter()
			if err != nil {
				logger.Fatal("Failed to create dependency writer", zap.Error(err))
			}

			primaryCfg := aliyunLogFactory.Options.GetPrimary()
			storage, err := app.NewLogStorage(primaryCfg, logger)
			if err != nil {
				logger.Fatal("Failed to create log storage", zap.Error(err))
			}
			calculator := app.NewDependencyCalculator(storage, dependencyWriter)

			option := new(app.StartOption).InitFromViper(v)
			err = calculator.Start(option)
			// health reporter
			// FIXME
			hc.Ready()
			return err
		},
	}

	command.AddCommand(version.Command())
	command.AddCommand(env.Command())

	config.AddFlags(
		v,
		command,
		flags.AddConfigFileFlag,
		flags.AddFlags,
		pMetrics.AddFlags,
		aliyunLogFactory.AddFlags,
		app.AddFlags,
	)

	if error := command.Execute(); error != nil {
		fmt.Println(error.Error())
		os.Exit(1)
	}
}

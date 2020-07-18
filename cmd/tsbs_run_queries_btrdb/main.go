package main

import (
	"fmt"
	"github.com/iznauy/tsbs/internal/utils"
	"github.com/iznauy/tsbs/query"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var url string

var (
	runner *query.BenchmarkRunner
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "localhost:2333", "BTrDB URL.")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	url = viper.GetString("url")

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.BTrDBPool, newProcessor)
}

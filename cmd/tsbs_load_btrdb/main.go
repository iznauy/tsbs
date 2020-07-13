package main

import (
	"bufio"
	"fmt"
	"github.com/iznauy/tsbs/internal/utils"
	"github.com/iznauy/tsbs/load"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
	"time"
)

var fatal = log.Fatalf
var info = log.Printf

var (
	url            string
	backoff        time.Duration
	useBatchInsert bool
)

var loader *load.BenchmarkRunner

func init() {
	var config load.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("url", "localhost:2333", "BTrDB URL.")
	pflag.Duration("backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	pflag.Bool("use-batch-insert", true, "Whether to use batch insert")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	url = viper.GetString("url")
	backoff = viper.GetDuration("backoff")
	useBatchInsert = viper.GetBool("use-batch-insert")

	loader = load.GetBenchmarkRunner(config)
}

type benchmark struct{}

func (b *benchmark) GetPointDecoder(br *bufio.Reader) load.PointDecoder {
	return &decoder{scanner: bufio.NewScanner(br)}
}

func (b *benchmark) GetBatchFactory() load.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(_ uint) load.PointIndexer {
	return &load.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() load.Processor {
	return &processor{}
}

func (b *benchmark) GetDBCreator() load.DBCreator { // btrdb 里面实际上没有数据库的概念，因此不需要专门去创建一个 db creator
	return &dbCreator{}
}

func main() {
	loader.RunBenchmark(&benchmark{}, load.SingleQueue)
}

package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	progressbar "github.com/schollz/progressbar/v3"
	flag "github.com/spf13/pflag"
)

var logger *Logger
var shutdownChan chan struct{}
var allKeys [][]byte

func main() {

	config, err := ParseConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		flag.Usage()
		os.Exit(2)
	}
	logger = NewLogger(config, os.Stderr)
	defer logger.Close()
	logger.Infof("Using following config for benchmark: \n %v \n", config)
	benchmarks := InitializeBenchmarks(config, config.Tests)
	scenarios := NewScenarioSetup(config)

	// benchSetup := NewBenchSetup(config)
	// defer benchSetup.destroy()

	shutdownChan = make(chan struct{}, config.NClients)
	reqIdChan := make(chan int, config.NReqs)
	clients := CreateClients(config, scenarios)

	setupInterruptHandler(config, shutdownChan, clients)
	enableCPUProfile(config)

	for tc, test := range config.Tests {
		// reset()
		wg := new(sync.WaitGroup)
		desc := fmt.Sprintf("[%d/%d] Running cases for %s", (tc + 1), len(config.Tests),
			strings.ToUpper(test))
		pb := progressbar.NewOptions(config.NReqs,
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowBytes(false),
			// progressbar.OptionShowCount(),
			progressbar.OptionSetRenderBlankState(true),
			progressbar.OptionSetDescription(desc),
			progressbar.OptionSetWidth(50))
		//logger.Infof("\n\n")
		// logger.Infof("======== %s ========", strings.ToUpper(test))
		bnchMk := benchmarks[test]
		bnchMk.StartBenchmark()
		go generateReqIds(config.NReqs, config.NClients, reqIdChan)
		for _, cl := range clients {
			wg.Add(1)
			go func(c Client) {
				c.SendReqs(bnchMk, reqIdChan, wg, pb)
			}(cl)
		}
		wg.Wait()
		bnchMk.EndBenchmark()
		bnchMk.Record(test)
		pb.Finish()
		logger.Infof(" Error: %0.2f%%", (float64(bnchMk.errorCount)/float64(config.NReqs))*100)
	}

	bms := make([]*Benchmark, 0, len(benchmarks))
	for _, b := range benchmarks {
		bms = append(bms, b)
	}
	reports := getReporter(config.OutputFormat).ReportResults(getResults(bms))
	fmt.Println(reports)

	pb := progressbar.Default(-1, "Cleaning up keys from redis and closing connections")
	for _, cl := range clients {
		cl.Close()
	}
	pb.Finish()
	logger.Infof("\n\nAll Done")
}

func enableCPUProfile(config *Config) {
	if config.CPUProf {
		c, err := os.Create("cpu_profile.pprof")
		if err != nil {
			logger.Debugf("Cannot create CPU Profile: %s", err.Error())
		}
		pprof.StartCPUProfile(c)
		defer pprof.StopCPUProfile()
	}
}

func setupInterruptHandler(config *Config, shutdown chan<- struct{}, clients []Client) {

	interrupt := make(chan os.Signal, 10)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-interrupt
		for i := 0; i < config.NClients; i++ {
			shutdownChan <- struct{}{}
		}
		time.AfterFunc(30*time.Second, func() {
			os.Exit(2)
		})
		for _, c := range clients {
			c.Close()
		}
	}()
}

func generateReqIds(reqs, clients int, reqIdChan chan<- int) {
	for i := 1; i <= reqs; i++ {
		reqIdChan <- i
	}
	for i := 1; i <= clients; i++ {
		reqIdChan <- -1
	}
}

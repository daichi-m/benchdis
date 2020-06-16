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
)

// Outf prints an output line to stdout
func Outf(format string, data ...interface{}) {
	if !strings.HasSuffix(format, "\n") {
		format = format + "\n"
	}
	fmt.Fprintf(os.Stdout, format, data...)
}

// Infof prints a log line on stderr
func Infof(format string, data ...interface{}) {
	if GlobalConfig.Quiet {
		return
	}

	if !strings.HasSuffix(format, "\n") {
		format = format + "\n"
	}
	fmt.Fprintf(os.Stderr, format, data...)
}

// Debugf prints a log line on stderr if debug mode is enabled
func Debugf(format string, data ...interface{}) {
	if !GlobalConfig.Debug {
		return
	}
	format = "DEBUG: " + format
	Infof(format, data...)
}

func main() {

	GlobalConfig = initConfig()
	initGlobals()
	Infof("Using following config for benchmark: \n %v \n", GlobalConfig)

	wg := new(sync.WaitGroup)
	intHandler := make(chan os.Signal, 1)
	signal.Notify(intHandler, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-intHandler
		Global.ShutDown = true
		time.AfterFunc(30*time.Second, func() {
			os.Exit(2)
		})
		wg.Wait()
		destroyGlobals()
	}()

	clients := make([]*Client, GlobalConfig.NClients)

	if GlobalConfig.CPUProf {
		c, err := os.Create("cpu_profile.pprof")
		if err != nil {
			Debugf("Cannot create CPU Profile: %s", err.Error())
		}
		pprof.StartCPUProfile(c)
		defer pprof.StopCPUProfile()
	}

	for i := range clients {
		clients[i] = GetClient(i, wg)
	}

	for _, test := range GlobalConfig.Tests {
		reset()
		Infof("\n\n")
		Infof("======== %s ========", strings.ToUpper(test))
		bench := GetBenchmark(test)
		bench.Start()
		for _, client := range clients {
			wg.Add(1)
			go client.SendReqs(test)
		}
		wg.Wait()
		bench.End()
		Infof("======== %s ========", strings.ToUpper(test))
		Outf("Test: %s %s\n", strings.ToUpper(test), bench.String())
	}
	destroyGlobals()
	Infof("\n\nAll Done")
}

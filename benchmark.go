package main

import (
	"time"

	"github.com/montanaflynn/stats"
)

// BenchmarkResult is the result for one benchmark test
type BenchmarkResult struct {
	BenchTestName string  `json:"test", yaml:"test"`
	QPS           float64 `json:"qps,omitempty", yaml:"qps,omitempty"`
	MinLatency    float64 `json:"min,omitempty", yaml:"min,omitempty"`
	AvgLatency    float64 `json:"avg,omitempty", yaml:"avg,omitempty"`
	MedianLatency float64 `json:"median,omitempty", yaml:"median,omitempty"`
	P75Latency    float64 `json:"p75,omitempty", yaml:"p75,omitempty"`
	P90Latency    float64 `json:"p90,omitempty",yaml:"p90,omitempty"`
	P99Latency    float64 `json:"p99,omitempty", yaml:"p99,omitempty"`
	MaxLatency    float64 `json:"max,omitempty", yaml:"max,omitempty"`
}

// Benchmark encapsulates all the benchmarking params
type Benchmark struct {
	*Config
	*BenchmarkResult
	BenchTestName string
	Start         time.Time
	End           time.Time
	requests      []int
	latencies     [][]float64
}

// var Benchmarks map[string]*Benchmark

// InitializeBenchmarks initializes the test set of benchmarks
func InitializeBenchmarks(conf *Config, tests []string) map[string]*Benchmark {
	bnchMks := make(map[string]*Benchmark, len(tests))
	for _, t := range tests {
		b := Benchmark{
			Config:          conf,
			BenchmarkResult: &BenchmarkResult{BenchTestName: t},
			BenchTestName:   t,
			requests:        make([]int, conf.NClients),
			latencies:       make([][]float64, conf.NClients),
		}
		bnchMks[t] = &b
	}
	return bnchMks
}

// // GetBenchmark gets the Benchmark object associated with a test
// func GetBenchmark(test string) *Benchmark {

// 	b, ok := Benchmarks[test]
// 	if ok && b != nil {
// 		return b
// 	}

// 	bx := Benchmark{}
// 	Global.Benchmarks[test] = &bx
// 	return &bx
// }

// Mark an execution of a function for benchmarking
func (b *Benchmark) Mark(clientId, reqId int, fn func() []interface{}) []interface{} {
	st := time.Now()
	ret := fn()
	end := time.Now()
	latency := float64(end.Sub(st).Microseconds()) / float64(1000)

	if b.Latency {
		b.markLatency(clientId, reqId, latency)
	}
	if b.Config.QPS {
		b.markQPS(clientId)
	}
	return ret
}

func (b *Benchmark) markLatency(clientId, reqId int, latency float64) {
	latSlc := b.latencies[clientId]
	if latSlc == nil {
		latSlc = make([]float64, 0, b.NReqs/b.NClients)
	}
	latSlc = append(latSlc, latency)
	b.latencies[clientId] = latSlc
}

func (b *Benchmark) markQPS(clientId int) {
	b.requests[clientId]++
}

// Start benchmarking
func (b *Benchmark) StartBenchmark() {
	b.Start = time.Now()
}

// End benchmarking
func (b *Benchmark) EndBenchmark() {
	b.End = time.Now()
}

// Record records the benchmark results from the internal representation into it's
// BenchmarkResult object.
func (b *Benchmark) Record(test string) {

	b.BenchTestName = test
	if b.Config.QPS {
		reqTot := 0
		for _, r := range b.requests {
			reqTot = reqTot + r
		}
		tm := b.End.Sub(b.Start).Seconds()
		qps := float64(reqTot) / tm
		b.BenchmarkResult.QPS = qps
	}

	if b.Latency {

		latencies := make([]float64, 0, b.NReqs)
		for _, l := range b.latencies {
			latencies = append(latencies, l...)
			// Debugf("Latencies list now: %v", latencies)
		}
		rawLatencies := stats.LoadRawData(latencies)
		b.MinLatency, _ = rawLatencies.Min()
		b.AvgLatency, _ = rawLatencies.Mean()
		b.MedianLatency, _ = rawLatencies.Median()
		b.P90Latency, _ = rawLatencies.Percentile(90)
		b.P99Latency, _ = rawLatencies.Percentile(99)
		b.MaxLatency, _ = rawLatencies.Max()
	}
}

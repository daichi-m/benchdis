package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

// Benchmark encapsulates all the benchmarking params
type Benchmark struct {
	mutex     sync.Mutex
	requests  []int
	start     time.Time
	end       time.Time
	latencies [][]float64
}

// BenchmarkResult is the result for one benchmark test
type BenchmarkResult struct {
	Test          string  `json:"test"`
	QPS           float64 `json:"qps,omitempty"`
	MinLatency    float64 `json:"min,omitempty"`
	AvgLatency    float64 `json:"avg,omitempty"`
	MedianLatency float64 `json:"median,omitempty"`
	P75Latency    float64 `json:"p75,omitempty"`
	P90Latency    float64 `json:"p90,omitempty"`
	P99Latency    float64 `json:"p99,omitempty"`
	MaxLatency    float64 `json:"max,omitempty"`
}

var benchResults map[string]BenchmarkResult

// GetBenchmark gets the Benchmark object associated with a test
func GetBenchmark(test string) *Benchmark {

	b, ok := Global.Benchmarks[test]
	if ok && b != nil {
		return b
	}

	bx := Benchmark{}
	Global.Benchmarks[test] = &bx
	return &bx
}

// Mark an execution of a function for benchmarking
func (b *Benchmark) Mark(c, r int, fn func() []interface{}) []interface{} {
	st := time.Now()
	ret := fn()
	end := time.Now()
	latency := float64(end.Sub(st).Microseconds()) / float64(1000)

	if GlobalConfig.Latency {
		b.markLatency(c, r, latency)
	}
	if GlobalConfig.QPS {
		b.markQPS(c, r)
	}
	return ret
}

func (b *Benchmark) markLatency(c, r int, latency float64) {
	lslc := b.latencies[c]
	if lslc == nil {
		lslc = make([]float64, 0, GlobalConfig.NReqs/GlobalConfig.NClients)
	}
	lslc = append(lslc, latency)
	b.latencies[c] = lslc
}

func (b *Benchmark) markQPS(c, r int) {
	b.requests[c]++
}

// Start benchmarking
func (b *Benchmark) Start() {
	b.start = time.Now()
	b.requests = make([]int, GlobalConfig.NClients)
	b.latencies = make([][]float64, GlobalConfig.NClients)
}

// End benchmarking
func (b *Benchmark) End() {
	b.end = time.Now()
}

// Record converts the benchmark into a nice string object
func (b *Benchmark) Record(test string) {

	var br BenchmarkResult
	br.Test = test
	if GlobalConfig.QPS {
		reqTot := 0
		for _, r := range b.requests {
			reqTot = reqTot + r
		}
		tm := b.end.Sub(b.start).Seconds()
		qps := float64(reqTot) / tm
		br.QPS = qps
	}

	if GlobalConfig.Latency {

		latencies := make([]float64, 0, GlobalConfig.NReqs)
		for _, l := range b.latencies {
			latencies = append(latencies, l...)
			// Debugf("Latencies list now: %v", latencies)
		}
		rawLatencies := stats.LoadRawData(latencies)
		br.MinLatency, _ = rawLatencies.Min()
		br.AvgLatency, _ = rawLatencies.Mean()
		br.MedianLatency, _ = rawLatencies.Median()
		br.P90Latency, _ = rawLatencies.Percentile(90)
		br.P99Latency, _ = rawLatencies.Percentile(99)
		br.MaxLatency, _ = rawLatencies.Max()
	}
	if benchResults == nil {
		benchResults = make(map[string]BenchmarkResult)
	}
	benchResults[test] = br
}

// ReportResults reports the benchmark result to stdout
func ReportResults() {
	res := make([]BenchmarkResult, 0, len(benchResults))
	for _, br := range benchResults {
		res = append(res, br)
	}
	j, _ := json.MarshalIndent(res, "", "  ")
	Outf("%s", string(j))
}

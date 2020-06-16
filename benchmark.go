package main

import (
	"fmt"
	"strings"
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

// String converts the benchmark into a nice string object
func (b *Benchmark) String() string {

	sb := strings.Builder{}
	if GlobalConfig.QPS {
		reqTot := 0
		for _, r := range b.requests {
			reqTot = reqTot + r
		}
		tm := b.end.Sub(b.start).Seconds()
		qps := float64(reqTot) / tm
		sb.WriteString(fmt.Sprintf("QPS: %f\t", qps))
	}

	if GlobalConfig.Latency {

		latencies := make([]float64, 0, GlobalConfig.NReqs)
		for _, l := range b.latencies {
			latencies = append(latencies, l...)
			// Debugf("Latencies list now: %v", latencies)
		}
		rawLatencies := stats.LoadRawData(latencies)
		min, _ := rawLatencies.Min()
		mean, _ := rawLatencies.Mean()
		median, _ := rawLatencies.Median()
		p90, _ := rawLatencies.Percentile(90)
		p99, _ := rawLatencies.Percentile(99)
		max, _ := rawLatencies.Max()

		sb.WriteString(fmt.Sprintf("\tMin: %f \t Mean: %f \t Median: %f \t"+
			"P90: %f \t P99: %f \t Max: %f",
			min, mean, median, p90, p99, max))
	}
	sb.WriteString(fmt.Sprintf("\tTotal Time: %v", b.end.Sub(b.start)))

	return sb.String()
}

package main

import (
	"errors"
	"fmt"
	"math"
	"time"

	flag "github.com/spf13/pflag"
)

const MaxDB = 15
const MaxNPool = 1000
const MaxNClients = 100
const MaxRequests = 1000000
const MinPort = 1024
const MaxPort = 65535
const MaxReqSize = 64 * 1024

var SupportedFormats []string = []string{"json", "csv", "yaml", "table"}
var SupportedTests []string = []string{"ping", "set", "get", "incr", "lpush", "rpush", "lpop",
	"rpop", "sadd", "spop", "hset", "hget"}

// Config is the main configuration struct for the benchmark test
type Config struct {
	Host         string
	Port         int
	Auth         string
	Database     int
	Timeout      time.Duration
	NClients     int
	NPool        int
	NReqs        int
	ReqSize      int
	Tests        []string
	Quiet        bool
	Debug        bool
	OutputFormat string
	QPS          bool
	Latency      bool
	CPUProf      bool
	MemProf      bool
}

// ParseConfig will initialize the GlobalConfig instance from command line flags
func ParseConfig() (*Config, error) {
	hostptr := flag.StringP("host", "h", "localhost", "Redis host")
	portptr := flag.IntP("port", "p", 6379, "Redis port")
	authptr := flag.StringP("auth", "a", "", "Password for redis auth")
	databaseptr := flag.IntP("db", "n", 0, "Database number in redis")
	timeoutptr := flag.DurationP("timeout", "x", 10*time.Second, "Connection timeout")
	nclientsptr := flag.IntP("clients", "c", 1, "Number of clients to simulate")
	npoolptr := flag.IntP("pool", "m", 50, "Connection pool size in each client")
	nreqsptr := flag.IntP("requests", "r", 100000, "Number of requests to send")
	reqsizeptr := flag.IntP("data", "d", 50, "Data size in bytes for each request")
	testptr := flag.StringSliceP("tests", "t", []string{"ping", "set", "get", "incr",
		"lpush", "rpush", "lpop", "rpop", "sadd", "spop", "hset", "hget"},
		"Tests to perform")
	quietptr := flag.BoolP("quiet", "q", false, "Quiet mode")
	debugptr := flag.Bool("debug", false, "Debug mode")
	qpsptr := flag.Bool("qps", true, "Track and report QPS")
	latencyptr := flag.Bool("latency", true, "Track and report latency")
	output := flag.StringP("output", "o", "table", "Output format, one of json, csv, yaml, table")

	cpuprofptr := flag.Bool("cpu", false, "Do CPU profile")
	memprofptr := flag.Bool("mem", false, "Do Memory profile")

	flag.Parse()

	poolsize := *npoolptr
	if poolsize < (*nclientsptr)*10 {
		poolsize = int(math.Max(float64((*nclientsptr)*10), float64(MaxNPool)))
	}
	conf := Config{
		Host:         *hostptr,
		Port:         *portptr,
		Auth:         *authptr,
		Database:     *databaseptr,
		Timeout:      *timeoutptr,
		NClients:     *nclientsptr,
		NPool:        poolsize,
		NReqs:        *nreqsptr,
		ReqSize:      *reqsizeptr,
		Tests:        *testptr,
		OutputFormat: *output,
		Quiet:        *quietptr,
		Debug:        *debugptr,
		QPS:          *qpsptr,
		Latency:      *latencyptr,
		CPUProf:      *cpuprofptr,
		MemProf:      *memprofptr,
	}

	_, err := conf.validateConfig()
	return &conf, err
}

func (conf *Config) validateConfig() (bool, error) {
	if conf.Debug == true && conf.Quiet == true {
		return false, errors.New("Cannot set both debug and quiet to be true")
	}
	if conf.Database > MaxDB {
		return false, fmt.Errorf("Maximum database selectable in %d", MaxDB)
	}
	if conf.Port < MinPort && conf.Port > MaxPort {
		return false, fmt.Errorf("The port should be between %d and %d", MinPort, MaxPort)
	}
	if conf.NClients > MaxNClients {
		return false, fmt.Errorf("Maximum %d clients allowed", MaxNClients)
	}
	if conf.NReqs > MaxRequests {
		return false, fmt.Errorf("Maximum %d requests can be sent", MaxRequests)
	}
	if conf.NPool > MaxNPool {
		return false, fmt.Errorf("Max size of the connection pool is %d", MaxNPool)
	}
	if conf.ReqSize > MaxReqSize {
		return false, fmt.Errorf("Maximum %d bytes data can be sent at one shot", MaxReqSize)
	}
	if validOpfmt := searchInList(conf.OutputFormat, SupportedFormats); !validOpfmt {
		return false, fmt.Errorf(
			"Output format %s is not valid, should be one of json, csv, yaml or table",
			conf.OutputFormat)
	}
	tests := make([]string, 0)
	for _, t := range conf.Tests {
		if validTest := searchInList(t, SupportedTests); validTest {
			tests = append(tests, t)
		}
	}
	if len(tests) == 0 {
		return false, fmt.Errorf("No valid tests found in %x", conf.Tests)
	}
	conf.Tests = tests

	return true, nil
}

func (conf Config) String() string {
	prompt := `
	Redis Host: %v,
	Redis Port: %v,
	Redis Auth: %v,
	Redis Database: %v,
	Connection/Read/Write Timeout: %v,
	Number of Clients: %v,
	Pool size: %v,
	Number of requests for each client: %v,
	Data size of request: %v,
	Tests to conduct: %v,
	Output format: %v,
	Quiet Mode: %v,
	Debug Mode: %v,
	Calculate throughput (QPS): %v,
	Calculate latency: %v
`

	auth := func() string {
		if len(conf.Auth) == 0 {
			return "<EMPTY>"
		}
		return "***** (Redacted)"
	}
	str := fmt.Sprintf(prompt,
		conf.Host, conf.Port, auth(), conf.Database, conf.Timeout, conf.NClients, conf.NPool,
		conf.NReqs, conf.ReqSize, conf.Tests, conf.OutputFormat, conf.Quiet, conf.Debug,
		conf.QPS, conf.Latency)
	return str
}

func searchInList(needle string, haystack []string) bool {

	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

/*
// BenchSetup encapsulates all global parameters for the run
type BenchSetup struct {
	ShutDown   bool
	Mutex      *sync.Mutex
	ReqCounter int
	LogTicker  *time.Ticker
	RedisPool  *redis.Pool
	Benchmarks map[string]*Benchmark
	TestSetup  *ScenarioSetup
	Keys       [][]byte
}

func NewBenchSetup(conf *Config) *BenchSetup {
	var setup BenchSetup
	setup.ShutDown = false
	setup.Mutex = new(sync.Mutex)
	setup.Benchmarks = make(map[string]*Benchmark)
	setup.LogTicker = time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {

			case _ = <-setup.LogTicker.C:
				r := setup.ReqCounter
				percent := float32(r*100) / float32(conf.NReqs)
				logger.Infof("%0.2f%% requests completed", percent)
			}
		}
	}()

	setup.ReqCounter = 0
	setup.TestSetup = initializeTests()
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		address := fmt.Sprintf("%s:%d", conf.Host, conf.Port)
		opts := make([]redis.DialOption, 0)
		if conf.Timeout != -1 {
			opts = append(opts, redis.DialConnectTimeout(conf.Timeout))
			opts = append(opts, redis.DialReadTimeout(conf.Timeout))
			opts = append(opts, redis.DialWriteTimeout(conf.Timeout))
			opts = append(opts, redis.DialKeepAlive(conf.Timeout))
		}
		if len(conf.Auth) > 0 {
			opts = append(opts, redis.DialPassword(conf.Auth))
		}

		if conf.Database != 0 {
			opts = append(opts, redis.DialDatabase(conf.Database))
		}
		// Infof("Dial options: %v\n", opts)
		conn, err := redis.Dial("tcp", address, opts...)
		return conn, err
	}, conf.NPool)
	setup.RedisPool = redisPool
	setup.Keys = make([][]byte, 0, conf.NReqs*len(conf.Tests))
	return &setup
}

func (setup *BenchSetup) destroy() {
	setup.ShutDown = true
	setup.Mutex = nil
	setup.Benchmarks = nil
	setup.LogTicker.Stop()
	setup.LogTicker = nil
	setup.ReqCounter = 0
	setup.TestSetup = nil
	conn := setup.RedisPool.Get()

	tick := time.NewTicker(2 * time.Second)
	l := len(setup.Keys)
	go func() {
		for range tick.C {
			logger.Infof("Cleaning up....")
		}
	}()

	keyChan := make(chan []byte, 10000)
	delWg := new(sync.WaitGroup)
	for i := 0; i < 50; i++ {
		go func() {
			conn := setup.RedisPool.Get()
			defer conn.Close()
			for k := range keyChan {
				conn.Do("DEL", k)
			}
			delWg.Done()
		}()
		delWg.Add(1)
	}
	for _, k := range setup.Keys {
		keyChan <- k
	}
	close(keyChan)
	delWg.Wait()
	logger.Infof("Cleaned up %d keys from redis", l)
	conn.Close()
	setup.RedisPool.Close()
	setup.RedisPool = nil
	setup.Keys = nil
	tick.Stop()
}
*/

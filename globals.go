package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	flag "github.com/spf13/pflag"
)

// Config is the main configuration struct for the benchmark test
type Config struct {
	Host     string
	Port     int
	Auth     string
	Database int
	Timeout  time.Duration
	NClients int
	NPool    int
	NReqs    int
	ReqSize  int
	Tests    []string
	Quiet    bool
	Debug    bool
	QPS      bool
	Latency  bool
	CPUProf  bool
	MemProf  bool
}

func (c Config) String() string {
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
	Quiet Mode: %v,
	Debug Mode: %v,
	Idle Mode: %v,
	Calculate throughput (QPS): %v,
	Calculate latency: %v
`

	auth := func() string {
		if len(c.Auth) == 0 {
			return "<EMPTY>"
		}
		return "***** (Redacted)"
	}
	str := fmt.Sprintf(prompt,
		c.Host, c.Port, auth(), c.Database, c.Timeout, c.NClients, c.NPool, c.NReqs, c.ReqSize,
		c.Tests, c.Quiet, c.Debug, c.QPS, c.Latency,
	)
	return str
}

// GlobalConfig is an instance of Config created from command line flags
var GlobalConfig Config

// GlobalParams encapsulates all global parameters for the run
type GlobalParams struct {
	ShutDown   bool
	Mutex      *sync.Mutex
	ReqCounter int
	LogTicker  *time.Ticker
	RedisPool  *redis.Pool
	Benchmarks map[string]*Benchmark
	TestSetup  *testSetup
	Keys       [][]byte
}

// Global is an instance of GlobalParams
var Global GlobalParams

const maxDB = 15
const maxNPool = 1000
const maxNClients = 100
const maxNReqs = 1000000
const minPort = 1024
const maxPort = 65535
const maxReqSize = 64 * 1024

// initConfig will initialize the GlobalConfig instance from command line flags
func initConfig() Config {
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

	cpuprofptr := flag.Bool("cpu", false, "Do CPU profile")
	memprofptr := flag.Bool("mem", false, "Do Memory profile")

	flag.Parse()

	poolsize := *npoolptr
	if poolsize < (*nclientsptr)*10 {
		poolsize = int(math.Max(float64((*nclientsptr)*10), float64(maxNPool)))
	}
	config := Config{
		Host:     *hostptr,
		Port:     *portptr,
		Auth:     *authptr,
		Database: *databaseptr,
		Timeout:  *timeoutptr,
		NClients: *nclientsptr,
		NPool:    poolsize,
		NReqs:    *nreqsptr,
		ReqSize:  *reqsizeptr,
		Tests:    *testptr,
		Quiet:    *quietptr,
		Debug:    *debugptr,
		QPS:      *qpsptr,
		Latency:  *latencyptr,
		CPUProf:  *cpuprofptr,
		MemProf:  *memprofptr,
	}

	_, err := validateConfig(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		flag.Usage()
		os.Exit(2)
	}
	return config
}

func validateConfig(config Config) (bool, error) {
	if config.Debug == true && config.Quiet == true {
		return false, errors.New("Cannot set both debug and quiet to be true")
	}
	if config.Database > maxDB {
		return false, fmt.Errorf("Maximum database selectable in %d", maxDB)
	}
	if config.Port < minPort && config.Port > maxPort {
		return false, fmt.Errorf("The port should be between %d and %d", minPort, maxPort)
	}
	if config.NClients > maxNClients {
		return false, fmt.Errorf("Maximum %d clients allowed", maxNClients)
	}
	if config.NReqs > maxNReqs {
		return false, fmt.Errorf("Maximum %d requests can be sent", maxNReqs)
	}
	if config.NPool > maxNPool {
		return false, fmt.Errorf("Max size of the connection pool is %d", maxNPool)
	}
	if config.ReqSize > maxReqSize {
		return false, fmt.Errorf("Maximum %d bytes data can be sent at one shot", maxReqSize)
	}
	return true, nil
}

func initGlobals() {
	Global.ShutDown = false
	Global.Mutex = new(sync.Mutex)
	Global.Benchmarks = make(map[string]*Benchmark)
	Global.LogTicker = time.NewTicker(2 * time.Second)
	go func() {
		for {
			select {

			case _ = <-Global.LogTicker.C:
				r := Global.ReqCounter
				percent := float32(r*100) / float32(GlobalConfig.NReqs)
				Infof("%0.2f%% requests completed", percent)
			}
		}
	}()

	Global.ReqCounter = 0
	Global.TestSetup = initializeTests()
	redisPool := redis.NewPool(func() (redis.Conn, error) {
		address := fmt.Sprintf("%s:%d", GlobalConfig.Host, GlobalConfig.Port)
		opts := make([]redis.DialOption, 0)
		if GlobalConfig.Timeout != -1 {
			opts = append(opts, redis.DialConnectTimeout(GlobalConfig.Timeout))
			opts = append(opts, redis.DialReadTimeout(GlobalConfig.Timeout))
			opts = append(opts, redis.DialWriteTimeout(GlobalConfig.Timeout))
			opts = append(opts, redis.DialKeepAlive(GlobalConfig.Timeout))
		}
		if len(GlobalConfig.Auth) > 0 {
			opts = append(opts, redis.DialPassword(GlobalConfig.Auth))
		}

		if GlobalConfig.Database != 0 {
			opts = append(opts, redis.DialDatabase(GlobalConfig.Database))
		}
		// Infof("Dial options: %v\n", opts)
		conn, err := redis.Dial("tcp", address, opts...)
		return conn, err
	}, GlobalConfig.NPool)
	Global.RedisPool = redisPool
	Global.Keys = make([][]byte, 0, GlobalConfig.NReqs*len(GlobalConfig.Tests))
}

func destroyGlobals() {
	Global.ShutDown = true
	Global.Mutex = nil
	Global.Benchmarks = nil
	Global.LogTicker.Stop()
	Global.LogTicker = nil
	Global.ReqCounter = 0
	Global.TestSetup = nil
	conn := Global.RedisPool.Get()

	tick := time.NewTicker(2 * time.Second)
	l := len(Global.Keys)
	go func() {
		for range tick.C {
			Infof("Cleaning up....")
		}
	}()

	keyChan := make(chan []byte, 10000)
	delWg := new(sync.WaitGroup)
	for i := 0; i < 50; i++ {
		go func() {
			conn := Global.RedisPool.Get()
			defer conn.Close()
			for k := range keyChan {
				conn.Do("DEL", k)
			}
			delWg.Done()
		}()
		delWg.Add(1)
	}
	for _, k := range Global.Keys {
		keyChan <- k
	}
	close(keyChan)
	delWg.Wait()
	Infof("Cleaned up %d keys from redis", l)
	conn.Close()
	Global.RedisPool.Close()
	Global.RedisPool = nil
	Global.Keys = nil
	tick.Stop()
}

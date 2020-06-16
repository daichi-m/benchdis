package main

import (
	"sync"

	"github.com/gomodule/redigo/redis"
)

// Client is the struct that encapsulates a single client for benchmarking.
// Each client has a pool of size Config.pool and sends Config.nreqs request
type Client struct {
	id   int
	Pool *redis.Pool
	wg   *sync.WaitGroup
}

func nextRequest() int {
	Global.Mutex.Lock()
	defer Global.Mutex.Unlock()
	Global.ReqCounter++
	r := Global.ReqCounter
	if r > GlobalConfig.NReqs {
		return -1
	}
	return r
}

func reset() {
	Global.Mutex.Lock()
	defer Global.Mutex.Unlock()
	Global.ReqCounter = 0
}

// SendReqs sends a total of GlobalConfig.nreqs request to Redis for a give test
func (c *Client) SendReqs(test string) {

	Debugf("Running test %s for Client %d", test, c.id)
	bench := GetBenchmark(test)
	conn := c.Pool.Get()
	defer conn.Close()

	r := nextRequest()
	for r != -1 && Global.ShutDown == false {
		Debugf("Received request: %d in client #%d", r, c.id)

		cmd, args, err := ToRedis(test, r)
		if err != nil {
			Infof("Error in converting test to Redis format: %s", err.Error())
			return
		}
		intfArgs := make([]interface{}, len(args))
		for i := range args {
			intfArgs[i] = args[i]
		}
		ret := bench.Mark(c.id, r, func() []interface{} {
			res, err := conn.Do(cmd, intfArgs...)
			return []interface{}{res, err}
		})

		if ret[1] != nil {
			err = ret[1].(error)
			Debugf("Could not send request #%d to redis due to %s", r, err.Error())
		}
		if r%100 == 0 {
			Debugf("Client #%d has sent %d requests", c.id, r)
		}
		r = nextRequest()
	}
	c.wg.Done()
}

// GetClient creates and returns a Client object to be used for testing
func GetClient(id int, wg *sync.WaitGroup) *Client {
	client := Client{
		id:   id,
		Pool: Global.RedisPool,
		wg:   wg,
	}
	return &client
}

// StartLogTicker starts the logging ticker

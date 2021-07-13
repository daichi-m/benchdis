package main

import (
	"fmt"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/schollz/progressbar/v3"
)

// RedisClient is the struct that encapsulates a single client for benchmarking.
// Each client has a pool of size Config.pool and sends Config.nreqs request
type RedisClient struct {
	Pool      *redis.Pool
	scenarios *ScenarioSetup
	keyspace  [][]byte
}

type Client struct {
	RedisClient
	id int
}

// func nextRequest() int {
// 	Global.Mutex.Lock()
// 	defer Global.Mutex.Unlock()
// 	Global.ReqCounter++
// 	r := Global.ReqCounter
// 	if r > GlobalConfig.NReqs {
// 		return -1
// 	}
// 	return r
// }

// func reset() {
// 	Global.Mutex.Lock()
// 	defer Global.Mutex.Unlock()
// 	Global.ReqCounter = 0
// }

// SendReqs sends a total of GlobalConfig.nreqs request to Redis for a given test
func (c *Client) SendReqs(bench *Benchmark, reqIdChan <-chan int, wg *sync.WaitGroup,
	pb *progressbar.ProgressBar) {

	logger.Debugf("Running test %s for Client %d", bench.BenchTestName, c.id)
	conn := c.Pool.Get()
	defer conn.Close()
	defer wg.Done()

	logger.Debugf("Client #%d up and runnnig", c.id)
	for {
		var reqId int
		select {
		case reqId = <-reqIdChan:
		case <-shutdownChan:
			// close clients
			break
		}
		if reqId == -1 {
			break
		}
		logger.Debugf("Received request: %d in client #%d", reqId, c.id)

		cmd, args, err := c.scenarios.ToRedis(bench.BenchTestName, reqId)
		if err != nil {
			logger.Errorf("Error in converting test to Redis format: %s", err.Error())
			return
		}
		if len(args) > 0 {
			key := args[0].([]byte)
			c.keyspace = append(c.keyspace, key)
		}

		intfArgs := make([]interface{}, len(args))
		for i := range args {
			intfArgs[i] = args[i]
		}
		ret := bench.Mark(c.id, reqId, func() []interface{} {
			res, err := conn.Do(cmd, intfArgs...)
			return []interface{}{res, err}
		})

		if ret[1] != nil {
			err = ret[1].(error)
			logger.Debugf("Could not send request #%d to redis due to %s", reqId, err.Error())
		}
		if reqId%100 == 0 {
			logger.Debugf("Client #%d has sent %d requests", c.id, reqId)
			pb.Set(reqId)
		}
	}
	// wg.Done()
}

func (c *Client) Close() {
	cl := c.RedisClient.Pool.Get()
	for _, key := range c.keyspace {
		cl.Do("DEL", key)
	}
	cl.Close()
	c.RedisClient.Pool.Close()
}

// CreateClients creates and returns a Client object to be used for testing
func CreateClients(conf *Config, scen *ScenarioSetup) []Client {

	redisClient := RedisClient{
		Pool:      createRedisPool(conf),
		scenarios: scen,
	}
	clients := make([]Client, 0, conf.NClients)
	for id := 0; id < conf.NClients; id++ {
		clients = append(clients, Client{
			id:          id,
			RedisClient: redisClient,
		})
	}
	return clients
}

func createRedisPool(conf *Config) *redis.Pool {
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
	return redisPool
}

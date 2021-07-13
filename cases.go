package main

import (
	"bytes"
	"fmt"
	"html/template"
	"math/rand"
	"strings"
	"time"
)

type params struct {
	Tag     string
	RandInt int
	Data    string
	Key     string
}

type redisCmd struct {
	cmd  string
	args []string
}

type ScenarioSetup struct {
	*Config
	scenarios map[string]redisCmd
	integers  []int
	keys      []string
	tag       string
	data      []byte
}

// NewScenarioSetup initializes all the test case scenarios
func NewScenarioSetup(conf *Config) *ScenarioSetup {
	sc := ScenarioSetup{Config: conf}
	rand.Seed(time.Now().UnixNano())
	sc.initializeScenarios()
	sc.integers = make([]int, conf.NReqs)
	for i := range sc.integers {
		sc.integers[i] = rand.Int()
	}

	sc.keys = make([]string, conf.NReqs)
	for i := range sc.keys {
		sc.keys[i] = randomKey(rand.Intn(15))
	}

	sc.tag = fmt.Sprintf("%d", time.Now().UTC().Unix())
	sc.data = make([]byte, conf.ReqSize)
	rand.Read(sc.data)
	return &sc
}

func (sc *ScenarioSetup) initializeScenarios() {
	sc.scenarios = make(map[string]redisCmd)

	// PING
	sc.scenarios["ping"] = redisCmd{
		cmd:  "PING",
		args: []string{},
	}

	// SET key value
	sc.scenarios["set"] = redisCmd{
		cmd:  "SET",
		args: []string{"key:{{.Tag}}:{{.RandInt}}", "{{.Data}}"},
	}

	// GET key value
	sc.scenarios["get"] = redisCmd{
		cmd:  "GET",
		args: []string{"key:{{.Tag}}:{{.RandInt}}"},
	}

	// INCR key
	sc.scenarios["incr"] = redisCmd{
		cmd:  "INCR",
		args: []string{"ctr:{{.Tag}}:{{.RandInt}}"},
	}

	// LPUSH key value
	sc.scenarios["lpush"] = redisCmd{
		cmd:  "LPUSH",
		args: []string{"llist:{{.Tag}}", "{{.Data}}"},
	}

	// RPUSH key value
	sc.scenarios["rpush"] = redisCmd{
		cmd:  "RPUSH",
		args: []string{"rlist:{{.Tag}}", "{{.Data}}"},
	}

	// LPOP key
	sc.scenarios["lpop"] = redisCmd{
		cmd:  "LPOP",
		args: []string{"llist:{{.Tag}}"},
	}

	// LPOP key
	sc.scenarios["rpop"] = redisCmd{
		cmd:  "RPOP",
		args: []string{"rlist:{{.Tag}}"},
	}

	// SADD key value
	sc.scenarios["sadd"] = redisCmd{
		cmd:  "SADD",
		args: []string{"set:{{.Tag}}", "{{.Data}}"},
	}

	// SPOP key
	sc.scenarios["spop"] = redisCmd{
		cmd:  "SPOP",
		args: []string{"set:{{.Tag}}"},
	}

	// HSET key hkey hval
	sc.scenarios["hset"] = redisCmd{
		cmd:  "HSET",
		args: []string{"hash:{{.Tag}}", "{{.Key}}", "{{.Data}}"},
	}

	// HGET key hkey
	sc.scenarios["hget"] = redisCmd{
		cmd:  "HGET",
		args: []string{"hash:{{.Tag}}", "{{.Key}}"},
	}
}

func randomKey(size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		ascii := rune(97 + rand.Intn(26))
		builder.WriteString(string(ascii))
	}
	return builder.String()
}

// ToRedis converts a test into the Redis command and data that can be fed to
// redis.Connection.Do method
func (sc *ScenarioSetup) ToRedis(test string, reqIdx int) (string, []interface{}, error) {
	rcmd, ok := sc.scenarios[test]
	if !ok {
		return "", nil, fmt.Errorf("No test scenario %s", test)
	}
	idx := reqIdx % sc.NReqs
	p := params{
		Tag:     sc.tag,
		RandInt: sc.integers[idx],
		Key:     sc.keys[idx],
		Data:    string(sc.data),
	}

	res := make([]interface{}, len(rcmd.args))
	for i, t := range rcmd.args {
		tmplt, _ := template.New("arg").Parse(t)
		bbuf := bytes.Buffer{}
		err := tmplt.Execute(&bbuf, p)
		if err != nil {
			logger.Debugf("Error in detemplatizing: %s", err.Error())
		}
		bts := bbuf.Bytes()
		res[i] = bts
	}
	return rcmd.cmd, res, nil
}

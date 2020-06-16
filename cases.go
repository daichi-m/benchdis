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
	NextKey string
}

type redisCmd struct {
	cmd  string
	args []string
}

type testSetup struct {
	scenarios map[string]redisCmd
	integers  []int
	keys      []string
	tag       string
	data      []byte
}

// InitializeTests initializes all the test case scenarios
func initializeTests() *testSetup {
	var ts testSetup
	rand.Seed(time.Now().UnixNano())
	initializeScenarios(&ts)
	ts.integers = make([]int, GlobalConfig.NReqs)
	for i := range ts.integers {
		ts.integers[i] = rand.Int()
	}

	ts.keys = make([]string, GlobalConfig.NReqs)
	for i := range ts.keys {
		ts.keys[i] = randomKey(rand.Intn(15))
	}

	ts.tag = fmt.Sprintf("%d", time.Now().UTC().Unix())
	ts.data = make([]byte, GlobalConfig.ReqSize)
	rand.Read(ts.data)
	return &ts
}

func initializeScenarios(ts *testSetup) {
	ts.scenarios = make(map[string]redisCmd)

	// PING
	ts.scenarios["ping"] = redisCmd{
		cmd:  "PING",
		args: []string{},
	}

	// SET key value
	ts.scenarios["set"] = redisCmd{
		cmd:  "SET",
		args: []string{"key:{{.Tag}}:{{.RandInt}}", "{{.Data}}"},
	}

	// GET key value
	ts.scenarios["get"] = redisCmd{
		cmd:  "GET",
		args: []string{"key:{{.Tag}}:{{.RandInt}}"},
	}

	// INCR key
	ts.scenarios["incr"] = redisCmd{
		cmd:  "INCR",
		args: []string{"ctr:{{.Tag}}:{{.RandInt}}"},
	}

	// LPUSH key value
	ts.scenarios["lpush"] = redisCmd{
		cmd:  "LPUSH",
		args: []string{"llist:{{.Tag}}", "{{.Data}}"},
	}

	// RPUSH key value
	ts.scenarios["rpush"] = redisCmd{
		cmd:  "RPUSH",
		args: []string{"rlist:{{.Tag}}", "{{.Data}}"},
	}

	// LPOP key
	ts.scenarios["lpop"] = redisCmd{
		cmd:  "LPOP",
		args: []string{"llist:{{.Tag}}"},
	}

	// LPOP key
	ts.scenarios["rpop"] = redisCmd{
		cmd:  "RPOP",
		args: []string{"rlist:{{.Tag}}"},
	}

	// SADD key value
	ts.scenarios["sadd"] = redisCmd{
		cmd:  "SADD",
		args: []string{"set:{{.Tag}}", "{{.Data}}"},
	}

	// SPOP key
	ts.scenarios["spop"] = redisCmd{
		cmd:  "SPOP",
		args: []string{"set:{{.Tag}}"},
	}

	// HSET key hkey hval
	ts.scenarios["hset"] = redisCmd{
		cmd:  "HSET",
		args: []string{"hash:{{.Tag}}", "{{.NextKey}}", "{{.Data}}"},
	}

	// HGET key hkey
	ts.scenarios["hget"] = redisCmd{
		cmd:  "HGET",
		args: []string{"hash:{{.Tag}}", "{{.NextKey}}"},
	}
}

func randomKey(size int) string {
	builder := strings.Builder{}
	for i := 0; i < size; i++ {
		ascii := 97 + rand.Intn(26)
		builder.WriteString(string(ascii))
	}
	return builder.String()
}

// ToRedis converts a test into the Redis command and data that can be fed to
// redis.Connection.Do method
func ToRedis(test string, reqIdx int) (string, []interface{}, error) {
	rcmd, ok := Global.TestSetup.scenarios[test]
	if !ok {
		return "", nil, fmt.Errorf("No test scenario %s", test)
	}
	idx := reqIdx % GlobalConfig.NReqs
	p := params{
		Tag:     Global.TestSetup.tag,
		RandInt: Global.TestSetup.integers[idx],
		NextKey: Global.TestSetup.keys[idx],
		Data:    string(Global.TestSetup.data),
	}

	res := make([]interface{}, len(rcmd.args))
	for i, t := range rcmd.args {
		tmplt, _ := template.New("arg").Parse(t)
		bbuf := bytes.Buffer{}
		err := tmplt.Execute(&bbuf, p)
		if err != nil {
			Debugf("Error in detemplatizing: %s", err.Error())
		}
		bts := bbuf.Bytes()
		res[i] = bts
	}

	if len(res) > 0 {
		key := res[0].([]byte)
		Global.Keys = append(Global.Keys, key)
	}

	return rcmd.cmd, res, nil
}

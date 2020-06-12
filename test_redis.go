package main

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

func main() {
	pool := redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", "localhost:6379", redis.DialDatabase(2))
	}, 5)
	c := pool.Get()
	v, err := c.Do("PING")
	fmt.Printf("Value: %+v, Error: %+v \n", v, err)
}

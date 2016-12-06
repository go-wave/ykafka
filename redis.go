package ykafka

import "github.com/garyburd/redigo/redis"

var RedisPool *redis.Pool

func InitRedis(cfg *RedisCfg) {
	RedisPool = &redis.Pool{
		MaxIdle:   cfg.PoolMaxIdle,
		MaxActive: cfg.PoolMaxActive,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.Dns)
			return c, err
		},
	}
}

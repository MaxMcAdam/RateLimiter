package ratelimiter

import (
	"github.com/go-redis/redis/v8"
)

type RedisBackend struct {
	client *redis.Client
}

func NewRedisBackend(config *RLConfig) RedisBackend {
	return RedisBackend{}
}

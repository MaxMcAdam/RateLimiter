package ratelimiter

import (
	"time"

	redis "github.com/redis/go-redis/v9"
)

type RedisBackend interface {
	CheckAndIncrementBucket(key string) (bool, error)
}

type SingleRedisBackend struct {
	client *redis.Client
}

func NewSingleRedisBackend(config *RLConfig) SingleRedisBackend {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password
		DB:       0,  // default DB

		// Connection pool settings
		PoolSize:        10,               // Max connections in pool
		MinIdleConns:    2,                // Min idle connections to maintain
		MaxIdleConns:    5,                // Max idle connections
		ConnMaxIdleTime: 30 * time.Minute, // Close idle connections after 30min
		ConnMaxLifetime: time.Hour,        // Close connections after 1 hour

		// Timeouts
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,

		// Retries
		MaxRetries:      3,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
	})
	return SingleRedisBackend{client}
}

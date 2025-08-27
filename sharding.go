package ratelimiter

import redis "github.com/redis/go-redis/v9"

// Manual sharding implementation
type ShardSelector interface {
	GetShard(key string) *redis.Client
}

type ModuloShardSelector struct {
	shards []*redis.Client
}

func NewModuloShardSelector(shards []*redis.Client) *ModuloShardSelector {
	return &ModuloShardSelector{shards: shards}
}

func (s *ModuloShardSelector) GetShard(key string) *redis.Client {
	// Simple FNV hash
	hash := uint32(2166136261)
	for _, b := range []byte(key) {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return s.shards[hash%uint32(len(s.shards))]
}

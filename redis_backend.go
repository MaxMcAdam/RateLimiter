package ratelimiter

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// Low-level abstraction - mirrors Redis operations, no business logic
// Implements redis.Scripter interface for compatibility with redis.Script
type RedisOperations interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	NewScript(script string) *redis.Script
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Incr(ctx context.Context, key string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Pipeline() redis.Pipeliner
	Close() error
}

// Single Redis implementation
type SingleRedisBackend struct {
	client *redis.Client
}

func NewSingleRedisBackend(config *SingleRedisConfig) *SingleRedisBackend {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Address,
		Password: config.Password,
		DB:       config.DB,

		// Connection pool settings
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxIdleTime: config.ConnMaxIdleTime,
		ConnMaxLifetime: config.ConnMaxLifetime,

		// Timeouts
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,

		// Retries
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
	})

	return &SingleRedisBackend{client: client}
}

func (s *SingleRedisBackend) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.client.Eval(ctx, script, keys, args...)
}

func (s *SingleRedisBackend) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return s.client.EvalRO(ctx, script, keys, args...)
}

func (s *SingleRedisBackend) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.client.EvalSha(ctx, sha1, keys, args...)
}

func (s *SingleRedisBackend) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return s.client.EvalShaRO(ctx, sha1, keys, args...)
}

func (s *SingleRedisBackend) NewScript(script string) *redis.Script {
	return redis.NewScript(script)
}

func (s *SingleRedisBackend) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return s.client.ScriptExists(ctx, hashes...)
}

func (s *SingleRedisBackend) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return s.client.ScriptLoad(ctx, script)
}

func (s *SingleRedisBackend) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return s.client.SetNX(ctx, key, value, expiration)
}

func (s *SingleRedisBackend) Incr(ctx context.Context, key string) *redis.IntCmd {
	return s.client.Incr(ctx, key)
}

func (s *SingleRedisBackend) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return s.client.Expire(ctx, key, expiration)
}

func (s *SingleRedisBackend) Pipeline() redis.Pipeliner {
	return s.client.Pipeline()
}

func (s *SingleRedisBackend) Close() error {
	return s.client.Close()
}

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

type ManualShardBackend struct {
	selector ShardSelector
}

func NewManualShardBackend(config *ManualShardConfig) *ManualShardBackend {
	shards := make([]*redis.Client, len(config.Addresses))
	for i, addr := range config.Addresses {
		shards[i] = redis.NewClient(&redis.Options{
			Addr:            addr,
			Password:        config.Password,
			PoolSize:        config.PoolSize,
			ConnMaxIdleTime: config.ConnMaxIdleTime,
			ConnMaxLifetime: config.ConnMaxLifetime,
			DialTimeout:     config.DialTimeout,
			ReadTimeout:     config.ReadTimeout,
			WriteTimeout:    config.WriteTimeout,
		})
	}

	var selector ShardSelector
	switch config.ShardingStrategy {
	case "consistent":
		selector = NewConsistentHashSelector(shards, config.VirtualNodes)
	default:
		selector = NewModuloShardSelector(shards)
	}

	return &ManualShardBackend{selector: selector}
}

func (m *ManualShardBackend) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if len(keys) == 0 {
		panic("ManualShardBackend.Eval requires at least one key for sharding")
	}
	shard := m.selector.GetShard(keys[0])
	return shard.Eval(ctx, script, keys, args...)
}

func (m *ManualShardBackend) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if len(keys) == 0 {
		panic("ManualShardBackend.EvalRO requires at least one key for sharding")
	}
	shard := m.selector.GetShard(keys[0])
	return shard.EvalRO(ctx, script, keys, args...)
}

func (m *ManualShardBackend) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if len(keys) == 0 {
		panic("ManualShardBackend.EvalSha requires at least one key for sharding")
	}
	shard := m.selector.GetShard(keys[0])
	return shard.EvalSha(ctx, sha1, keys, args...)
}

func (m *ManualShardBackend) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if len(keys) == 0 {
		panic("ManualShardBackend.EvalShaRO requires at least one key for sharding")
	}
	shard := m.selector.GetShard(keys[0])
	return shard.EvalShaRO(ctx, sha1, keys, args...)
}

func (s *ManualShardBackend) NewScript(script string) *redis.Script {
	return redis.NewScript(script)
}

func (m *ManualShardBackend) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	// For manual sharding, check script existence on first shard
	// In production, you might want to check all shards
	shard := m.selector.GetShard("default")
	return shard.ScriptExists(ctx, hashes...)
}

func (m *ManualShardBackend) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	// For manual sharding, load script on first shard
	// In production, you might want to load on all shards
	shard := m.selector.GetShard("default")
	return shard.ScriptLoad(ctx, script)
}

func (m *ManualShardBackend) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	shard := m.selector.GetShard(key)
	return shard.SetNX(ctx, key, value, expiration)
}

func (m *ManualShardBackend) Incr(ctx context.Context, key string) *redis.IntCmd {
	shard := m.selector.GetShard(key)
	return shard.Incr(ctx, key)
}

func (m *ManualShardBackend) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	shard := m.selector.GetShard(key)
	return shard.Expire(ctx, key, expiration)
}

func (m *ManualShardBackend) Pipeline() redis.Pipeliner {
	// For simplicity, return pipeline from first shard
	// In production, you'd need cross-shard pipeline support
	return m.selector.GetShard("default").Pipeline()
}

func (m *ManualShardBackend) Close() error {
	// Close all shards
	return nil // Implementation needed
}

// Redis Cluster implementation
type ClusterRedisBackend struct {
	cluster *redis.ClusterClient
}

func NewClusterRedisBackend(config *ClusterRedisConfig) *ClusterRedisBackend {
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:           config.Addresses,
		Password:        config.Password,
		PoolSize:        config.PoolSize,
		ConnMaxLifetime: config.ConnMaxLifetime,
	})

	return &ClusterRedisBackend{cluster: cluster}
}

func (c *ClusterRedisBackend) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return c.cluster.Eval(ctx, script, keys, args...)
}

func (c *ClusterRedisBackend) EvalRO(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return c.cluster.EvalRO(ctx, script, keys, args...)
}

func (c *ClusterRedisBackend) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return c.cluster.EvalSha(ctx, sha1, keys, args...)
}

func (c *ClusterRedisBackend) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return c.cluster.EvalShaRO(ctx, sha1, keys, args...)
}

func (s *ClusterRedisBackend) NewScript(script string) *redis.Script {
	return redis.NewScript(script)
}

func (c *ClusterRedisBackend) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return c.cluster.ScriptExists(ctx, hashes...)
}

func (c *ClusterRedisBackend) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return c.cluster.ScriptLoad(ctx, script)
}

func (c *ClusterRedisBackend) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	return c.cluster.SetNX(ctx, key, value, expiration)
}

func (c *ClusterRedisBackend) Incr(ctx context.Context, key string) *redis.IntCmd {
	return c.cluster.Incr(ctx, key)
}

func (c *ClusterRedisBackend) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return c.cluster.Expire(ctx, key, expiration)
}

func (c *ClusterRedisBackend) Pipeline() redis.Pipeliner {
	return c.cluster.Pipeline()
}

func (c *ClusterRedisBackend) Close() error {
	return c.cluster.Close()
}

// Factory function
func NewRedisBackend(config *RLConfig) (RedisOperations, error) {
	switch config.RedisMode {
	case "single":
		return NewSingleRedisBackend(config.SingleRedisConfig), nil
	case "manual_sharding":
		return NewManualShardBackend(config.ManualShardConfig), nil
	case "cluster":
		return NewClusterRedisBackend(config.ClusterRedisConfig), nil
	default:
		return nil, fmt.Errorf("unsupported Redis mode: %s", config.RedisMode)
	}
}

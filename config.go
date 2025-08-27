package ratelimiter

import (
	"encoding/json"
	"os"
	"time"
)

const (
	GRANULARITY_IP       = "ip"
	GRANULARITY_ENDPOINT = "endpoint"
	GRANULARITY_TIER     = "tiered"

	REDIS_KEY_PREFIX = "rate_limit"
	DEFAULT_TIER     = "default"
)

type RLConfig struct {
	Granularity        string              `yaml:"granularity"` // Options are "ip", "endpoint", or "tiered"
	EndpointConfig     *EndpointConfig     `yaml:"endpoint_config"`
	TieredConfig       *TieredConfig       `yaml:"tiered_config"`
	IPConfig           BucketSpec          `yaml:"ip_spec"`
	RedisMode          string              `yaml:"redis_mode"` // Options are "single", "manual_sharding", "cluster"
	SingleRedisConfig  *SingleRedisConfig  `yaml:"single_redis_config"`
	ManualShardConfig  *ManualShardConfig  `yaml:"manual_shard_config"`
	ClusterRedisConfig *ClusterRedisConfig `yaml:"cluster_redis_config"`
}

type TieredConfig struct {
	TierSpec       map[string]BucketSpec `yaml:"tier_spec"`
	EndpointToTier map[string]string     `yaml:"endpoint_to_tier"`
	DefaultSpec    BucketSpec            `yaml:"default_spec"`
}

func (c TieredConfig) GetBucketSpecForEndpoint(endpoint string) BucketSpec {
	if tier, ok := c.EndpointToTier[endpoint]; ok {
		if spec, ok := c.TierSpec[tier]; ok {
			return spec
		}
	}
	return c.DefaultSpec
}

type EndpointConfig struct {
	PerEndpointSpecs map[string]BucketSpec `yaml:"per_endpoint_specs"`
	DefaultSpec      BucketSpec            `yaml:"default_spec"`
}

func (c *EndpointConfig) GetBucketSpecForEndpoint(endpoint string) BucketSpec {
	if spec, ok := c.PerEndpointSpecs[endpoint]; ok {
		return spec
	}
	return c.DefaultSpec
}

type BucketSpec struct {
	Capacity   uint64 `yaml:"capacity"`    // Max tokens per bucket
	RefillRate uint64 `yaml:"refill_rate"` // Tokens per minute refill rate
}

func (c *RLConfig) GetBucketSpecForEndpoint(endpoint string) BucketSpec {
	if c.EndpointConfig != nil {
		return c.EndpointConfig.GetBucketSpecForEndpoint(endpoint)
	} else if c.TieredConfig != nil {
		return c.TieredConfig.GetBucketSpecForEndpoint(endpoint)
	}
	return c.IPConfig
}

// Redis configuration structs
type SingleRedisConfig struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

	// Connection pool settings
	PoolSize        int           `yaml:"pool_size"`
	MinIdleConns    int           `yaml:"min_idle_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`

	// Timeouts
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`

	// Retries
	MaxRetries      int           `yaml:"max_retries"`
	MinRetryBackoff time.Duration `yaml:"min_retry_backoff"`
	MaxRetryBackoff time.Duration `yaml:"max_retry_backoff"`
}

type ManualShardConfig struct {
	Addresses        []string `yaml:"addresses"`
	Password         string   `yaml:"password"`
	ShardingStrategy string   `yaml:"sharding_strategy"` // "modulo" or "consistent"
	VirtualNodes     int      `yaml:"virtual_nodes"`     // for consistent hashing

	// Connection pool settings (applied to each shard)
	PoolSize        int           `yaml:"pool_size"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	DialTimeout     time.Duration `yaml:"dial_timeout"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
}

type ClusterRedisConfig struct {
	Addresses       []string      `yaml:"addresses"`
	Password        string        `yaml:"password"`
	PoolSize        int           `yaml:"pool_size"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

func ReadInConfig(path string) (*RLConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var cfg RLConfig
	var cfgBytes []byte
	_, err = f.Read(cfgBytes)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(cfgBytes, &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

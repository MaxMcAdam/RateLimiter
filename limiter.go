package ratelimiter

import (
	"context"
	"time"
)

type Ratelimiter struct {
	persistence *RedisBackend
	config      *RLConfig
}

type RequestContext struct {
	ClientIP  string
	Endpoint  string
	Method    string    // For method-based limiting
	UserAgent string    // Simple bot detection
	Timestamp time.Time // For pattern analysis
}

type RLConfig struct {
	Granularity       string              `yaml:"granularity"` // Options are "ip", "endpoint", or "tiered"
	EndpointConfig    *EndpointConfig     `yaml:"endpoint_config"`
	TieredConfig      *TieredConfig       `yaml:"tiered_config"`
	IPConfig          *BucketSpec         `yaml:"ip_spec"`
	RedisMode         string              `yaml:"redis_mode"` // Options are "single", "manual_sharding", "cluster"
	SingleRedisConfig *SingleRedisConfig  `yaml:"single_redis_config"`
	ManualShardConfig *ManualShardConfig  `yaml:"manual_shard_config"`
	ClusterRedisCnfig *ClusterRedisConfig `yaml:"cluster_redis_config"`
}

type TieredConfig struct {
	TierSpec       map[string]BucketSpec `yaml:"tier_spec"`
	EndpointToTier map[string]string     `yaml:"endpoint_to_tier"`
	DefaultSpec    BucketSpec
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
	} else {
		return c.DefaultSpec
	}
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
	return *c.IPConfig
}

func New(config RLConfig) Ratelimiter {
	return Ratelimiter{}
}

func (rl *Ratelimiter) Allow(ctx *context.Context, reqCtx RequestContext) (bool, error) {
	endpoint := reqCtx.Endpoint

	refillRate := rl.config.GetRefillForEndpoint()

	return true, nil
}

package ratelimiter

import (
	"context"
	"fmt"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// Token bucket Lua script - business logic centralized here
const tokenBucketScript = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])  -- tokens per second
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Get current bucket state
local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

-- Calculate tokens to add based on time elapsed
local elapsed = math.max(0, now - last_refill)
local tokens_to_add = math.floor(elapsed * refill_rate)
tokens = math.min(capacity, tokens + tokens_to_add)

-- Check if we can fulfill the request
if tokens >= requested then
    tokens = tokens - requested
    -- Update bucket state
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)  -- TTL for cleanup
    return {1, tokens}  -- {allowed, remaining_tokens}
else
    -- Update last_refill time even if denied
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    redis.call('EXPIRE', key, 3600)
    return {0, tokens}  -- {denied, current_tokens}
end
`

type Ratelimiter struct {
	redis       RedisOperations
	config      *RLConfig
	tokenScript *redis.Script
	key         func(string, string, *RLConfig) string // accepts ip then endpoint and returns the redis key approriate for the granularity
}

type RequestContext struct {
	ClientIP  string
	Endpoint  string
	Method    string    // For method-based limiting
	UserAgent string    // Simple bot detection
	Timestamp time.Time // For pattern analysis
}

func New(config *RLConfig) (*Ratelimiter, error) {
	redis, err := NewRedisBackend(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis backend: %w", err)
	}

	var key func(string, string, *RLConfig) string

	switch config.Granularity {
	case GRANULARITY_IP:
		key = func(ip string, endpoint string, cfg *RLConfig) string {
			return fmt.Sprintf("%s:%s", REDIS_KEY_PREFIX, ip)
		}
	case GRANULARITY_ENDPOINT:
		key = func(ip string, endpoint string, cfg *RLConfig) string {
			return fmt.Sprintf("%s:%s:%s", REDIS_KEY_PREFIX, ip, endpoint)
		}
	case GRANULARITY_TIER:
		key = func(ip string, endpoint string, cfg *RLConfig) string {
			if tier, ok := cfg.TieredConfig.EndpointToTier[endpoint]; ok {
				return fmt.Sprintf("%s:%s", tier, ip)
			}
			return fmt.Sprintf("%s:%s", DEFAULT_TIER, ip)
		}
	}

	return &Ratelimiter{
		redis:       redis,
		config:      config,
		tokenScript: redis.NewScript(tokenBucketScript),
		key:         key,
	}, nil
}

func (rl *Ratelimiter) Allow(ctx context.Context, reqCtx RequestContext) (bool, error) {
	// Generate rate limit key based on granularity
	key := rl.generateKey(reqCtx)

	// Get bucket specification for this request
	bucketSpec := rl.config.GetBucketSpecForEndpoint(reqCtx.Endpoint)

	// Convert refill rate from tokens/minute to tokens/second
	refillRatePerSecond := float64(bucketSpec.RefillRate) / 60.0

	// Execute token bucket algorithm
	result, err := rl.tokenScript.Run(ctx, rl.redis, []string{key},
		bucketSpec.Capacity,
		refillRatePerSecond,
		1, // requesting 1 token
		time.Now().Unix()).Result()

	if err != nil {
		return false, fmt.Errorf("redis script execution failed: %w", err)
	}

	// Parse result
	values := result.([]interface{})
	allowed := values[0].(int64) == 1
	// remaining := values[1].(int64) // Could be used for response headers

	return allowed, nil
}

func (rl *Ratelimiter) generateKey(reqCtx RequestContext) string {
	return rl.key(reqCtx.ClientIP, reqCtx.Endpoint, rl.config)
}

// Optional: Method for getting current bucket status without consuming tokens
func (rl *Ratelimiter) GetBucketStatus(ctx context.Context, reqCtx RequestContext) (tokens int64, err error) {
	key := rl.generateKey(reqCtx)

	// Simple script to get current tokens without consuming
	statusScriptText := `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

local elapsed = math.max(0, now - last_refill)
local tokens_to_add = math.floor(elapsed * refill_rate)
tokens = math.min(capacity, tokens + tokens_to_add)

return tokens
`

	bucketSpec := rl.config.GetBucketSpecForEndpoint(reqCtx.Endpoint)
	refillRatePerSecond := float64(bucketSpec.RefillRate) / 60.0

	// Use EvalRO since this is read-only operation
	result, err := rl.redis.EvalRO(ctx, statusScriptText, []string{key},
		bucketSpec.Capacity,
		refillRatePerSecond,
		time.Now().Unix()).Result()

	if err != nil {
		return 0, err
	}

	return result.(int64), nil
}

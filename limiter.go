package ratelimiter

import (
	"context"
	"time"
)

type Ratelimiter struct {
	persistence *RedisBackend
}

type RequestContext struct {
	ClientIP  string
	Endpoint  string
	Method    string    // For method-based limiting
	UserAgent string    // Simple bot detection
	Timestamp time.Time // For pattern analysis
}

type RLConfig struct {
}

func New(config RLConfig) Ratelimiter {
	return Ratelimiter{}
}

func (rl *Ratelimiter) Allow(ctx context.Context, reqCtx RequestContext) {

}

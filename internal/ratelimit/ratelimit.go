// Package ratelimit implements a fixed-window counter backed by Redis —
// reusing the Redis instance this project already runs for upload sessions,
// rather than adding a new dependency for what's a small, well-understood
// problem. Fixed-window is simpler than sliding-window/token-bucket and
// good enough for "stop one IP from hammering the upload endpoint"; it
// allows a burst at window boundaries, which is an accepted trade-off here.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	client *redis.Client
	limit  int
	window time.Duration
}

func New(addr string, limit int, window time.Duration) *Limiter {
	return &Limiter{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		limit:  limit,
		window: window,
	}
}

// Allow increments key's counter for the current window and reports whether
// the caller is still under the limit. The first increment in a window also
// sets the window's expiry, so the counter resets automatically — no
// separate cleanup job needed.
func (l *Limiter) Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error) {
	redisKey := fmt.Sprintf("ratelimit:%s", key)

	count, err := l.client.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, 0, fmt.Errorf("incr: %w", err)
	}
	if count == 1 {
		if err := l.client.Expire(ctx, redisKey, l.window).Err(); err != nil {
			return false, 0, fmt.Errorf("expire: %w", err)
		}
	}
	if count > int64(l.limit) {
		ttl, err := l.client.TTL(ctx, redisKey).Result()
		if err != nil {
			return false, 0, fmt.Errorf("ttl: %w", err)
		}
		return false, ttl, nil
	}
	return true, 0, nil
}

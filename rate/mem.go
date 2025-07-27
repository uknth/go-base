package rate

import (
	"sync"

	"golang.org/x/time/rate"
)

type inMemoryLimiter struct {
	mu       sync.RWMutex
	limit    float64
	burst    int
	limiters map[Key]*rate.Limiter
}

func (il *inMemoryLimiter) Allow(key Key) bool {
	if il.limit == 0 {
		return false
	}

	il.mu.RLock()
	limiter, ok := il.limiters[key]
	il.mu.RUnlock()
	if ok {
		return limiter.Allow()
	}

	il.mu.Lock()
	defer il.mu.Unlock()

	// Double-check after acquiring write lock
	limiter, ok = il.limiters[key]
	if !ok {
		limiter = rate.NewLimiter(rate.Limit(il.limit), il.burst)
		il.limiters[key] = limiter
	}

	return limiter.Allow()
}

// NewInMemoryLimiter returns a new in memory rate limiter using golang.org/x/time/rate
// `limit` defines the number of events per second that are allowed to happen and
// it permits bursts of at most `burst` number of events.
// The limits aren't configurable for each limiter, instead we only allow limiter
// to be created with a fixed `limit` and `burst`
func NewInMemoryLimiter(limit float64, burst int) Limiter {
	return &inMemoryLimiter{
		limit:    limit,
		burst:    burst,
		limiters: make(map[Key]*rate.Limiter),
	}
}

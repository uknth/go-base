package rate

import (
	"fmt"
	"testing"
	"time"
)

func TestNewInMemoryLimiter(t *testing.T) {
	limiter := NewInMemoryLimiter(1, 2)
	if limiter == nil {
		t.Fatal("Expected non-nil limiter")
	}
}

func TestAllow_BurstAndRate(t *testing.T) {
	tests := []struct {
		name      string
		limit     float64
		burst     int
		requests  int
		sleep     time.Duration
		wantAllow []bool
	}{
		{
			name:      "burst=2, limit=2, 3 requests, no wait",
			limit:     2,
			burst:     2,
			requests:  3,
			sleep:     0,
			wantAllow: []bool{true, true, false},
		},
		{
			name:      "burst=1, limit=1, 2 requests, no wait",
			limit:     1,
			burst:     1,
			requests:  2,
			sleep:     0,
			wantAllow: []bool{true, false},
		},
		{
			name:      "burst=5, limit=2, 6 requests, no wait",
			limit:     2,
			burst:     5,
			requests:  6,
			sleep:     0,
			wantAllow: []bool{true, true, true, true, true, false},
		},
		{
			name:      "burst=3, limit=1, 3 requests, wait for recovery",
			limit:     1,
			burst:     3,
			requests:  3,
			sleep:     1100 * time.Millisecond,
			wantAllow: []bool{true, true, true}, // after wait, should allow again
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter := NewInMemoryLimiter(tt.limit, tt.burst)
			key := Key("user1")
			for i := 0; i < len(tt.wantAllow); i++ {
				allowed := limiter.Allow(key)
				if allowed != tt.wantAllow[i] {
					t.Errorf("Request %d: got %v, want %v", i+1, allowed, tt.wantAllow[i])
				}
			}
			if tt.sleep > 0 {
				time.Sleep(tt.sleep)
				allowed := limiter.Allow(key)
				if !allowed {
					t.Error("Expected event to be allowed after waiting for rate recovery")
				}
			}
		})
	}
}

func TestAllow_MultipleKeysIndependence(t *testing.T) {
	limiter := NewInMemoryLimiter(2, 2)
	key1 := Key("user1")
	key2 := Key("user2")

	// Both should allow burst
	if !limiter.Allow(key1) {
		t.Error("Expected key1 first event to be allowed")
	}
	if !limiter.Allow(key2) {
		t.Error("Expected key2 first event to be allowed")
	}
	if !limiter.Allow(key1) {
		t.Error("Expected key1 second event to be allowed (burst)")
	}
	if !limiter.Allow(key2) {
		t.Error("Expected key2 second event to be allowed (burst)")
	}
	// Both should now be rate limited
	if limiter.Allow(key1) {
		t.Error("Expected key1 to be rate limited after burst")
	}
	if limiter.Allow(key2) {
		t.Error("Expected key2 to be rate limited after burst")
	}
}

func TestAllow_ZeroBurst(t *testing.T) {
	limiter := NewInMemoryLimiter(1, 0)
	key := Key("user1")
	if limiter.Allow(key) {
		t.Error("Expected event to be rate limited with zero burst")
	}
}

func TestAllow_ZeroLimit(t *testing.T) {
	limiter := NewInMemoryLimiter(0, 1)
	key := Key("user1")
	if limiter.Allow(key) {
		t.Error("Expected event to be rate limited with zero limit")
	}
}

func TestAllow_HighBurst(t *testing.T) {
	limiter := NewInMemoryLimiter(1, 10)
	key := Key("user1")
	for i := 0; i < 10; i++ {
		if !limiter.Allow(key) {
			t.Errorf("Expected event %d to be allowed in high burst", i+1)
		}
	}
	if limiter.Allow(key) {
		t.Error("Expected event to be rate limited after high burst consumed")
	}
}

func TestAllow_RaceCondition(t *testing.T) {
	// Test for race condition when multiple goroutines access the same key simultaneously
	limiter := NewInMemoryLimiter(100, 10)
	key := Key("race_test_key")

	// Number of concurrent goroutines
	numGoroutines := 100
	results := make(chan bool, numGoroutines)

	// Start multiple goroutines that all try to access the same key
	for i := 0; i < numGoroutines; i++ {
		go func() {
			results <- limiter.Allow(key)
		}()
	}

	// Collect all results
	allowedCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-results {
			allowedCount++
		}
	}

	// With limit=100 and burst=10, we should allow at most 10 requests
	// The race condition might cause more than 10 to be allowed if multiple
	// limiters are created for the same key
	if allowedCount > 10 {
		t.Errorf("Race condition detected: %d requests allowed, expected at most 10", allowedCount)
	}
}

func TestAllow_ConcurrentDifferentKeys(t *testing.T) {
	// Test for race condition when multiple goroutines access different keys
	limiter := NewInMemoryLimiter(10, 5)
	numGoroutines := 50
	results := make(chan bool, numGoroutines)

	// Start multiple goroutines with different keys
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := Key(fmt.Sprintf("key_%d", id))
			results <- limiter.Allow(key)
		}(i)
	}

	// Collect all results
	allowedCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-results {
			allowedCount++
		}
	}

	// Each key should allow at most 5 requests (burst), so total should be at most 50
	if allowedCount > 50 {
		t.Errorf("Unexpected number of allowed requests: %d, expected at most 50", allowedCount)
	}
}

func TestAllow_StressTest(t *testing.T) {
	// Stress test with high concurrency and mixed operations
	limiter := NewInMemoryLimiter(10, 5) // Lower limits for more realistic test
	numGoroutines := 50
	iterations := 5
	results := make(chan int, numGoroutines)

	// Start multiple goroutines that repeatedly access the same key
	for i := 0; i < numGoroutines; i++ {
		go func() {
			key := Key("stress_test_key")
			allowed := 0
			for j := 0; j < iterations; j++ {
				if limiter.Allow(key) {
					allowed++
				}
			}
			results <- allowed
		}()
	}

	// Collect all results
	totalAllowed := 0
	for i := 0; i < numGoroutines; i++ {
		totalAllowed += <-results
	}

	// With limit=10 and burst=5, and 50 goroutines making 5 requests each,
	// we should allow at most 5 requests total (burst limit for the single key)
	if totalAllowed > 5 {
		t.Errorf("Stress test failed: %d total requests allowed, expected at most 5", totalAllowed)
	}
}

func BenchmarkInMemoryLimiter_Allow(b *testing.B) {
	limiter := NewInMemoryLimiter(1000, 100)
	key := Key("benchmark_key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.Allow(key)
	}
}

func BenchmarkInMemoryLimiter_ConcurrentAllow(b *testing.B) {
	limiter := NewInMemoryLimiter(1000, 100)
	key := Key("benchmark_key")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Allow(key)
		}
	})
}

func BenchmarkInMemoryLimiter_DifferentKeys(b *testing.B) {
	limiter := NewInMemoryLimiter(1000, 100)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := Key(fmt.Sprintf("key_%d", i))
			limiter.Allow(key)
			i++
		}
	})
}

func TestAllow_DoubleCheckedLocking(t *testing.T) {
	// Test that double-checked locking prevents duplicate limiter creation
	limiter := NewInMemoryLimiter(100, 10)
	key := Key("double_check_key")

	numGoroutines := 100
	results := make(chan bool, numGoroutines)

	// All goroutines try to access the same key simultaneously
	// Only one should create the limiter, others should reuse it
	for i := 0; i < numGoroutines; i++ {
		go func() {
			results <- limiter.Allow(key)
		}()
	}

	// Collect results
	allowedCount := 0
	for i := 0; i < numGoroutines; i++ {
		if <-results {
			allowedCount++
		}
	}

	// Should respect burst limit
	if allowedCount > 10 {
		t.Errorf("Double-checked locking failed: %d requests allowed, expected at most 10", allowedCount)
	}
}

func TestAllow_MixedConcurrentOperations(t *testing.T) {
	// Test mixed operations: same key, different keys, and edge cases
	limiter := NewInMemoryLimiter(50, 5)

	numWorkers := 20
	results := make(chan int, numWorkers)

	// Workers with different behaviors
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			allowed := 0
			for j := 0; j < 10; j++ {
				var key Key
				switch workerID % 3 {
				case 0:
					// Same key for all workers of this type
					key = Key("shared_key")
				case 1:
					// Unique key per worker
					key = Key(fmt.Sprintf("worker_%d", workerID))
				case 2:
					// Mixed keys
					key = Key(fmt.Sprintf("mixed_%d", j%3))
				}

				if limiter.Allow(key) {
					allowed++
				}
			}
			results <- allowed
		}(i)
	}

	// Collect results
	totalAllowed := 0
	for i := 0; i < numWorkers; i++ {
		totalAllowed += <-results
	}

	// With the mixed pattern, we expect multiple keys to have their burst consumed
	// This is mainly testing that no race conditions occur
	t.Logf("Mixed concurrent operations: %d total requests allowed", totalAllowed)
}

func TestAllow_EdgeCaseConcurrency(t *testing.T) {
	// Test edge cases under concurrency
	testCases := []struct {
		name  string
		limit float64
		burst int
	}{
		{"zero_limit", 0, 5},
		{"zero_burst", 10, 0},
		{"high_limit", 10000, 1000},
		{"fractional_limit", 2.5, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limiter := NewInMemoryLimiter(tc.limit, tc.burst)
			key := Key(fmt.Sprintf("edge_%s", tc.name))

			numGoroutines := 50
			results := make(chan bool, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					results <- limiter.Allow(key)
				}()
			}

			allowedCount := 0
			for i := 0; i < numGoroutines; i++ {
				if <-results {
					allowedCount++
				}
			}

			// For zero limit, nothing should be allowed
			if tc.limit == 0 && allowedCount > 0 {
				t.Errorf("Zero limit should allow nothing, got %d", allowedCount)
			}

			// For zero burst, nothing should be allowed initially
			if tc.burst == 0 && allowedCount > 0 {
				t.Errorf("Zero burst should allow nothing initially, got %d", allowedCount)
			}

			// For positive limits and bursts, should respect burst limit
			if tc.limit > 0 && tc.burst > 0 && allowedCount > tc.burst {
				t.Errorf("Allowed %d requests, expected at most %d (burst)", allowedCount, tc.burst)
			}
		})
	}
}

package catalog

import (
	"errors"
	"strconv"
	"time"
)

// Header names carrying retry metadata between topics.
const (
	HeaderAttempts  = "x-retry-attempts"
	HeaderOrigTopic = "x-retry-original-topic"
	HeaderLastError = "x-retry-last-error"
)

// MaxAttempts bounds how many processing attempts one event gets before it
// is dead-lettered.
const MaxAttempts = 3

// BackoffPolicy delays retry consumption attempts progressively so
// downstream saturation recovers before events are reprocessed.
type BackoffPolicy struct {
	Base time.Duration
	Max  time.Duration
}

// DefaultBackoff is the platform's standard retry pacing.
var DefaultBackoff = BackoffPolicy{Base: 500 * time.Millisecond, Max: 5 * time.Second}

// ErrNonRetryable marks errors that retries cannot fix; the policy
// dead-letters them on the first failure.
var ErrNonRetryable = errors.New("non-retryable failure")

// LinearRetryPolicy is the platform's retry verdict: failures get another
// attempt while the budget lasts, then the event is dead-lettered. The
// attempt budget turns "retry forever" into "retry a bounded number of
// times, then surface the failure for humans".
type LinearRetryPolicy struct{}

// OnFailure implements RetryPolicy.
func (LinearRetryPolicy) OnFailure(attempt int, err error) RetryAction {
	if errors.Is(err, ErrNonRetryable) {
		return RetryActionDeadLetter
	}
	if attempt >= MaxAttempts-1 {
		return RetryActionDeadLetter
	}
	return RetryActionRetry
}

// DelayFor returns the exponential wait before the next attempt.
func (b BackoffPolicy) DelayFor(attempt int) time.Duration {
	if attempt < 0 {
		return b.Base
	}
	delay := b.Base
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay > b.Max {
			return b.Max
		}
	}
	return delay
}

// EncodeAttempts renders an attempt count as a header value.
func EncodeAttempts(n int) []byte { return []byte(strconv.Itoa(n)) }

// DecodeAttempts parses an attempt count from a header value, defaulting
// to zero when absent or malformed.
func DecodeAttempts(v []byte) int {
	if len(v) == 0 {
		return 0
	}
	n, err := strconv.Atoi(string(v))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

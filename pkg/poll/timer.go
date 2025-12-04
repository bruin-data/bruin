package poll

import (
	"math"
	"time"
)

type Timer struct {
	BaseDuration time.Duration
	RetryCount   int
	MaxRetry     int
}

func (p *Timer) Duration() time.Duration {
	return p.BaseDuration * time.Duration(
		math.Pow(2, float64(p.RetryCount)),
	)
}

func (p *Timer) Reset() {
	p.RetryCount = 0
}

func (p *Timer) Increase() {
	if p.MaxRetry > 0 && p.RetryCount < p.MaxRetry {
		p.RetryCount++
	}
}

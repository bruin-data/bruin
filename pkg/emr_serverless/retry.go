package emr_serverless //nolint

import (
	"math"
	"time"
)

type PollTimer struct {
	BaseDuration time.Duration
	RetryCount   int
	MaxRetry     int
}

func (p *PollTimer) Duration() time.Duration {
	return p.BaseDuration * time.Duration(
		math.Pow(2, float64(p.RetryCount)),
	)
}

func (p *PollTimer) Reset() {
	p.RetryCount = 0
}

func (p *PollTimer) Increase() {
	if p.MaxRetry > 0 && p.RetryCount < p.MaxRetry {
		p.RetryCount += 1
	}
}

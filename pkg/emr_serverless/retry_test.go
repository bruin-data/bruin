package emr_serverless //nolint

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPollTimer(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Retry  int
		Expect time.Duration
	}

	const maxRetry = 5

	testCases := []testCase{
		{
			0, time.Second,
		},
		{
			1, 2 * time.Second,
		},
		{
			2, 4 * time.Second,
		},
		{
			3, 8 * time.Second,
		},
		{
			4, 16 * time.Second,
		},
		{
			5, 32 * time.Second,
		},
		{
			6, 32 * time.Second, // max retry should limit it to 32
		},
	}
	for _, testCase := range testCases {
		timer := &PollTimer{
			BaseDuration: time.Second,
			MaxRetry:     maxRetry,
		}
		for range testCase.Retry {
			timer.Increase()
		}
		assert.Equal(
			t,
			testCase.Expect,
			timer.Duration(),
		)
	}
}

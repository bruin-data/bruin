package diff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDateTime(t *testing.T) {
	t.Parallel()

	// timePtr is a helper function to create a pointer to a time.Time
	timePtr := func(t time.Time) *time.Time {
		return &t
	}

	tests := []struct {
		name     string
		input    interface{}
		wantTime *time.Time
		wantErr  bool
	}{
		{
			name:     "time.Time input",
			input:    time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC),
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339 string",
			input:    "2023-01-15T10:30:00Z",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339 without timezone",
			input:    "2017-11-11T07:04:52",
			wantTime: timePtr(time.Date(2017, 11, 11, 7, 4, 52, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "RFC3339Nano string",
			input:    "2023-01-15T10:30:00.123456789Z",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 123456789, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "standard datetime string",
			input:    "2023-01-15 10:30:00",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "datetime with microseconds",
			input:    "2023-01-15 10:30:00.123456",
			wantTime: timePtr(time.Date(2023, 1, 15, 10, 30, 0, 123456000, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "date only string",
			input:    "2023-01-15",
			wantTime: timePtr(time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "time only string",
			input:    "10:30:00",
			wantTime: timePtr(time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "actual bigquery example",
			input:    "2000-01-01 00:00:00 +0000 UTC",
			wantTime: timePtr(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
			wantErr:  false,
		},
		{
			name:     "empty string",
			input:    "",
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "invalid datetime string",
			input:    "invalid-datetime",
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "nil input",
			input:    nil,
			wantTime: nil,
			wantErr:  true,
		},
		{
			name:     "int input",
			input:    12345,
			wantTime: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotTime, err := ParseDateTime(tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, gotTime)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, gotTime)
			assert.Equal(t, tt.wantTime, gotTime)
		})
	}
}

package date

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_parseTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			input:    "2023-03-16 10:30:00",
			expected: time.Date(2023, 0o3, 16, 10, 30, 0, 0, time.UTC),
		},
		{
			input:    "2023-03-16",
			expected: time.Date(2023, 0o3, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			input:    "2023/03/16",
			expected: time.Time{},
			wantErr:  true,
		},
		{
			input:    "2023-03-16 10:30:00 PM",
			expected: time.Time{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			actual, err := ParseTime(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestConvertPythonDateFormat(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "%Y-%m-%d %H:%M:%S",
			expected: "2006-01-02 15:04:05",
		},
		{
			input:    "%d/%m/%Y",
			expected: "02/01/2006",
		},
		{
			input:    "%a, %d %b %Y %H:%M:%S %z",
			expected: "Mon, 02 Jan 2006 15:04:05 Z",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			actual := ConvertPythonDateFormatToGolang(tt.input)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

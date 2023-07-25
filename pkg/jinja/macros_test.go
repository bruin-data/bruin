package jinja

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_DateAdd(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "add 5 days to a date string with default output format",
			args:     []interface{}{"2022-12-31", 5},
			expected: "2023-01-05",
		},
		{
			name:     "add 10 days to a date string with custom output format",
			args:     []interface{}{"2022-12-31", 10, "2006/01/02"},
			expected: "2023/01/10",
		},
		{
			name:     "add -3 days to a datetime string with custom input and output formats",
			args:     []interface{}{"2022-12-31 12:34:56", -3, "02/01/06 15:04:05", "2006-01-02 15:04:05"},
			expected: "28/12/22 12:34:56",
		},
		{
			name:     "invalid arguments - fewer than 2",
			args:     []interface{}{},
			expected: "at least 2 arguments needed for date_add",
		},
		{
			name:     "invalid arguments - date format",
			args:     []interface{}{"12/31/2022", 10},
			expected: "invalid date format:12/31/2022",
		},
		{
			name:     "invalid arguments - output format",
			args:     []interface{}{"2022-12-31", 10, 123},
			expected: "invalid output format",
		},
	}

	for _, tc := range testCases {
		tt := tc
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualDate := dateAdd(tt.args...)
			assert.Equal(t, tt.expected, actualDate)
		})
	}
}

func Test_DateFormat(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		args     []interface{}
		expected string
		wantErr  bool
	}{
		{
			name:     "invalid arguments - fewer than 2",
			args:     []interface{}{},
			expected: "invalid arguments for date_format",
		},
		{
			name:     "format the date",
			args:     []interface{}{"2023-01-10", "%Y/%m/%d"},
			expected: "2023/01/10",
		},
		{
			name:     "format with minutes",
			args:     []interface{}{"2023-01-10 12:34:56", "%Y/%m/%d %H-%M"},
			expected: "2023/01/10 12-34",
		},
		{
			name:     "format with custom input format",
			args:     []interface{}{"2023/01/10", "%Y-%m-%d %H-%M", "%Y/%m/%d"},
			expected: "2023-01-10 00-00",
		},
		{
			name:     "invalid arguments - date format",
			args:     []interface{}{"12/31/2022", "%Y-%m-%d %H-%M", "abc-xyz"},
			expected: "invalid date format:abc-xyz",
		},
		{
			name:     "invalid arguments - output format",
			args:     []interface{}{"2022-12-31", 123},
			expected: "invalid output format",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actualDate := dateFormat(tt.args...)
			assert.Equal(t, tt.expected, actualDate)
		})
	}
}

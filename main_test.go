package main

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v3"
)

func TestPrintUnhandledError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "plain error",
			err:      errors.New("something went wrong"),
			expected: "error: something went wrong\n",
		},
		{
			name: "exit coder",
			err:  cli.Exit("already handled", 1),
		},
		{
			name: "wrapped exit coder",
			err:  fmt.Errorf("wrapped: %w", cli.Exit("already handled", 1)),
		},
		{
			name: "nil error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var output bytes.Buffer
			printUnhandledError(tt.err, &output)

			assert.Equal(t, tt.expected, output.String())
		})
	}
}

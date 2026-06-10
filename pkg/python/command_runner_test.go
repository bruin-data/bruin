package python

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumePipe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		passthrough bool
		expected    string
	}{
		{
			name:     "simple newline gets prefix",
			input:    "hello world\n",
			expected: ">> hello world\n",
		},
		{
			name:     "multiple lines each get prefix",
			input:    "line1\nline2\n",
			expected: ">> line1\n>> line2\n",
		},
		{
			name:     "no trailing newline still flushes",
			input:    "hello",
			expected: ">> hello\n",
		},
		{
			name:     "windows line endings collapse to newline",
			input:    "hello\r\nworld\r\n",
			expected: ">> hello\n>> world\n",
		},
		{
			name:     "empty input produces nothing",
			input:    "",
			expected: "",
		},
		{
			// Without passthrough, ReadLine only strips the carriage return
			// directly before a newline; interior \r are kept but the whole
			// thing is wrapped as one prefixed line ending in \n. The terminal
			// never gets a chance to redraw progressively, which is why bars
			// appear stalled until completion. This documents existing behavior.
			name:     "carriage returns wrapped as single prefixed line without passthrough",
			input:    "progress 0%\rprogress 50%\rprogress 100%\n",
			expected: ">> progress 0%\rprogress 50%\rprogress 100%\n",
		},
		{
			// A trailing \r before \n is stripped by ReadLine.
			name:     "trailing carriage return before newline is stripped",
			input:    "progress 100%\r\n",
			expected: ">> progress 100%\n",
		},
		{
			name:        "passthrough preserves bytes verbatim",
			input:       "hello world\n",
			passthrough: true,
			expected:    "hello world\n",
		},
		{
			name:        "passthrough preserves carriage returns",
			input:       "progress 0%\rprogress 50%\rprogress 100%\n",
			passthrough: true,
			expected:    "progress 0%\rprogress 50%\rprogress 100%\n",
		},
		{
			name:        "passthrough tqdm-style output untouched",
			input:       "  0%|          | 0/100\r 50%|#####     | 50/100\r100%|##########| 100/100\n",
			passthrough: true,
			expected:    "  0%|          | 0/100\r 50%|#####     | 50/100\r100%|##########| 100/100\n",
		},
		{
			name:        "passthrough adds no prefix and no trailing newline",
			input:       "partial",
			passthrough: true,
			expected:    "partial",
		},
		{
			name:        "passthrough empty input produces nothing",
			input:       "",
			passthrough: true,
			expected:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			reader := strings.NewReader(tt.input)

			err := consumePipe(reader, &buf, tt.passthrough)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, buf.String())
		})
	}
}

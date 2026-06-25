package dataprocserverless

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitLines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		buf       string
		flush     bool
		wantLines []string
		wantRest  string
	}{
		{
			name:      "complete lines, no trailing partial",
			buf:       "alpha\nbeta\n",
			flush:     false,
			wantLines: []string{"alpha", "beta"},
			wantRest:  "",
		},
		{
			name:      "trailing partial is held back when not flushing",
			buf:       "alpha\nbeta\ngam",
			flush:     false,
			wantLines: []string{"alpha", "beta"},
			wantRest:  "gam",
		},
		{
			name:      "trailing partial is emitted on flush",
			buf:       "alpha\ngam",
			flush:     true,
			wantLines: []string{"alpha", "gam"},
			wantRest:  "",
		},
		{
			name:      "carriage returns are trimmed",
			buf:       "alpha\r\nbeta\r\n",
			flush:     false,
			wantLines: []string{"alpha", "beta"},
			wantRest:  "",
		},
		{
			name:      "blank lines are preserved",
			buf:       "a\n\nb\n",
			flush:     false,
			wantLines: []string{"a", "", "b"},
			wantRest:  "",
		},
		{
			name:      "empty buffer yields nothing",
			buf:       "",
			flush:     false,
			wantLines: nil,
			wantRest:  "",
		},
		{
			name:      "empty buffer on flush yields nothing",
			buf:       "",
			flush:     true,
			wantLines: nil,
			wantRest:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lines, rest := splitLines([]byte(tt.buf), tt.flush)
			assert.Equal(t, tt.wantLines, lines)
			assert.Equal(t, tt.wantRest, string(rest))
		})
	}
}

func TestGCSLogConsumer_SetOutputURI(t *testing.T) {
	t.Parallel()

	t.Run("parses bucket and prefix", func(t *testing.T) {
		t.Parallel()
		c := newGCSLogConsumer(t.Context(), nil)
		c.SetOutputURI("gs://my-bucket/path/to/jobs/srvls-batch-123/driveroutput")
		assert.True(t, c.resolved)
		assert.Equal(t, "my-bucket", c.bucket)
		assert.Equal(t, "path/to/jobs/srvls-batch-123/driveroutput", c.prefix)
	})

	t.Run("empty URI is ignored", func(t *testing.T) {
		t.Parallel()
		c := newGCSLogConsumer(t.Context(), nil)
		c.SetOutputURI("")
		assert.False(t, c.resolved)
	})

	t.Run("first non-empty URI wins", func(t *testing.T) {
		t.Parallel()
		c := newGCSLogConsumer(t.Context(), nil)
		c.SetOutputURI("gs://first/driveroutput")
		c.SetOutputURI("gs://second/driveroutput")
		assert.Equal(t, "first", c.bucket)
	})

	t.Run("not resolved returns no lines", func(t *testing.T) {
		t.Parallel()
		c := newGCSLogConsumer(t.Context(), nil)
		assert.Nil(t, c.Next())
		assert.Nil(t, c.Flush())
	})
}

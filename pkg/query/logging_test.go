package query

import (
	"bytes"
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/stretchr/testify/assert"
)

func TestLogQueryID(t *testing.T) {
	t.Parallel()

	t.Run("logs query ID when writer is in context", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		ctx := context.WithValue(context.Background(), executor.KeyPrinter, &buf)

		LogQueryID(ctx, "BigQuery", "job-123")

		assert.Equal(t, "BigQuery query ID: job-123\n", buf.String())
	})

	t.Run("no-op when query ID is empty", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		ctx := context.WithValue(context.Background(), executor.KeyPrinter, &buf)

		LogQueryID(ctx, "BigQuery", "")

		assert.Empty(t, buf.String())
	})

	t.Run("no-op when no writer in context", func(t *testing.T) {
		t.Parallel()
		LogQueryID(context.Background(), "BigQuery", "job-123")
		// should not panic
	})
}

func TestLogOrSinkQueryID(t *testing.T) {
	t.Parallel()

	t.Run("logs when no sink in context", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		ctx := context.WithValue(context.Background(), executor.KeyPrinter, &buf)

		LogOrSinkQueryID(ctx, "BigQuery", "job-456")

		assert.Equal(t, "BigQuery query ID: job-456\n", buf.String())
	})

	t.Run("sinks when sink is in context", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		ctx := context.WithValue(context.Background(), executor.KeyPrinter, &buf)

		var sink string
		ctx = WithQueryIDSink(ctx, &sink)

		LogOrSinkQueryID(ctx, "BigQuery", "job-789")

		assert.Equal(t, "job-789", sink)
		assert.Empty(t, buf.String(), "should not log when sink is set")
	})

	t.Run("no-op when query ID is empty", func(t *testing.T) {
		t.Parallel()
		var sink string
		ctx := WithQueryIDSink(context.Background(), &sink)

		LogOrSinkQueryID(ctx, "BigQuery", "")

		assert.Empty(t, sink)
	})
}

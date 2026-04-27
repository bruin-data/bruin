package query

import (
	"context"
	"fmt"
	"io"

	"github.com/bruin-data/bruin/pkg/executor"
)

type queryIDSinkKey struct{}

// LogQueryID prints a database query ID to the console writer in the context.
// It is a no-op if the context has no writer or the queryID is empty.
func LogQueryID(ctx context.Context, dbType string, queryID string) {
	if queryID == "" {
		return
	}

	writer, ok := ctx.Value(executor.KeyPrinter).(io.Writer)
	if !ok || writer == nil {
		return
	}

	_, _ = fmt.Fprintf(writer, "%s query ID: %s\n", dbType, queryID)
}

// WithQueryIDSink returns a context that captures query IDs into the given
// string pointer instead of logging them. This allows callers (e.g. sensors)
// to control when and whether to log the ID.
func WithQueryIDSink(ctx context.Context, sink *string) context.Context {
	return context.WithValue(ctx, queryIDSinkKey{}, sink)
}

// LogOrSinkQueryID either writes the query ID to the sink in the context
// (if one was set via WithQueryIDSink) or logs it to the console.
func LogOrSinkQueryID(ctx context.Context, dbType string, queryID string) {
	if queryID == "" {
		return
	}

	if sink, ok := ctx.Value(queryIDSinkKey{}).(*string); ok && sink != nil {
		*sink = queryID
		return
	}

	LogQueryID(ctx, dbType, queryID)
}

package query

import (
	"context"
	"fmt"
	"io"

	"github.com/bruin-data/bruin/pkg/executor"
)

type queryIDSinkKey struct{}

type queryTypeKey struct{}

// Query type labels used by backends (e.g. BigQuery) to tag jobs with a
// recognizable prefix so users can identify the source of a query in the
// platform's job/query history. Values are intentionally short and stable —
// they end up in user-visible identifiers.
const (
	QueryTypeMain    = "main"
	QueryTypeColumn  = "column"
	QueryTypeCustom  = "custom"
	QueryTypeSensor  = "sensor"
	QueryTypeQuery   = "query"
	QueryTypeDiff    = "diff"
	QueryTypeImport  = "import"
	QueryTypePatch   = "patch"
	QueryTypeFetch   = "fetch"
	QueryTypeDryRun  = "dryrun"
	QueryTypePing    = "ping"
	QueryTypeSchema  = "schema"
	QueryTypeEnhance = "enhance"
)

// WithQueryType returns a context labeled with the kind of work driving the
// query. Backends may read this label to tag their jobs (e.g. BigQuery uses
// it as a job ID prefix). Empty types are ignored.
func WithQueryType(ctx context.Context, queryType string) context.Context {
	if queryType == "" {
		return ctx
	}
	return context.WithValue(ctx, queryTypeKey{}, queryType)
}

// QueryTypeFromContext returns the query type label set via WithQueryType,
// or the empty string if none was set.
func QueryTypeFromContext(ctx context.Context) string {
	s, _ := ctx.Value(queryTypeKey{}).(string)
	return s
}

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

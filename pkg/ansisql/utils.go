package ansisql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

const (
	DefaultQueryAnnotations = "default"
	QueryLogCharacterLimit  = 10000
)

// BuildAnnotationJSON builds the annotation JSON string by merging standard fields
// with any user-provided annotations from the context. Returns empty string if
// annotations are not enabled.
func BuildAnnotationJSON(ctx context.Context, fields map[string]interface{}) (string, error) {
	annotations, ok := ctx.Value(pipeline.RunConfigQueryAnnotations).(string)
	if !ok || annotations == "" {
		return "", nil
	}

	userAnnotations := make(map[string]interface{})
	if annotations != DefaultQueryAnnotations {
		if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
			return "", errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
		}
	}

	finalAnnotations := make(map[string]interface{}, len(fields)+len(userAnnotations))
	maps.Copy(finalAnnotations, fields)
	maps.Copy(finalAnnotations, userAnnotations)

	finalJSON, err := json.Marshal(finalAnnotations)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal final annotations")
	}

	return string(finalJSON), nil
}

func prependAnnotationComment(ctx context.Context, q *query.Query, fields map[string]interface{}) (*query.Query, error) {
	jsonStr, err := BuildAnnotationJSON(ctx, fields)
	if err != nil {
		return nil, err
	}
	if jsonStr == "" {
		return q, nil
	}

	return &query.Query{
		Query: fmt.Sprintf("-- @bruin.config: %s\n", jsonStr) + q.Query,
	}, nil
}

func AddAnnotationComment(ctx context.Context, q *query.Query, assetName, taskType, pipelineName string) (*query.Query, error) {
	return prependAnnotationComment(ctx, q, map[string]interface{}{
		"asset":    assetName,
		"type":     taskType,
		"pipeline": pipelineName,
	})
}

func AddColumnCheckAnnotationComment(ctx context.Context, q *query.Query, assetName, columnName, checkType, pipelineName string) (*query.Query, error) {
	return prependAnnotationComment(ctx, q, map[string]interface{}{
		"asset":             assetName,
		"asset_name":        assetName,
		"column_name":       columnName,
		"type":              "column_check",
		"column_check_type": checkType,
		"pipeline":          pipelineName,
	})
}

func AddCustomCheckAnnotationComment(ctx context.Context, q *query.Query, assetName, checkName, pipelineName string) (*query.Query, error) {
	return prependAnnotationComment(ctx, q, map[string]interface{}{
		"asset":             assetName,
		"asset_name":        assetName,
		"type":              "custom_check",
		"custom_check_name": checkName,
		"pipeline":          pipelineName,
	})
}

// AdhocQueryIDs carries the optional tracking identifiers attached to an
// adhoc query (e.g. via `bruin query`). Empty fields are omitted from
// annotations.
type AdhocQueryIDs struct {
	ThreadID      string
	AgentID       string
	MessagePairID string
}

// HasAny reports whether at least one tracking ID is set.
func (a AdhocQueryIDs) HasAny() bool {
	return a.ThreadID != "" || a.AgentID != "" || a.MessagePairID != ""
}

// AddAdhocQueryAnnotationComment prepends a tracking annotation comment to the
// query. The comment is prepended to the beginning of the query (works for
// BigQuery and others). For Snowflake, use BuildAdhocQueryTag with
// gosnowflake.WithQueryTag instead, since Snowflake strips leading SQL comments.
func AddAdhocQueryAnnotationComment(q *query.Query, ids AdhocQueryIDs) *query.Query {
	if !ids.HasAny() {
		return q
	}

	comment := "-- @bruin.config: " + BuildAdhocQueryTag(ids)

	return &query.Query{
		Query: comment + "\n" + q.Query,
	}
}

// BuildAdhocQueryTag builds the JSON query tag string for adhoc query
// tracking. This is used for Snowflake's QUERY_TAG via
// gosnowflake.WithQueryTag. Empty IDs are omitted from the JSON payload.
func BuildAdhocQueryTag(ids AdhocQueryIDs) string {
	annotations := map[string]interface{}{
		"type": "adhoc_query",
	}
	if ids.ThreadID != "" {
		annotations["thread_id"] = ids.ThreadID
	}
	if ids.AgentID != "" {
		annotations["agent_id"] = ids.AgentID
	}
	if ids.MessagePairID != "" {
		annotations["message_pair_id"] = ids.MessagePairID
	}

	finalJSON, err := json.Marshal(annotations)
	if err != nil {
		return ""
	}

	return string(finalJSON)
}

// LogQueryIfVerbose logs the SQL query to the writer if verbose mode is enabled.
// It checks for the verbose flag in the context and writes a formatted query preview
// to the printer writer, truncating queries longer than QueryLogCharacterLimit.
func LogQueryIfVerbose(ctx context.Context, writer interface{}, queryString string) {
	verbose := ctx.Value(executor.KeyVerbose)
	if verbose == nil || !verbose.(bool) {
		return
	}

	w, ok := writer.(io.Writer)
	if !ok {
		return
	}

	queryPreview := strings.TrimSpace(queryString)
	if len(queryPreview) > QueryLogCharacterLimit {
		queryPreview = queryPreview[:QueryLogCharacterLimit] + "\n... (truncated)"
	}
	fmt.Fprintf(w, "Executing SQL query:\n%s\n\n", queryPreview)
}

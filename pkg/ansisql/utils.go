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

// AddAgentIDAnnotationComment adds an agent ID annotation comment to the query.
// This is used for adhoc queries to track which agent executed them.
// The comment is prepended to the beginning of the query (works for BigQuery and others).
// For Snowflake, use BuildAgentIDQueryTag with gosnowflake.WithQueryTag instead.
func AddAgentIDAnnotationComment(q *query.Query, agentID string) *query.Query {
	if agentID == "" {
		return q
	}

	comment := "-- @bruin.config: " + BuildAgentIDQueryTag(agentID)

	return &query.Query{
		Query: comment + "\n" + q.Query,
	}
}

// BuildAgentIDQueryTag builds the JSON query tag string for agent ID annotation.
// This is used for Snowflake's QUERY_TAG via gosnowflake.WithQueryTag.
func BuildAgentIDQueryTag(agentID string) string {
	annotations := map[string]interface{}{
		"agent_id": agentID,
		"type":     "adhoc_query",
	}

	finalJSON, err := json.Marshal(annotations)
	if err != nil {
		// If marshaling fails, return just the agent ID
		return agentID
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

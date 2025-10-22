package ansisql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func AddAnnotationComment(ctx context.Context, q *query.Query, assetName, taskType, pipelineName string) (*query.Query, error) {
	annotations, ok := ctx.Value(pipeline.RunConfigQueryAnnotations).(string)
	if !ok || annotations == "" {
		return q, nil
	}
	userAnnotations := make(map[string]interface{})
	// If not "default", try to parse as JSON
	if annotations != DefaultQueryAnnotations {
		if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
			return nil, errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
		}
	}

	finalAnnotations := map[string]interface{}{
		"asset":    assetName,
		"type":     taskType,
		"pipeline": pipelineName,
	}

	for k, v := range userAnnotations {
		finalAnnotations[k] = v
	}

	finalJSON, err := json.Marshal(finalAnnotations)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal final annotations")
	}

	comment := fmt.Sprintf("-- @bruin.config: %s\n", string(finalJSON))

	// Return a new query with the annotation prepended
	return &query.Query{
		Query: comment + q.Query,
	}, nil
}

func AddColumnCheckAnnotationComment(ctx context.Context, q *query.Query, assetName, columnName, checkType, pipelineName string) (*query.Query, error) {
	annotations, ok := ctx.Value(pipeline.RunConfigQueryAnnotations).(string)
	if !ok || annotations == "" {
		return q, nil
	}
	userAnnotations := make(map[string]interface{})
	// If not "default", try to parse as JSON
	if annotations != DefaultQueryAnnotations {
		if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
			return nil, errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
		}
	}

	finalAnnotations := map[string]interface{}{
		"asset_name":        assetName,
		"column_name":       columnName,
		"type":              "column_check",
		"column_check_type": checkType,
		"pipeline":          pipelineName,
	}

	for k, v := range userAnnotations {
		finalAnnotations[k] = v
	}

	finalJSON, err := json.Marshal(finalAnnotations)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal final annotations")
	}

	comment := fmt.Sprintf("-- @bruin.config: %s\n", string(finalJSON))

	// Return a new query with the annotation prepended
	return &query.Query{
		Query: comment + q.Query,
	}, nil
}

func AddCustomCheckAnnotationComment(ctx context.Context, q *query.Query, assetName, checkName, pipelineName string) (*query.Query, error) {
	annotations, ok := ctx.Value(pipeline.RunConfigQueryAnnotations).(string)
	if !ok || annotations == "" {
		return q, nil
	}
	userAnnotations := make(map[string]interface{})
	// If not "default", try to parse as JSON
	if annotations != DefaultQueryAnnotations {
		if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
			return nil, errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
		}
	}

	finalAnnotations := map[string]interface{}{
		"asset_name":        assetName,
		"type":              "custom_check",
		"custom_check_name": checkName,
		"pipeline":          pipelineName,
	}

	for k, v := range userAnnotations {
		finalAnnotations[k] = v
	}

	finalJSON, err := json.Marshal(finalAnnotations)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal final annotations")
	}

	comment := fmt.Sprintf("-- @bruin.config: %s\n", string(finalJSON))

	// Return a new query with the annotation prepended
	return &query.Query{
		Query: comment + q.Query,
	}, nil
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

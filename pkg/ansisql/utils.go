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

// mergeAnnotations marshals the baseline fields merged with the user-provided
// annotations. User fields win on conflict. Returns "" when annotations is
// empty; treats the DefaultQueryAnnotations sentinel as "baseline only".
func mergeAnnotations(annotations string, baseline map[string]interface{}) (string, error) {
	if annotations == "" {
		return "", nil
	}

	merged := make(map[string]interface{}, len(baseline))
	maps.Copy(merged, baseline)

	if annotations != DefaultQueryAnnotations {
		userAnnotations := make(map[string]interface{})
		if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
			return "", errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
		}
		maps.Copy(merged, userAnnotations)
	}

	finalJSON, err := json.Marshal(merged)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal annotations")
	}
	return string(finalJSON), nil
}

// BuildAnnotationJSON builds the annotation JSON string by merging standard fields
// with any user-provided annotations from the context. Returns empty string if
// annotations are not enabled.
func BuildAnnotationJSON(ctx context.Context, fields map[string]interface{}) (string, error) {
	annotations, _ := ctx.Value(pipeline.RunConfigQueryAnnotations).(string)
	return mergeAnnotations(annotations, fields)
}

func prependAnnotationComment(ctx context.Context, q *query.Query, fields map[string]interface{}) (*query.Query, error) {
	jsonStr, err := BuildAnnotationJSON(ctx, fields)
	if err != nil {
		return nil, err
	}
	return AddAnnotationJSONComment(q, jsonStr)
}

// AddAnnotationJSONComment adds the annotation payload to the SQL and carries
// the same fields as structured metadata for backends with native query tags.
func AddAnnotationJSONComment(q *query.Query, annotationJSON string) (*query.Query, error) {
	if annotationJSON == "" {
		return q, nil
	}

	annotations := make(map[string]json.RawMessage)
	if err := json.Unmarshal([]byte(annotationJSON), &annotations); err != nil {
		return nil, errors.Wrapf(err, "invalid JSON in annotations: %s", annotationJSON)
	}

	annotationTags := make(map[string]string, len(annotations))
	for key, value := range annotations {
		if len(value) > 0 && value[0] == '"' {
			var stringValue string
			if err := json.Unmarshal(value, &stringValue); err != nil {
				return nil, errors.Wrapf(err, "failed to decode annotation %q", key)
			}
			annotationTags[key] = stringValue
			continue
		}
		annotationTags[key] = string(value)
	}

	annotatedQuery := *q
	annotatedQuery.Query = fmt.Sprintf("-- @bruin.config: %s\n", annotationJSON) + q.Query
	annotatedQuery.Annotations = annotationTags
	return &annotatedQuery, nil
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

func AddSensorAnnotationComment(ctx context.Context, q *query.Query, assetName, sensorType, pipelineName string) (*query.Query, error) {
	return prependAnnotationComment(ctx, q, map[string]interface{}{
		"asset":       assetName,
		"type":        "sensor",
		"sensor_type": sensorType,
		"pipeline":    pipelineName,
	})
}

// BuildAdhocQueryTag builds the JSON annotation payload for an adhoc query
// (e.g. `bruin query`). When annotations is empty, the baseline annotation
// is still emitted so every adhoc query is tagged.
func BuildAdhocQueryTag(annotations string) (string, error) {
	if annotations == "" {
		annotations = DefaultQueryAnnotations
	}
	return mergeAnnotations(annotations, map[string]interface{}{"type": "adhoc_query"})
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

package ansisql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

func AddAnnotationComment(ctx context.Context, q *query.Query, assetName, taskType, pipeline string) error {
	annotations, ok := ctx.Value("query-annotations").(string)
	if !ok || annotations == "" {
		return nil
	}

	var userAnnotations map[string]interface{}
	if err := json.Unmarshal([]byte(annotations), &userAnnotations); err != nil {
		return errors.Wrapf(err, "invalid JSON in annotations: %s", annotations)
	}

	finalAnnotations := map[string]interface{}{
		"asset":    assetName,
		"type":     taskType,
		"pipeline": pipeline,
	}

	for k, v := range userAnnotations {
		finalAnnotations[k] = v
	}

	finalJSON, err := json.Marshal(finalAnnotations)
	if err != nil {
		return errors.Wrap(err, "failed to marshal final annotations")
	}

	comment := fmt.Sprintf("-- @bruin.config: %s\n", string(finalJSON))
	q.Query = comment + q.Query

	return nil
}

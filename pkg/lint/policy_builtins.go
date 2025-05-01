package lint

import (
	"context"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func validatorsFromAssetValidator(av AssetValidator) validators {
	return validators{
		Pipeline: CallFuncForEveryAsset(av),
		Asset:    av,
	}
}

var builtinRules = map[string]validators{
	"asset-name-is-lowercase": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if strings.ToLower(asset.Name) == asset.Name {
				return nil, nil
			}

			return []*Issue{
				{
					Task:        asset,
					Description: "Asset name must be lowercase",
				},
			}, nil
		},
	),
	"asset-name-is-schema-dot-table": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if strings.Count(asset.Name, ".") == 1 {
				return nil, nil
			}

			return []*Issue{
				{
					Task:        asset,
					Description: "Asset name must be of the form {schema}.{table}",
				},
			}, nil
		},
	),
	"asset-has-description": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if strings.TrimSpace(asset.Description) != "" {
				return nil, nil
			}
			return []*Issue{
				{
					Task:        asset,
					Description: "Asset must have a description",
				},
			}, nil
		},
	),
	"asset-has-owner": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if strings.TrimSpace(asset.Owner) != "" {
				return nil, nil
			}
			return []*Issue{
				{
					Task:        asset,
					Description: "Asset must have an owner",
				},
			}, nil
		},
	),
	"asset-has-columns": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if len(asset.Columns) > 0 {
				return nil, nil
			}
			return []*Issue{
				{
					Task:        asset,
					Description: "Asset must have columns",
				},
			}, nil
		},
	),
}

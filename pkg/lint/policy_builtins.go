package lint

import (
	"context"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var builtinRules = map[string]AssetValidator{
	"asset_name_is_lowercase": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
	"asset_name_is_schema_dot_table": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
	"asset_has_description": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
	"asset_has_owner": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
	"asset_has_columns": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
}

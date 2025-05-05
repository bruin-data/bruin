package lint

import (
	"context"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func validatorsFromAssetValidator(av AssetValidator) validators {
	return validators{
		Pipeline: CallFuncForEveryAsset(av),
		Asset:    av,
	}
}

const (
	msgPrimaryKeyMustBeSet = "Asset must have atleast one primary key"
)

var (
	snakeCasePattern = regexp.MustCompile("^[a-z]+(_[a-z]+)*$")
	camelCasePattern = regexp.MustCompile("^[a-z]+(?:[A-Z][a-z0-9])*$")
)

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
	"asset-has-primary-key": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if len(asset.Columns) == 0 {
				return []*Issue{
					{
						Task:        asset,
						Description: msgPrimaryKeyMustBeSet,
					},
				}, nil
			}
			var primaryKeyFound bool
			for _, col := range asset.Columns {
				if col.PrimaryKey {
					primaryKeyFound = true
					break
				}
			}
			if !primaryKeyFound {
				return []*Issue{
					{
						Task:        asset,
						Description: msgPrimaryKeyMustBeSet,
					},
				}, nil
			}
			return nil, nil
		},
	),
	"asset-has-checks": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if len(asset.CustomChecks) == 0 {
				return []*Issue{
					{
						Task:        asset,
						Description: "Asset must have a custom check",
					},
				}, nil
			}
			return nil, nil
		},
	),
	"asset-has-tags": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if len(asset.Tags) == 0 {
				return []*Issue{
					{
						Task:        asset,
						Description: "Asset must have tags",
					},
				}, nil
			}
			return nil, nil
		},
	),
	"column-has-description": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			for _, col := range asset.Columns {
				if strings.TrimSpace(col.Description) != "" {
					continue
				}

				return []*Issue{
					{
						Task:        asset,
						Description: "Column must have a description",
					},
				}, nil
			}
			return nil, nil
		},
	),
	"column-has-type": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			for _, col := range asset.Columns {
				if strings.TrimSpace(col.Type) != "" {
					continue
				}

				return []*Issue{
					{
						Task:        asset,
						Description: "Column must have a type",
					},
				}, nil
			}
			return nil, nil
		},
	),
	"column-name-is-snake-case": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			for _, col := range asset.Columns {
				if snakeCasePattern.MatchString(col.Name) {
					continue
				}

				return []*Issue{
					{
						Task:        asset,
						Description: "Column names must be in snake_case",
					},
				}, nil
			}
			return nil, nil
		},
	),
	"column-name-is-camel-case": validatorsFromAssetValidator(
		func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			for _, col := range asset.Columns {
				if camelCasePattern.MatchString(col.Name) {
					continue
				}

				return []*Issue{
					{
						Task:        asset,
						Description: "Column names must be in camelCase",
					},
				}, nil
			}
			return nil, nil
		},
	),
}

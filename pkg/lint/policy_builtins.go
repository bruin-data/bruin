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

var validBigQueryTypes = map[string]struct{}{
	"string":     {},
	"bytes":      {},
	"int64":      {},
	"integer":    {}, // Alias for int64
	"smallint":   {}, // Alias for int64
	"tinyint":    {}, // Alias for int64
	"byteint":    {}, // Alias for int64
	"float64":    {},
	"numeric":    {},
	"decimal":    {}, // Alias for numeric
	"bignumeric": {},
	"bigdecimal": {}, // Alias for bignumeric
	"bool":       {},
	"boolean":    {}, // Alias for bool
	"timestamp":  {},
	"date":       {},
	"time":       {},
	"datetime":   {},
	"interval":   {},
	"geography":  {},
	"json":       {},
	"struct":     {}, // Type parameters are not validated here
	"array":      {}, // Type parameters are not validated here
	"range":      {}, // Type parameters are not validated here
}

var validSnowflakeTypes = map[string]struct{}{
	"number":           {},
	"decimal":          {}, // Alias for number
	"numeric":          {}, // Alias for number
	"int":              {}, // Alias for number
	"integer":          {}, // Alias for number
	"bigint":           {}, // Alias for number
	"smallint":         {}, // Alias for number
	"tinyint":          {}, // Alias for number
	"byteint":          {}, // Alias for number
	"float":            {},
	"float4":           {}, // Alias for float
	"float8":           {}, // Alias for float
	"double":           {}, // Alias for float
	"double precision": {}, // Alias for float
	"real":             {}, // Alias for float
	"varchar":          {},
	"char":             {}, // Alias for varchar
	"character":        {}, // Alias for varchar
	"string":           {}, // Alias for varchar
	"text":             {}, // Alias for varchar
	"binary":           {},
	"varbinary":        {}, // Alias for binary
	"boolean":          {},
	"date":             {},
	"datetime":         {}, // Alias for timestamp_ntz
	"time":             {},
	"timestamp":        {}, // Alias for timestamp_ntz by default
	"timestamp_ltz":    {},
	"timestamp_ntz":    {},
	"timestamp_tz":     {},
	"variant":          {},
	"object":           {},
	"array":            {},
	"geography":        {},
	"geometry":         {},
	"vector":           {},
}

var placeholderDescriptions = []string{
	"todo",
	"fixme",
	"tbd",
	"wip",
	"temp",
	"temporary",
	"placeholder",
	"add description",
	"description goes here",
	"to be added",
	"work in progress",
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
	"column-type-is-valid-for-platform": validatorsFromAssetValidator(
		func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			var validTypes map[string]struct{}
			var platformName string

			switch {
			case strings.HasPrefix(string(asset.Type), "bq."):
				validTypes = validBigQueryTypes
				platformName = "BigQuery"
			case strings.HasPrefix(string(asset.Type), "sf."):
				validTypes = validSnowflakeTypes
				platformName = "Snowflake"
			default:
				// Ignore assets of other types
				return nil, nil
			}

			if len(validTypes) == 0 {
				// Should not happen if sets are defined, but good practice
				return nil, nil
			}

			var issues []*Issue
			for _, col := range asset.Columns {
				if strings.TrimSpace(col.Type) == "" {
					// Let column-has-type rule handle this
					continue
				}

				// Normalize type: lowercase and remove parameters like (size) or (precision, scale)
				normalizedType := strings.ToLower(col.Type)
				if idx := strings.Index(normalizedType, "("); idx != -1 {
					normalizedType = normalizedType[:idx]
				}
				// Handle potential spaces in type names like 'double precision'
				normalizedType = strings.TrimSpace(normalizedType)

				if _, ok := validTypes[normalizedType]; !ok {
					issues = append(issues, &Issue{
						Task:        asset,
						Description: "Column '" + col.Name + "' has invalid type '" + col.Type + "' for platform '" + platformName + "'",
					})
				}
			}

			return issues, nil
		},
	),
	"description-must-not-be-placeholder": validatorsFromAssetValidator(
		func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			var issues []*Issue

			lowerAssetDesc := strings.ToLower(strings.TrimSpace(asset.Description))
			if lowerAssetDesc != "" {
				for _, placeholder := range placeholderDescriptions {
					if strings.Contains(lowerAssetDesc, placeholder) {
						issues = append(issues, &Issue{
							Task:        asset,
							Description: "Asset description appears to contain placeholder text: '" + placeholder + "'",
						})
						break
					}
				}
			}

			for _, col := range asset.Columns {
				lowerColDesc := strings.ToLower(strings.TrimSpace(col.Description))
				if lowerColDesc == "" {
					continue
				}

				for _, placeholder := range placeholderDescriptions {
					if strings.Contains(lowerColDesc, placeholder) {
						issues = append(issues, &Issue{
							Task:        asset,
							Description: "Column '" + col.Name + "' description appears to contain placeholder text: '" + placeholder + "'",
						})
						break
					}
				}
			}

			return issues, nil
		},
	),
	"asset-has-no-cross-pipeline-dependencies": validatorsFromAssetValidator(
		func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			for _, upstream := range asset.Upstreams {
				if upstream.Type != "uri" {
					continue
				}

				return []*Issue{
					{
						Task:        asset,
						Description: "Asset must not have a cross pipeline dependency",
					},
				}, nil
			}
			return nil, nil
		},
	),
}

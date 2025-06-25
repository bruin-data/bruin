package lint

import (
	"context"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

const (
	msgPrimaryKeyMustBeSet = "Asset must have atleast one primary key"
)

var (
	snakeCasePattern = regexp.MustCompile("^[a-z]+(_[a-z]+)*$")
	camelCasePattern = regexp.MustCompile("^[a-z]+(?:[A-Z][a-z0-9]+)*$")
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

func getBuiltinRules(sqlParser *sqlparser.SQLParser) map[string]validators {
	builtinRules := map[string]validators{
		"asset-name-is-lowercase": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-name-is-schema-dot-table": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-description": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-owner": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-columns": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-primary-key": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-checks": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
				if asset.CheckCount() == 0 {
					return []*Issue{
						{
							Task:        asset,
							Description: "Asset must have atleast one check",
						},
					}, nil
				}
				return nil, nil
			},
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-tags": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"column-has-description": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"column-has-type": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"column-name-is-snake-case": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"column-name-is-camel-case": {
			Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"column-type-is-valid-for-platform": {
			Asset: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
		},
		"description-must-not-be-placeholder": {
			Asset: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"asset-has-no-cross-pipeline-dependencies": {
			Asset: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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
			Severity: ValidatorSeverityCritical,
		},
		"pipeline-has-notifications": {
			Pipeline: func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
				notifs := pipeline.Notifications
				if len(notifs.Discord) > 0 || len(notifs.MSTeams) > 0 || len(notifs.Slack) > 0 {
					return nil, nil
				}
				return []*Issue{
					{
						Description: "Pipeline must have at least one notification set up",
					},
				}, nil
			},
			Severity: ValidatorSeverityCritical,
		},
		"pipeline-has-retries": {
			Pipeline: func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
				if pipeline.Retries > 0 {
					return nil, nil
				}

				return []*Issue{
					{
						Description: "Pipeline must have `retries` set to a value greater than zero",
					},
				}, nil
			},
			Severity: ValidatorSeverityCritical,
		},
		"pipeline-has-start-date": {
			Pipeline: func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
				if strings.TrimSpace(pipeline.StartDate) != "" {
					return nil, nil
				}
				return []*Issue{
					{
						Description: "Pipeline must have a start date",
					},
				}, nil
			},
			Severity: ValidatorSeverityCritical,
		},
		"pipeline-has-metadata-push": {
			Pipeline: func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
				if pipeline.MetadataPush.HasAnyEnabled() {
					return nil, nil
				}

				return []*Issue{
					{
						Description: "Pipeline must have metadata push enabled",
					},
				}, nil
			},
			Severity: ValidatorSeverityCritical,
		},
	}

	// Add the columns-match-query rule that requires sqlParser
	builtinRules["columns-match-query"] = validators{
		Asset: func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			issues := make([]*Issue, 0)

			if asset.Materialization.Type == pipeline.MaterializationTypeNone {
				return issues, nil
			}

			if !asset.IsSQLAsset() {
				return issues, nil
			}

			dialect, err := sqlparser.AssetTypeToDialect(asset.Type)
			if err != nil { //nolint:nilerr
				return issues, nil
			}

			// Create a renderer for this asset
			renderer := jinja.NewRendererWithYesterday("columns-match-query", "validation")
			renderedQuery, err := renderer.Render(asset.ExecutableFile.Content)
			if err != nil { //nolint:nilerr
				return issues, nil
			}

			if sqlParser == nil { //nolint:nilerr
				return issues, nil
			}

			err = sqlParser.Start()
			if err != nil { //nolint:nilerr
				return issues, nil
			}

			queryColumns, err := sqlParser.ExtractColumns(renderedQuery, dialect)
			if err != nil { //nolint:nilerr
				return issues, nil
			}

			if len(queryColumns) == 0 {
				return issues, nil
			}

			yamlColumns := make(map[string]bool)
			for _, col := range asset.Columns {
				yamlColumns[col.Name] = true
			}

			missingColumns := make([]string, 0)
			for _, queryCol := range queryColumns {
				if !yamlColumns[queryCol] {
					missingColumns = append(missingColumns, queryCol)
				}
			}

			if len(missingColumns) > 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Columns found in query but missing from columns metadata: " + strings.Join(missingColumns, ", "),
					Context:     missingColumns,
				})
			}

			return issues, nil
		},
		Severity: ValidatorSeverityWarning,
	}
	return builtinRules
}

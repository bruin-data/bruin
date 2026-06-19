package python

import (
	"context"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var ingestrValueParameterFlags = []string{
	"incremental_key",
	"incremental_strategy",
	"partition_by",
	"cluster_by",
	"schema_contract",
	"schema_naming",
	"page_size",
	"loader_file_size",
	"extract_parallelism",
	"sql_reflection_level",
	"sql_limit",
	"sql_exclude_columns",
	"mask",
	"pipelines_dir",
	"staging_bucket",
	"staging_dataset",
	"flush_interval",
	"flush_records",
	"sql_backend",
	"loader_file_format",
}

var ingestrBoolParameterFlags = []string{
	"no_inference",
	"trim_whitespace",
	"stream",
}

func ConsolidatedParameters(ctx context.Context, asset *pipeline.Asset, cmdArgs []string, columnOpts *ColumnHintOptions) ([]string, error) {
	cmdArgs = appendIngestrParameterFlags(asset.Parameters, cmdArgs)

	if value, exists := asset.Parameters["incremental_key"]; exists && value != "" {
		// Check if the incremental key column exists and is of date type
		for _, column := range asset.Columns {
			if column.Name == value && strings.ToLower(column.Type) == "date" {
				cmdArgs = append(cmdArgs, "--columns", column.Name+":"+column.Type)
			}
		}
	}

	// Handle primary keys
	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		for _, pk := range primaryKeys {
			cmdArgs = append(cmdArgs, "--primary-key", pk)
		}
	}

	if ctx.Value(pipeline.RunConfigStartDate) != nil {
		startTimeInstance, okParse := ctx.Value(pipeline.RunConfigStartDate).(time.Time)
		if okParse {
			fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)

			// If full-refresh and asset has a start_date, use that instead
			if fullRefresh && asset.StartDate != "" {
				parsedStartDate, err := time.Parse("2006-01-02", asset.StartDate)
				if err == nil {
					startTimeInstance = time.Date(parsedStartDate.Year(), parsedStartDate.Month(), parsedStartDate.Day(), 0, 0, 0, 0, time.UTC)
				}
			}

			applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
			startTime := startTimeInstance
			if ok && applyModifiers {
				startTime = pipeline.ModifyDate(startTimeInstance, asset.IntervalModifiers.Start)
			}
			cmdArgs = append(cmdArgs, "--interval-start", startTime.Format(time.RFC3339))
		}
	}

	if ctx.Value(pipeline.RunConfigEndDate) != nil {
		endTimeInstance, okParse := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
		if okParse {
			applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
			endTime := endTimeInstance
			if ok && applyModifiers {
				endTime = pipeline.ModifyDate(endTimeInstance, asset.IntervalModifiers.End)
			}
			cmdArgs = append(cmdArgs, "--interval-end", endTime.Format(time.RFC3339))
		}
	}

	fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
	if fullRefresh != nil && fullRefresh.(bool) {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}

	// Handle column hints based on enforce_schema parameter
	if columnOpts != nil && len(asset.Columns) > 0 {
		shouldEnforce := columnOpts.EnforceSchemaByDefault
		if value, exists := asset.Parameters["enforce_schema"]; exists {
			shouldEnforce = value == "true"
		}

		if shouldEnforce {
			columns := ColumnHints(asset.Columns, columnOpts.NormalizeColumnNames)
			if columns != "" {
				cmdArgs = append(cmdArgs, "--columns", columns)
			}
		}
	}

	return cmdArgs, nil
}

func appendIngestrParameterFlags(params map[string]string, cmdArgs []string) []string {
	for _, param := range ingestrValueParameterFlags {
		if value, exists := params[param]; exists && value != "" {
			cmdArgs = append(cmdArgs, "--"+strings.ReplaceAll(param, "_", "-"), value)
		}
	}

	for _, param := range ingestrBoolParameterFlags {
		if value, exists := params[param]; exists && value == "true" {
			cmdArgs = append(cmdArgs, "--"+strings.ReplaceAll(param, "_", "-"))
		}
	}

	return cmdArgs
}

func AddExtraPackages(destURI, sourceURI string, extraPackages []string) []string {
	if strings.HasPrefix(sourceURI, "mssql://") || strings.HasPrefix(destURI, "mssql://") {
		extraPackages = []string{"pyodbc==5.1.0"}
	}
	return extraPackages
}

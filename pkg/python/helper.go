package python

import (
	"context"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func ConsolidatedParameters(ctx context.Context, asset *pipeline.Asset, cmdArgs []string) ([]string, error) {
	if value, exists := asset.Parameters["incremental_key"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--incremental-key", value)

		// Check if the incremental key column exists and is of date type
		for _, column := range asset.Columns {
			if column.Name == value && strings.ToLower(column.Type) == "date" {
				cmdArgs = append(cmdArgs, "--columns", column.Name+":"+column.Type)
			}
		}
	}

	if value, exists := asset.Parameters["incremental_strategy"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--incremental-strategy", value)
	}

	if value, exists := asset.Parameters["loader_file_format"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--loader-file-format", value)
	}

	if value, exists := asset.Parameters["partition_by"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--partition-by", value)
	}

	if value, exists := asset.Parameters["cluster_by"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--cluster-by", value)
	}

	if value, exists := asset.Parameters["sql_backend"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--sql-backend", value)
	}

	if value, exists := asset.Parameters["loader_file_size"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--loader-file-size", value)
	}

	if value, exists := asset.Parameters["schema_naming"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--schema-naming", value)
	}

	if value, exists := asset.Parameters["extract_parallelism"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--extract-parallelism", value)
	}

	if value, exists := asset.Parameters["sql_reflection_level"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--sql-reflection-level", value)
	}

	if value, exists := asset.Parameters["sql_limit"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--sql-limit", value)
	}

	if value, exists := asset.Parameters["sql_exclude_columns"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--sql-exclude-columns", value)
	}

	if value, exists := asset.Parameters["staging_bucket"]; exists && value != "" {
		cmdArgs = append(cmdArgs, "--staging-bucket", value)
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
			applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
			startTime := startTimeInstance
			if ok && applyModifiers {
				startTime = pipeline.ModifyDate(startTimeInstance, asset.IntervalModifiers.Start)
			}

			// Asset-level start_date overrides global start date ONLY when full-refresh is enabled
			fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
			if asset.StartDate != "" && fullRefresh != nil && fullRefresh.(bool) {
				assetStartTime, err := time.Parse("2006-01-02", asset.StartDate)
				if err == nil {
					startTime = assetStartTime
				}
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
	return cmdArgs, nil
}

func AddExtraPackages(destURI, sourceURI string, extraPackages []string) []string {
	if strings.HasPrefix(sourceURI, "mssql://") || strings.HasPrefix(destURI, "mssql://") {
		extraPackages = []string{"pyodbc==5.1.0"}
	}
	return extraPackages
}

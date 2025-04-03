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
			startTime, err := pipeline.ModifyDate(startTimeInstance, asset.IntervalModifiers.Start)
			if err != nil {
				return nil, err
			}
			cmdArgs = append(cmdArgs, "--interval-start", startTime.Format(time.RFC3339))
		}
	}

	if ctx.Value(pipeline.RunConfigEndDate) != nil {
		endTimeInstance, okParse := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
		if okParse {
			endTime, err := pipeline.ModifyDate(endTimeInstance, asset.IntervalModifiers.End)
			if err != nil {
				return nil, err
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

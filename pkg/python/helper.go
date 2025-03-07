package python

import (
	"context"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func ConsolidatedParameters(ctx context.Context, asset *pipeline.Asset, cmdArgs []string) []string {
	parameters := map[string]string{
		"loader_file_format":   "--loader-file-format",
		"incremental_key":      "--incremental-key",
		"incremental_strategy": "--incremental-strategy",
		"partition_by":         "--partition-by",
		"cluster_by":           "--cluster-by",
		"sql_backend":          "--sql-backend",
		"loader_file_size":     "--loader-file-size",
		"schema_naming":        "--schema-naming",
		"extract_parallelism":  "--extract-parallelism",
		"sql_reflection_level": "--sql-reflection-level",
		"sql_limit":            "--sql-limit",
		"sql_exclude_columns":  "--sql-exclude-columns",
	}

	for param, flag := range parameters {
		if value, exists := asset.Parameters[param]; exists && value != "" {
			cmdArgs = append(cmdArgs, flag, value)
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
			cmdArgs = append(cmdArgs, "--interval-start", startTimeInstance.Format(time.RFC3339))
		}
	}

	if ctx.Value(pipeline.RunConfigEndDate) != nil {
		endTimeInstance, okParse := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
		if okParse {
			cmdArgs = append(cmdArgs, "--interval-end", endTimeInstance.Format(time.RFC3339))
		}
	}

	fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
	if fullRefresh != nil && fullRefresh.(bool) {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}
	return cmdArgs
}

func AddExtraPackages(destURI, sourceURI string, extraPackages []string) []string {
	if strings.HasPrefix(sourceURI, "mssql://") || strings.HasPrefix(destURI, "mssql://") {
		extraPackages = []string{"pyodbc==5.1.0"}
	}
	return extraPackages
}

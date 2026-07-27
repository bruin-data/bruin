package python

import (
	"context"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

const ingestrIntervalTimestampFormat = "2006-01-02T15:04:05.999999Z07:00"

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
	"extract_partition_by",
	"extract_partition_interval",
	"sql_reflection_level",
	"sql_limit",
	"sql_exclude_columns",
	"pipelines_dir",
	"staging_bucket",
	"staging_dataset",
	"sql_backend",
	"loader_file_format",
}

// ingestrStreamValueParameterFlags carry ingestr flags that are only valid
// together with --stream; ingestr errors if they are passed without it.
var ingestrStreamValueParameterFlags = []string{
	"flush_interval",
	"flush_records",
}

var ingestrBoolParameterFlags = []string{
	"no_inference",
	"trim_whitespace",
	"stream",
}

// isStreamingParams reports whether the asset runs in continuous streaming mode.
// The ingestr operator sets stream=true for cdc_mode: stream before this runs, so
// the generic stream parameter is a reliable signal here (and avoids an import
// cycle back into the ingestr package).
func isStreamingParams(params pipeline.ParameterMap) bool {
	value, _ := params.GetString("stream")
	return value == "true"
}

func ConsolidatedParameters(ctx context.Context, asset *pipeline.Asset, cmdArgs []string, columnOpts *ColumnHintOptions) ([]string, error) {
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	cmdArgs = appendIngestrParameterFlags(asset.Parameters, cmdArgs)
	cmdArgs = appendIngestrMaskFlags(asset.Parameters, asset.Columns, cmdArgs, columnOpts != nil && columnOpts.NormalizeColumnNames)

	if value, exists := asset.Parameters.GetString("incremental_key"); exists && value != "" {
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
			cmdArgs = append(cmdArgs, "--interval-start", startTime.Format(ingestrIntervalTimestampFormat))
		}
	}

	// In streaming mode --interval-end would truncate the live tail, so only bound
	// the initial snapshot with --interval-start and leave the stream open-ended.
	streaming := isStreamingParams(asset.Parameters)
	if !streaming && ctx.Value(pipeline.RunConfigEndDate) != nil {
		endTimeInstance, okParse := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
		if okParse {
			applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
			endTime := endTimeInstance
			if ok && applyModifiers {
				endTime = pipeline.ModifyDate(endTimeInstance, asset.IntervalModifiers.End)
			}
			cmdArgs = append(cmdArgs, "--interval-end", endTime.Format(ingestrIntervalTimestampFormat))
		}
	}

	// A full refresh on a stream would reset ingestr's own CDC state; the run
	// command rejects the combination up front, but guard here as well.
	if fullRefresh && !streaming {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}

	// Handle column hints based on enforce_schema parameter
	if columnOpts != nil && len(asset.Columns) > 0 {
		shouldEnforce := columnOpts.EnforceSchemaByDefault
		if value, exists := asset.Parameters.GetString("enforce_schema"); exists {
			shouldEnforce = value == "true"
		}

		if shouldEnforce {
			columns := ColumnHints(asset.Columns, columnOpts.NormalizeColumnNames, columnOpts.TypeHintOverlay, columnOpts.TypeWrappers)
			if columns != "" {
				cmdArgs = append(cmdArgs, "--columns", columns)
			}
		}
	}

	return cmdArgs, nil
}

func appendIngestrMaskFlags(params pipeline.ParameterMap, cols []pipeline.Column, cmdArgs []string, normalizeColumnNames bool) []string {
	seen := make(map[string]struct{})
	if mask, exists := params.GetString("mask"); exists {
		cmdArgs = appendIngestrMaskRule(cmdArgs, seen, strings.TrimSpace(mask))
	}

	for _, col := range cols {
		mask := strings.TrimSpace(col.Mask)
		if mask == "" {
			continue
		}

		rule := mask
		if !strings.Contains(mask, ":") {
			columnName := strings.TrimSpace(col.Name)
			if columnName == "" {
				continue
			}
			if normalizeColumnNames {
				columnName = NormalizeColumnName(columnName)
			}
			rule = columnName + ":" + mask
		}
		cmdArgs = appendIngestrMaskRule(cmdArgs, seen, rule)
	}

	return cmdArgs
}

func appendIngestrMaskRule(cmdArgs []string, seen map[string]struct{}, rule string) []string {
	if rule == "" {
		return cmdArgs
	}
	if _, ok := seen[rule]; ok {
		return cmdArgs
	}
	seen[rule] = struct{}{}
	return append(cmdArgs, "--mask", rule)
}

func appendIngestrParameterFlags(params pipeline.ParameterMap, cmdArgs []string) []string {
	for _, param := range ingestrValueParameterFlags {
		if value, exists := params.GetString(param); exists && value != "" {
			cmdArgs = append(cmdArgs, "--"+strings.ReplaceAll(param, "_", "-"), value)
		}
	}

	for _, param := range ingestrBoolParameterFlags {
		if value, exists := params.GetString(param); exists && value == "true" {
			cmdArgs = append(cmdArgs, "--"+strings.ReplaceAll(param, "_", "-"))
		}
	}

	// Flush flags are only valid alongside --stream; ingestr rejects them otherwise.
	if isStreamingParams(params) {
		for _, param := range ingestrStreamValueParameterFlags {
			if value, exists := params.GetString(param); exists && value != "" {
				cmdArgs = append(cmdArgs, "--"+strings.ReplaceAll(param, "_", "-"), value)
			}
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

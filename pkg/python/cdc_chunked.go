package python

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var cdcChunkedSnapshotParameterNames = []string{
	"cdc_snapshot_chunked",
	"cdc_snapshot_execution",
	"cdc_snapshot_chunk_size",
	"cdc_snapshot_chunk_timeout",
	"cdc_wal_staging",
	"cdc_wal_buffer_bytes",
}

func HasCDCChunkedSnapshotParameters(asset *pipeline.Asset) bool {
	if asset == nil {
		return false
	}
	for _, key := range cdcChunkedSnapshotParameterNames {
		if _, present := asset.Parameters[key]; present {
			return true
		}
	}
	return false
}

// ValidateCDCChunkedSnapshotParameters validates explicit asset options without
// adding defaults. Source and destination capabilities are checked at execution.
func ValidateCDCChunkedSnapshotParameters(asset *pipeline.Asset) error {
	if !HasCDCChunkedSnapshotParameters(asset) {
		return nil
	}
	params := asset.Parameters
	if cdc, _ := params.GetString("cdc"); cdc != "true" {
		return errors.New("chunked CDC snapshot parameters require cdc: true")
	}

	chunked, _ := params.GetString("cdc_snapshot_chunked")
	if _, present := params["cdc_snapshot_chunked"]; present && chunked != "true" && chunked != "false" {
		return errors.New("invalid cdc_snapshot_chunked: must be true or false")
	}
	enabled := chunked == "true"
	execution := "serial"
	if _, present := params["cdc_snapshot_execution"]; present {
		execution, _ = params.GetString("cdc_snapshot_execution")
		if execution != "serial" && execution != "parallel" {
			return errors.New("invalid cdc_snapshot_execution: must be \"serial\" or \"parallel\"")
		}
	}
	staging := "inmemory"
	if _, present := params["cdc_wal_staging"]; present {
		staging, _ = params.GetString("cdc_wal_staging")
		if staging != "inmemory" && staging != "destination" {
			return errors.New("invalid cdc_wal_staging: must be \"inmemory\" or \"destination\"")
		}
	}
	for _, key := range []string{"cdc_snapshot_chunk_size", "cdc_wal_buffer_bytes"} {
		if _, present := params[key]; !present {
			continue
		}
		value, _ := params.GetString(key)
		number, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid %s: must be an integer within the signed 64-bit range", key)
		}
		if enabled && number <= 0 {
			return fmt.Errorf("invalid %s: must be positive when cdc_snapshot_chunked is true", key)
		}
	}
	if _, present := params["cdc_snapshot_chunk_timeout"]; present {
		value, _ := params.GetString("cdc_snapshot_chunk_timeout")
		duration, err := time.ParseDuration(value)
		if err != nil {
			return errors.New("invalid cdc_snapshot_chunk_timeout: must be a Go duration such as \"30s\" or \"2m\"")
		}
		if enabled && duration <= 0 {
			return errors.New("invalid cdc_snapshot_chunk_timeout: must be positive when cdc_snapshot_chunked is true")
		}
	}
	if !enabled {
		return nil
	}
	if execution == "serial" && staging == "destination" {
		return errors.New("cdc_wal_staging: destination requires cdc_snapshot_execution: parallel; serial execution retains pending changes in source WAL")
	}
	if mode, _ := params.GetString("cdc_snapshot_mode"); mode == "never" {
		return errors.New("cdc_snapshot_chunked cannot be combined with cdc_snapshot_mode: never")
	}
	if _, present := params["cdc_start_lsn"]; present {
		return errors.New("cdc_snapshot_chunked cannot be combined with cdc_start_lsn")
	}
	if strategy, _ := params.GetString("incremental_strategy"); strategy != "" && strategy != "merge" {
		return errors.New("cdc_snapshot_chunked requires incremental_strategy: merge")
	}
	mat := asset.Materialization
	if mat.Strategy != pipeline.MaterializationStrategyNone && mat.Strategy != pipeline.MaterializationStrategyMerge {
		return errors.New("cdc_snapshot_chunked requires materialization.strategy: merge")
	}
	if mat.IncrementalKey != "" || strings.TrimSpace(mat.IncrementalPredicate) != "" {
		return errors.New("cdc_snapshot_chunked cannot be combined with materialization incremental_key or incremental_predicate")
	}
	for _, key := range []string{"sql_limit", "sql_exclude_columns", "incremental_key", "incremental_predicate", "interval_start", "interval_end", "extract_partition_by"} {
		raw, present := params[key]
		if !present || raw == nil {
			continue
		}
		value, scalar := params.GetString(key)
		if scalar && (value == "" || key == "incremental_predicate" && strings.TrimSpace(value) == "") {
			continue
		}
		if key == "sql_limit" {
			if number, err := strconv.ParseInt(value, 10, 64); err == nil && number == 0 {
				continue
			}
		}
		return fmt.Errorf("cdc_snapshot_chunked cannot be combined with %s", key)
	}
	return nil
}

// CI experiment marker 2

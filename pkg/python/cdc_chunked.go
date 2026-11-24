package python

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"golang.org/x/mod/semver"
)

var cdcChunkedSnapshotParameterNames = []string{
	"cdc_snapshot_chunked",
	"cdc_snapshot_spool",
	"cdc_snapshot_execution",
	"cdc_snapshot_chunk_size",
	"cdc_snapshot_chunk_timeout",
	"cdc_wal_staging",
	"cdc_wal_spool_dir",
	"cdc_wal_buffer_bytes",
}

// HasCDCChunkedSnapshotParameters reports explicit chunked or spooled snapshot
// options, including shared WAL storage settings.
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

// ValidateCDCSnapshotEngineVersion checks snapshot options against a resolved
// ingestr version without the leading v.
func ValidateCDCSnapshotEngineVersion(asset *pipeline.Asset, version string) error {
	if !HasCDCChunkedSnapshotParameters(asset) {
		return nil
	}
	_, hasSpool := asset.Parameters["cdc_snapshot_spool"]
	_, hasSpoolDir := asset.Parameters["cdc_wal_spool_dir"]
	staging, _ := asset.Parameters.GetString("cdc_wal_staging")
	if (hasSpool || hasSpoolDir || staging == "disk" || staging == "memory") && semver.Compare("v"+version, "v1.1.86") < 0 {
		return fmt.Errorf("CDC snapshot spooling and memory/disk WAL staging parameters require ingestr 1.1.86 or newer, but parameters.version resolved to %s", version)
	}
	if semver.Compare("v"+version, "v1.1.81") < 0 {
		return fmt.Errorf("chunked CDC snapshot parameters require ingestr 1.1.81 or newer, but parameters.version resolved to %s", version)
	}
	return nil
}

// IsCDCFullTableSnapshot reports whether CDC copies complete tables using
// chunking or spooling, without scheduler interval bounds.
func IsCDCFullTableSnapshot(asset *pipeline.Asset) bool {
	if asset == nil {
		return false
	}
	cdc, _ := asset.Parameters.GetString("cdc")
	chunked, _ := asset.Parameters.GetString("cdc_snapshot_chunked")
	spool, _ := asset.Parameters.GetString("cdc_snapshot_spool")
	return cdc == "true" && (chunked == "true" || spool == "true")
}

// ValidateCDCChunkedSnapshotParameters validates chunked and spooled snapshot
// options without adding defaults. Source and destination capabilities are
// checked at execution.
func ValidateCDCChunkedSnapshotParameters(asset *pipeline.Asset) error {
	if !HasCDCChunkedSnapshotParameters(asset) {
		return nil
	}
	params := asset.Parameters
	if cdc, _ := params.GetString("cdc"); cdc != "true" {
		return errors.New("CDC snapshot parameters require cdc: true")
	}

	for _, key := range []string{"cdc_snapshot_chunked", "cdc_snapshot_spool"} {
		value, _ := params.GetString(key)
		if _, present := params[key]; present && value != "true" && value != "false" {
			return fmt.Errorf("invalid %s: must be true or false", key)
		}
	}
	chunked, _ := params.GetString("cdc_snapshot_chunked")
	spool, _ := params.GetString("cdc_snapshot_spool")
	chunkedEnabled, spoolEnabled := chunked == "true", spool == "true"
	if chunkedEnabled && spoolEnabled {
		return errors.New("cdc_snapshot_spool cannot be combined with cdc_snapshot_chunked")
	}
	execution := "serial"
	if _, present := params["cdc_snapshot_execution"]; present {
		execution, _ = params.GetString("cdc_snapshot_execution")
		if execution != "serial" && execution != "parallel" {
			return errors.New("invalid cdc_snapshot_execution: must be \"serial\" or \"parallel\"")
		}
	}
	staging := ""
	if _, present := params["cdc_wal_staging"]; present {
		staging, _ = params.GetString("cdc_wal_staging")
		if staging != "inmemory" && staging != "memory" && staging != "destination" && staging != "disk" {
			return errors.New("invalid cdc_wal_staging: must be \"inmemory\", \"memory\", \"destination\", or \"disk\"")
		}
	}
	spoolDir := ""
	if raw, present := params["cdc_wal_spool_dir"]; present {
		var valid bool
		spoolDir, valid = raw.(string)
		if !valid || strings.TrimSpace(spoolDir) == "" {
			return errors.New("invalid cdc_wal_spool_dir: must be a nonempty directory path")
		}
	}
	if staging == "disk" && !spoolEnabled {
		return errors.New("cdc_wal_staging: disk requires cdc_snapshot_spool: true")
	}
	if spoolDir != "" && (!spoolEnabled || staging != "disk") {
		return errors.New("cdc_wal_spool_dir requires cdc_snapshot_spool: true and cdc_wal_staging: disk")
	}
	if staging == "disk" && spoolDir == "" {
		return errors.New("cdc_wal_spool_dir is required with cdc_wal_staging: disk")
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
		if number <= 0 {
			if chunkedEnabled {
				return fmt.Errorf("invalid %s: must be positive when cdc_snapshot_chunked is true", key)
			}
			if key == "cdc_wal_buffer_bytes" && spoolEnabled {
				return errors.New("invalid cdc_wal_buffer_bytes: must be positive when cdc_snapshot_spool is true")
			}
		}
	}
	if _, present := params["cdc_snapshot_chunk_timeout"]; present {
		value, _ := params.GetString("cdc_snapshot_chunk_timeout")
		duration, err := time.ParseDuration(value)
		if err != nil {
			return errors.New("invalid cdc_snapshot_chunk_timeout: must be a Go duration such as \"30s\" or \"2m\"")
		}
		if chunkedEnabled && duration <= 0 {
			return errors.New("invalid cdc_snapshot_chunk_timeout: must be positive when cdc_snapshot_chunked is true")
		}
	}
	if !chunkedEnabled && !spoolEnabled {
		return nil
	}
	if chunkedEnabled && execution == "serial" && staging == "destination" {
		return errors.New("cdc_wal_staging: destination requires cdc_snapshot_execution: parallel; serial execution retains pending changes in source WAL")
	}
	modeParameter := "cdc_snapshot_chunked"
	if spoolEnabled {
		modeParameter = "cdc_snapshot_spool"
	}
	return validateCDCFullTableSnapshot(asset, modeParameter)
}

func validateCDCFullTableSnapshot(asset *pipeline.Asset, modeParameter string) error {
	params := asset.Parameters
	if mode, _ := params.GetString("cdc_snapshot_mode"); mode == "never" {
		return fmt.Errorf("%s cannot be combined with cdc_snapshot_mode: never", modeParameter)
	}
	if _, present := params["cdc_start_lsn"]; present {
		return fmt.Errorf("%s cannot be combined with cdc_start_lsn", modeParameter)
	}
	if strategy, _ := params.GetString("incremental_strategy"); strategy != "" && strategy != "merge" {
		return fmt.Errorf("%s requires incremental_strategy: merge", modeParameter)
	}
	mat := asset.Materialization
	if mat.Strategy != pipeline.MaterializationStrategyNone && mat.Strategy != pipeline.MaterializationStrategyMerge {
		return fmt.Errorf("%s requires materialization.strategy: merge", modeParameter)
	}
	if mat.IncrementalKey != "" || strings.TrimSpace(mat.IncrementalPredicate) != "" {
		return fmt.Errorf("%s cannot be combined with materialization incremental_key or incremental_predicate", modeParameter)
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
		return fmt.Errorf("%s cannot be combined with %s", modeParameter, key)
	}
	return nil
}

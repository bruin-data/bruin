package python

import (
	"errors"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// ValidateCDCSnapshotParameters checks the snapshot options shared by batch and
// streaming CDC. Checks that require database access are left to ingestr.
func ValidateCDCSnapshotParameters(asset *pipeline.Asset, runFullRefresh bool) error {
	_, hasMode := asset.Parameters["cdc_snapshot_mode"]
	_, hasStartLSN := asset.Parameters["cdc_start_lsn"]
	if !hasMode && !hasStartLSN {
		return nil
	}

	if cdc, _ := asset.Parameters.GetString("cdc"); cdc != "true" {
		return errors.New("cdc_snapshot_mode and cdc_start_lsn require cdc: true")
	}

	mode, _ := asset.Parameters.GetString("cdc_snapshot_mode")
	if hasMode && mode != "auto" && mode != "never" {
		return errors.New("invalid cdc_snapshot_mode: must be \"auto\" or \"never\"")
	}
	if hasStartLSN {
		lsn, _ := asset.Parameters.GetString("cdc_start_lsn")
		if !validCDCStartLSN(lsn) {
			return errors.New("invalid cdc_start_lsn: must be a nonzero PostgreSQL LSN in X/Y hexadecimal format with at most 8 digits per part")
		}
		if mode != "never" {
			return errors.New("cdc_start_lsn requires cdc_snapshot_mode: never")
		}
	}
	if mode != "never" {
		return nil
	}

	if slot, _ := asset.Parameters.GetString("cdc_slot"); strings.TrimSpace(slot) == "" {
		return errors.New("cdc_snapshot_mode: never requires a nonempty cdc_slot")
	}
	tables, _ := asset.Parameters.GetString("source_table")
	for table := range strings.SplitSeq(tables, ",") {
		if strings.TrimSpace(table) == "" || strings.Contains(table, "*") {
			return errors.New("cdc_snapshot_mode: never requires explicit, nonempty source_table names without wildcards")
		}
	}
	if asset.FullRefreshEnabled(runFullRefresh) {
		return errors.New("cdc_snapshot_mode: never cannot be used with full refresh")
	}
	return nil
}

func validCDCStartLSN(lsn string) bool {
	high, low, found := strings.Cut(lsn, "/")
	if !found || len(high) == 0 || len(high) > 8 || len(low) == 0 || len(low) > 8 {
		return false
	}
	highValue, highErr := strconv.ParseUint(high, 16, 32)
	lowValue, lowErr := strconv.ParseUint(low, 16, 32)
	return highErr == nil && lowErr == nil && (highValue != 0 || lowValue != 0)
}

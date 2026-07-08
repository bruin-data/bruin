package oracle

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

// scd2TimestampColumns are the SCD2 auto-managed columns that must be
// timezone-aware (TIMESTAMP WITH TIME ZONE). Oracle disallows leading
// underscores in identifiers, so bruin prefixes them with "bruin_".
var scd2TimestampColumns = []string{"bruin_valid_from", "bruin_valid_until"}

// tzMigrationSuffix names the transient column used while migrating an SCD2
// timestamp column to a timezone-aware type.
const tzMigrationSuffix = "__bruin_tz"

// isSCD2 reports whether the asset uses one of the SCD2 materialization strategies.
func isSCD2(asset *pipeline.Asset) bool {
	switch asset.Materialization.Strategy {
	case pipeline.MaterializationStrategySCD2ByColumn, pipeline.MaterializationStrategySCD2ByTime:
		return true
	default:
		return false
	}
}

// MigrateSCD2Columns brings a pre-existing SCD2 table's bruin_valid_from/
// bruin_valid_until columns up to the timezone-aware standard. Older bruin
// versions created these columns as naive TIMESTAMP; the values were written
// under the UTC convention. Oracle cannot alter a column's type from TIMESTAMP
// to TIMESTAMP WITH TIME ZONE in place, so a new timezone-aware column is added,
// the existing values are copied into it with FROM_TZ (interpreting them as
// UTC), the old column is dropped and the new column is renamed back.
//
// It is a no-op when the table does not exist yet or the columns are already
// timezone-aware.
func (db *Client) MigrateSCD2Columns(ctx context.Context, asset *pipeline.Asset) error {
	owner, tableName := "", strings.ToUpper(asset.Name)
	if parts := strings.SplitN(asset.Name, ".", 2); len(parts) == 2 {
		owner, tableName = strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
	}

	ownerFilter := ""
	if owner != "" {
		ownerFilter = fmt.Sprintf(" AND owner = '%s'", owner)
	}
	schemaQuery := fmt.Sprintf(
		"SELECT column_name, data_type FROM all_tab_columns WHERE table_name = '%s'%s",
		tableName, ownerFilter,
	)
	result, err := db.Select(ctx, &query.Query{Query: schemaQuery})
	if err != nil {
		return fmt.Errorf("failed to inspect columns for SCD2 migration of '%s': %w", asset.Name, err)
	}
	if len(result) == 0 {
		return nil
	}

	columnTypes := make(map[string]string, len(result))
	for _, row := range result {
		if len(row) < 2 {
			continue
		}
		name, ok := row[0].(string)
		if !ok {
			continue
		}
		dataType, ok := row[1].(string)
		if !ok {
			continue
		}
		columnTypes[strings.ToLower(name)] = strings.ToLower(dataType)
	}

	for _, stmt := range buildSCD2MigrationStatements(asset.Name, columnTypes) {
		if err := db.RunQueryWithoutResult(ctx, &query.Query{Query: stmt}); err != nil {
			return err
		}
	}
	return nil
}

// buildSCD2MigrationStatements returns the ordered statements that convert any
// legacy naive SCD2 timestamp columns to TIMESTAMP WITH TIME ZONE, or nil when
// every SCD2 timestamp column is already timezone-aware (or absent). columnTypes
// maps lowercase column names to their lowercase all_tab_columns data_type.
func buildSCD2MigrationStatements(tableName string, columnTypes map[string]string) []string {
	stmts := make([]string, 0, len(scd2TimestampColumns)*4)
	for _, name := range scd2TimestampColumns {
		colType, ok := columnTypes[name]
		if !ok || isTimezoneAware(colType) {
			continue
		}
		tmp := name + tzMigrationSuffix
		stmts = append(
			stmts,
			fmt.Sprintf("ALTER TABLE %s ADD (%s TIMESTAMP(6) WITH TIME ZONE)", tableName, tmp),
			fmt.Sprintf("UPDATE %s SET %s = FROM_TZ(CAST(%s AS TIMESTAMP(6)), 'UTC')", tableName, tmp, name),
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableName, name),
			fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", tableName, tmp, name),
		)
	}
	return stmts
}

// isTimezoneAware reports whether an Oracle timestamp data_type already carries
// timezone information (TIMESTAMP WITH TIME ZONE).
func isTimezoneAware(columnType string) bool {
	return strings.Contains(strings.ToLower(columnType), "with time zone")
}

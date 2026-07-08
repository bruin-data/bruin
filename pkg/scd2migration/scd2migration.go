// Package scd2migration is a TEMPORARY, self-contained upgrade shim.
//
// ⚠️ TEMPORARY MIGRATION — SAFE TO DELETE AFTER IT EXPIRES ⚠️
//
//	Created:  2026-07-08
//	Expires:  2026-10-08  (~3 months later)
//	Purpose:  SCD2 tables created before _valid_from/_valid_until became
//	          timezone-aware still carry the legacy column types. This shim
//	          converts them on the incremental path, reading the stored values
//	          as UTC. It is a no-op once the columns are correct, so it is safe
//	          to run on every incremental.
//	Affects:  Postgres, Snowflake and MySQL (this package) plus Oracle
//	          (pkg/oracle). DuckDB and Athena are NOT affected — their
//	          incremental rebuilds the table and converts legacy values
//	          automatically.
//	Removal:  On or after the expiry date, once existing deployments have run at
//	          least once on the timezone-aware release, delete this package and
//	          the call sites in pkg/{postgres,snowflake,mysql}/operator.go.
package scd2migration

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
)

// Querier is satisfied by every SQL engine's DB client.
type Querier interface {
	Select(ctx context.Context, q *query.Query) ([][]interface{}, error)
	RunQueryWithoutResult(ctx context.Context, q *query.Query) error
}

const tmpSuffix = "__bruin_tz"

// columns are the SCD2 auto-managed timestamp columns to migrate.
var columns = []string{"_valid_from", "_valid_until"}

// Postgres converts naive _valid_from/_valid_until columns to TIMESTAMPTZ,
// reading the stored values as UTC.
func Postgres(ctx context.Context, db Querier, assetName string) error {
	schema, table := "public", assetName
	if p := strings.SplitN(assetName, ".", 2); len(p) == 2 {
		schema, table = p[0], p[1]
	}
	types, err := fetchTypes(ctx, db, fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'",
		schema, table,
	))
	if err != nil {
		return err
	}
	stmts := buildSwap(pgQuote(assetName), "TIMESTAMPTZ", types, alreadyTZ, pgQuote,
		func(col string) string { return col + " AT TIME ZONE 'UTC'" })
	return runEach(ctx, db, stmts)
}

// Snowflake converts TIMESTAMP_NTZ (and non-target TIMESTAMP_LTZ) columns to
// TIMESTAMP_TZ, reading the stored values as UTC.
func Snowflake(ctx context.Context, db Querier, assetName string) error {
	parts := strings.Split(assetName, ".")
	prefix, schema, table := "", "", strings.ToUpper(assetName)
	switch len(parts) {
	case 2:
		schema, table = strings.ToUpper(parts[0]), strings.ToUpper(parts[1])
	case 3:
		prefix, schema, table = parts[0]+".", strings.ToUpper(parts[1]), strings.ToUpper(parts[2])
	}
	types, err := fetchTypes(ctx, db, fmt.Sprintf(
		"SELECT column_name, data_type FROM %sinformation_schema.columns WHERE table_schema = '%s' AND table_name = '%s'",
		prefix, schema, table,
	))
	if err != nil {
		return err
	}
	stmts := buildSwap(assetName, "TIMESTAMP_TZ", types, alreadyTZ, ident,
		func(col string) string { return fmt.Sprintf("CAST(%s AS TIMESTAMP_TZ)", col) })
	if len(stmts) == 0 {
		return nil
	}
	batch := "ALTER SESSION SET TIMEZONE = 'UTC';\n" + strings.Join(stmts, ";\n") + ";"
	return db.RunQueryWithoutResult(ctx, &query.Query{Query: batch})
}

// MySQL converts the legacy VARCHAR _valid_until (from the old string sentinel)
// and any non-DATETIME _valid_from to DATETIME. MySQL SCD2 columns are naive
// (UTC by convention), so no timezone conversion is applied.
func MySQL(ctx context.Context, db Querier, assetName string) error {
	schemaFilter, table := "DATABASE()", assetName
	if p := strings.SplitN(assetName, ".", 2); len(p) == 2 {
		schemaFilter, table = "'"+p[0]+"'", p[1]
	}
	types, err := fetchTypes(ctx, db, fmt.Sprintf(
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = %s AND table_name = '%s'",
		schemaFilter, table,
	))
	if err != nil {
		return err
	}
	stmts := buildSwap(assetName, "DATETIME", types, isDateTime, ident,
		func(col string) string { return fmt.Sprintf("CAST(%s AS DATETIME)", col) })
	return runEach(ctx, db, stmts)
}

func buildSwap(table, targetType string, colTypes map[string]string, alreadyOK func(string) bool, quote func(string) string, convert func(col string) string) []string {
	var stmts []string
	for _, col := range columns {
		colType, ok := colTypes[col]
		if !ok || alreadyOK(colType) {
			continue
		}
		qCol, qTmp := quote(col), quote(col+tmpSuffix)
		if _, leftover := colTypes[col+tmpSuffix]; leftover {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, qTmp))
		}
		stmts = append(
			stmts,
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, qTmp, targetType), // new empty column
			fmt.Sprintf("UPDATE %s SET %s = %s", table, qTmp, convert(qCol)),        // copy values with conversion
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, qCol),               // drop old column
			fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, qTmp, qCol), // rename new column to old name
		)
	}
	return stmts
}

func fetchTypes(ctx context.Context, db Querier, sql string) (map[string]string, error) {
	rows, err := db.Select(ctx, &query.Query{Query: sql})
	if err != nil {
		return nil, fmt.Errorf("scd2 migration: failed to inspect columns: %w", err)
	}
	types := make(map[string]string, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		name, ok := asString(row[0])
		dataType, ok2 := asString(row[1])
		if !ok || !ok2 {
			continue
		}
		types[strings.ToLower(name)] = strings.ToLower(dataType)
	}
	return types, nil
}

func runEach(ctx context.Context, db Querier, stmts []string) error {
	for _, s := range stmts {
		if err := db.RunQueryWithoutResult(ctx, &query.Query{Query: s}); err != nil {
			return err
		}
	}
	return nil
}

func alreadyTZ(dataType string) bool {
	return strings.Contains(dataType, "with time zone") ||
		strings.Contains(dataType, "timestamptz") ||
		strings.Contains(dataType, "timestamp_tz")
}

func isDateTime(dataType string) bool { return dataType == "datetime" }

func ident(id string) string { return id }

// pgQuote double-quotes each dotted part of a Postgres identifier.
func pgQuote(id string) string {
	parts := strings.Split(id, ".")
	for i, p := range parts {
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

func asString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	default:
		return "", false
	}
}

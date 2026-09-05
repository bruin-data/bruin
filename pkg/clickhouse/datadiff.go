package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/bruin-data/bruin/pkg/diff"
)

// GetTableSummary reads the declared schema and, optionally, live table statistics.
func (c *Client) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	parts := strings.Split(tableName, ".")
	if len(parts) > 2 || parts[0] == "" || parts[len(parts)-1] == "" {
		return nil, fmt.Errorf("table name must be in format database.table or table, %q given", tableName)
	}
	database := c.config.GetDatabase()
	if database == "" {
		database = "default"
	}
	if len(parts) == 2 {
		database = parts[0]
	}
	table := parts[len(parts)-1]
	qualified := quoteSummaryIdentifier(database) + "." + quoteSummaryIdentifier(table)
	rows, err := c.connection.Query(ctx, `SELECT name, type, is_in_primary_key
FROM system.columns WHERE database = ? AND table = ? ORDER BY position`, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch schema for %q: %w", tableName, err)
	}
	defer rows.Close()
	result := &diff.TableSummaryResult{Table: &diff.Table{Name: tableName}}
	for rows.Next() {
		var name, dataType sql.NullString
		var primaryKey sql.NullInt64
		if err := rows.Scan(&name, &dataType, &primaryKey); err != nil {
			return nil, fmt.Errorf("failed to scan schema for %q: %w", tableName, err)
		}
		normalized, nullable := summaryColumnType(dataType.String)
		result.Table.Columns = append(result.Table.Columns, &diff.Column{
			Name: name.String, Type: dataType.String, NormalizedType: normalized,
			Nullable: nullable, PrimaryKey: primaryKey.Int64 == 1,
			// ClickHouse primary keys are sorting indexes, not uniqueness constraints.
			Unique: false,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read schema for %q: %w", tableName, err)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(result.Table.Columns) == 0 {
		return nil, fmt.Errorf("table %q does not exist or has no visible columns", tableName)
	}
	if schemaOnly {
		return result, nil
	}

	// Do not use system.tables.total_rows: it can include lightweight-deleted rows.
	var count sql.NullInt64
	if err := c.scanSummaryRow(ctx, "SELECT count() FROM "+qualified, &count); err != nil {
		return nil, fmt.Errorf("failed to count rows for %q: %w", tableName, err)
	}
	result.RowCount = count.Int64
	for _, column := range result.Table.Columns {
		column.Stats, err = c.fetchSummaryStats(ctx, qualified, column)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch statistics for %s.%s: %w", tableName, column.Name, err)
		}
	}
	return result, nil
}

func quoteSummaryIdentifier(name string) string {
	return "`" + strings.NewReplacer("\\", "\\\\", "`", "\\`").Replace(name) + "`"
}

func summaryColumnType(dataType string) (diff.CommonDataType, bool) {
	t := strings.ToLower(dataType)
	nullable := false
	for strings.HasPrefix(t, "nullable(") || strings.HasPrefix(t, "lowcardinality(") {
		nullable = nullable || strings.HasPrefix(t, "nullable(")
		t = t[strings.IndexByte(t, '(')+1 : len(t)-1]
	}
	if index := strings.IndexByte(t, '('); index >= 0 {
		t = t[:index]
	}
	switch t {
	case "int8", "int16", "int32", "int64", "int128", "int256", "uint8", "uint16", "uint32", "uint64", "uint128", "uint256", "float32", "float64", "bfloat16", "decimal", "decimal32", "decimal64", "decimal128", "decimal256":
		return diff.CommonTypeNumeric, nullable
	case "string", "fixedstring", "enum8", "enum16", "uuid", "ipv4", "ipv6":
		return diff.CommonTypeString, nullable
	case "bool", "boolean":
		return diff.CommonTypeBoolean, nullable
	case "date", "date32", "datetime", "datetime64":
		return diff.CommonTypeDateTime, nullable
	case "json":
		return diff.CommonTypeJSON, nullable
	default:
		return diff.CommonTypeUnknown, nullable
	}
}

func (c *Client) scanSummaryRow(ctx context.Context, query string, dest ...any) error {
	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := rows.Scan(dest...); err != nil {
		return err
	}
	for rows.Next() {
	}
	return rows.Err()
}

func (c *Client) fetchSummaryStats(ctx context.Context, table string, column *diff.Column) (diff.ColumnStatistics, error) {
	name := quoteSummaryIdentifier(column.Name)
	var count, nulls, distinct, first, second, empty sql.NullInt64
	var minVal, maxVal, avgVal, sumVal, stddev sql.NullFloat64
	var earliest, latest sql.NullTime
	var expressions string
	dest := []any{&count, &nulls}
	switch column.NormalizedType {
	case diff.CommonTypeNumeric:
		expressions = fmt.Sprintf("minOrNull(toFloat64(%[1]s)), maxOrNull(toFloat64(%[1]s)), avgOrNull(toFloat64(%[1]s)), sumOrNull(toFloat64(%[1]s)), stddevSampOrNull(toFloat64(%[1]s))", name)
		dest = append(dest, &minVal, &maxVal, &avgVal, &sumVal, &stddev)
	case diff.CommonTypeString:
		expressions = fmt.Sprintf("uniqExact(%[1]s), minOrNull(lengthUTF8(toString(%[1]s))), maxOrNull(lengthUTF8(toString(%[1]s))), avgOrNull(lengthUTF8(toString(%[1]s))), countIf(toString(%[1]s) = '')", name)
		dest = append(dest, &distinct, &first, &second, &avgVal, &empty)
	case diff.CommonTypeBoolean:
		expressions = fmt.Sprintf("countIf(%[1]s = true), countIf(%[1]s = false)", name)
		dest = append(dest, &first, &second)
	case diff.CommonTypeDateTime:
		expressions = fmt.Sprintf("minOrNull(%[1]s), maxOrNull(%[1]s), uniqExact(%[1]s)", name)
		dest = append(dest, &earliest, &latest, &distinct)
	case diff.CommonTypeJSON:
	case diff.CommonTypeBinary, diff.CommonTypeUnknown:
		return &diff.UnknownStatistics{}, nil
	}
	if expressions != "" {
		expressions = ", " + expressions
	}
	query := fmt.Sprintf("SELECT count(), countIf(isNull(%s))%s FROM %s", name, expressions, table)
	if err := c.scanSummaryRow(ctx, query, dest...); err != nil {
		return nil, err
	}
	switch column.NormalizedType {
	case diff.CommonTypeNumeric:
		return &diff.NumericalStatistics{
			Count: count.Int64 - nulls.Int64, NullCount: nulls.Int64,
			Min: summaryFloat(minVal), Max: summaryFloat(maxVal), Avg: summaryFloat(avgVal),
			Sum: summaryFloat(sumVal), StdDev: summaryFloat(stddev),
		}, nil
	case diff.CommonTypeString:
		return &diff.StringStatistics{Count: count.Int64, NullCount: nulls.Int64, DistinctCount: distinct.Int64, MinLength: int(first.Int64), MaxLength: int(second.Int64), AvgLength: avgVal.Float64, EmptyCount: empty.Int64}, nil
	case diff.CommonTypeBoolean:
		return &diff.BooleanStatistics{Count: count.Int64, NullCount: nulls.Int64, TrueCount: first.Int64, FalseCount: second.Int64}, nil
	case diff.CommonTypeDateTime:
		stats := &diff.DateTimeStatistics{Count: count.Int64, NullCount: nulls.Int64, UniqueCount: distinct.Int64}
		if earliest.Valid {
			stats.EarliestDate = &earliest.Time
		}
		if latest.Valid {
			stats.LatestDate = &latest.Time
		}
		return stats, nil
	case diff.CommonTypeJSON:
		return &diff.JSONStatistics{Count: count.Int64, NullCount: nulls.Int64}, nil
	default:
		return &diff.UnknownStatistics{}, nil
	}
}

func summaryFloat(value sql.NullFloat64) *float64 {
	// ClickHouse returns NaN for sample standard deviation with fewer than two values.
	if !value.Valid || math.IsNaN(value.Float64) || math.IsInf(value.Float64, 0) {
		return nil
	}
	return &value.Float64
}

package fabric

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tablename"
)

var _ diff.TableSummarizer = (*DB)(nil)

func (db *DB) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	table, err := db.parseDiffTableName(tableName)
	if err != nil {
		return nil, err
	}

	quotedTableName := quoteTableName(table)
	var rowCount int64
	if !schemaOnly {
		countResult, err := db.Select(ctx, &query.Query{Query: "SELECT COUNT_BIG(*) as row_count FROM " + quotedTableName})
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}
		if len(countResult) > 0 && len(countResult[0]) > 0 {
			rowCount, err = parseFabricInt64(countResult[0][0], "row count")
			if err != nil {
				return nil, fmt.Errorf("failed to parse row count for table '%s': %w", tableName, err)
			}
		}
	}

	infoSchema := "INFORMATION_SCHEMA"
	if table.Catalog != "" {
		infoSchema = QuoteIdentifier(table.Catalog) + ".INFORMATION_SCHEMA"
	}

	schemaQuery := fmt.Sprintf(`
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM %s.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`, infoSchema)

	rows, err := db.conn.QueryContext(ctx, schemaQuery, table.Schema, table.Table)
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}
	defer rows.Close()

	columns := make([]*diff.Column, 0)
	for rows.Next() {
		var columnName, dataType, isNullable string
		var columnDefault, charMaxLength, numericPrecision, numericScale interface{}

		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &charMaxLength, &numericPrecision, &numericScale); err != nil {
			return nil, fmt.Errorf("failed to scan schema info for table '%s': %w", tableName, err)
		}

		fullType := formatColumnType(dataType, charMaxLength, numericPrecision, numericScale)
		normalizedType := db.diffTypeMapper().MapType(dataType)
		nullable := strings.EqualFold(isNullable, "YES")

		var stats diff.ColumnStatistics
		if !schemaOnly {
			quotedColumnName := QuoteIdentifier(columnName)
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = db.fetchNumericalStats(ctx, quotedTableName, quotedColumnName)
			case diff.CommonTypeString:
				stats, err = db.fetchStringStats(ctx, quotedTableName, quotedColumnName)
			case diff.CommonTypeBoolean:
				stats, err = db.fetchBooleanStats(ctx, quotedTableName, quotedColumnName)
			case diff.CommonTypeDateTime:
				stats, err = db.fetchDateTimeStats(ctx, quotedTableName, quotedColumnName)
			case diff.CommonTypeBinary, diff.CommonTypeJSON, diff.CommonTypeUnknown:
				stats = &diff.UnknownStatistics{}
			}
			if err != nil {
				return nil, fmt.Errorf("failed to fetch %s stats for column '%s': %w", normalizedType, columnName, err)
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           fullType,
			NormalizedType: normalizedType,
			Nullable:       nullable,
			PrimaryKey:     false,
			Unique:         false,
			Stats:          stats,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error after iterating schema rows for table '%s': %w", tableName, err)
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table: &diff.Table{
			Name:    tableName,
			Columns: columns,
		},
	}, nil
}

func (db *DB) parseDiffTableName(tableName string) (tablename.TableName, error) {
	cb, ok := tablename.For("fabric")
	if !ok {
		return tablename.TableName{}, errors.New("fabric table-name capability not found")
	}

	databaseName := ""
	if db.config != nil {
		databaseName = db.config.Database
	}

	return cb.Parse(tableName, tablename.Defaults{Catalog: databaseName, Schema: "dbo"})
}

func (db *DB) diffTypeMapper() *diff.DatabaseTypeMapper {
	if db.typeMapper != nil {
		return db.typeMapper
	}
	return diff.NewSQLServerTypeMapper()
}

func quoteTableName(table tablename.TableName) string {
	return QuoteIdentifier(table.String("."))
}

func (db *DB) fetchNumericalStats(ctx context.Context, tableName, columnName string) (*diff.NumericalStatistics, error) {
	queryString := fmt.Sprintf(`
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG(%s) as null_count,
    MIN(CAST(%s AS FLOAT)) as min_val,
    MAX(CAST(%s AS FLOAT)) as max_val,
    AVG(CAST(%s AS FLOAT)) as avg_val,
    SUM(CAST(%s AS FLOAT)) as sum_val,
    STDEV(CAST(%s AS FLOAT)) as stddev_val
FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryString})
	if err != nil {
		return nil, err
	}

	stats := &diff.NumericalStatistics{}
	if len(result) == 0 || len(result[0]) < 7 {
		return stats, nil
	}

	row := result[0]
	if stats.Count, err = parseFabricInt64(row[0], "count"); err != nil {
		return nil, err
	}
	if stats.NullCount, err = parseFabricInt64(row[1], "null count"); err != nil {
		return nil, err
	}
	if stats.Min, err = parseFabricOptionalFloat64(row[2], "min"); err != nil {
		return nil, err
	}
	if stats.Max, err = parseFabricOptionalFloat64(row[3], "max"); err != nil {
		return nil, err
	}
	if stats.Avg, err = parseFabricOptionalFloat64(row[4], "avg"); err != nil {
		return nil, err
	}
	if stats.Sum, err = parseFabricOptionalFloat64(row[5], "sum"); err != nil {
		return nil, err
	}
	if stats.StdDev, err = parseFabricOptionalFloat64(row[6], "stddev"); err != nil {
		return nil, err
	}

	return stats, nil
}

func (db *DB) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	valueExpr := fmt.Sprintf("CONVERT(NVARCHAR(4000), %s)", columnName)
	queryString := fmt.Sprintf(`
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG(%s) as null_count,
    COUNT(DISTINCT %s) as distinct_count,
    COUNT_BIG(CASE WHEN %s = N'' THEN 1 END) as empty_count,
    MIN(LEN(%s)) as min_length,
    MAX(LEN(%s)) as max_length,
    AVG(CAST(LEN(%s) AS FLOAT)) as avg_length
FROM %s`,
		columnName, valueExpr, valueExpr, valueExpr, valueExpr, valueExpr, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryString})
	if err != nil {
		return nil, err
	}

	stats := &diff.StringStatistics{}
	if len(result) == 0 || len(result[0]) < 7 {
		return stats, nil
	}

	row := result[0]
	if stats.Count, err = parseFabricInt64(row[0], "count"); err != nil {
		return nil, err
	}
	if stats.NullCount, err = parseFabricInt64(row[1], "null count"); err != nil {
		return nil, err
	}
	if stats.DistinctCount, err = parseFabricInt64(row[2], "distinct count"); err != nil {
		return nil, err
	}
	if stats.EmptyCount, err = parseFabricInt64(row[3], "empty count"); err != nil {
		return nil, err
	}
	if row[4] != nil {
		minLength, err := parseFabricInt64(row[4], "min length")
		if err != nil {
			return nil, err
		}
		stats.MinLength = int(minLength)
	}
	if row[5] != nil {
		maxLength, err := parseFabricInt64(row[5], "max length")
		if err != nil {
			return nil, err
		}
		stats.MaxLength = int(maxLength)
	}
	avgLength, err := parseFabricOptionalFloat64(row[6], "avg length")
	if err != nil {
		return nil, err
	}
	if avgLength != nil {
		stats.AvgLength = *avgLength
	}

	return stats, nil
}

func (db *DB) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	queryString := fmt.Sprintf(`
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG(%s) as null_count,
    COUNT_BIG(CASE WHEN %s = 1 THEN 1 END) as true_count,
    COUNT_BIG(CASE WHEN %s = 0 THEN 1 END) as false_count
FROM %s`,
		columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryString})
	if err != nil {
		return nil, err
	}

	stats := &diff.BooleanStatistics{}
	if len(result) == 0 || len(result[0]) < 4 {
		return stats, nil
	}

	row := result[0]
	if stats.Count, err = parseFabricInt64(row[0], "count"); err != nil {
		return nil, err
	}
	if stats.NullCount, err = parseFabricInt64(row[1], "null count"); err != nil {
		return nil, err
	}
	if stats.TrueCount, err = parseFabricInt64(row[2], "true count"); err != nil {
		return nil, err
	}
	if stats.FalseCount, err = parseFabricInt64(row[3], "false count"); err != nil {
		return nil, err
	}

	return stats, nil
}

func (db *DB) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	queryString := fmt.Sprintf(`
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG(%s) as null_count,
    COUNT(DISTINCT %s) as unique_count,
    CONVERT(VARCHAR(33), MIN(%s), 126) as earliest_date,
    CONVERT(VARCHAR(33), MAX(%s), 126) as latest_date
FROM %s`,
		columnName, columnName, columnName, columnName, tableName)

	result, err := db.Select(ctx, &query.Query{Query: queryString})
	if err != nil {
		return nil, err
	}

	stats := &diff.DateTimeStatistics{}
	if len(result) == 0 || len(result[0]) < 5 {
		return stats, nil
	}

	row := result[0]
	if stats.Count, err = parseFabricInt64(row[0], "count"); err != nil {
		return nil, err
	}
	if stats.NullCount, err = parseFabricInt64(row[1], "null count"); err != nil {
		return nil, err
	}
	if stats.UniqueCount, err = parseFabricInt64(row[2], "unique count"); err != nil {
		return nil, err
	}
	if stats.EarliestDate, err = parseFabricOptionalTime(row[3], "earliest date"); err != nil {
		return nil, err
	}
	if stats.LatestDate, err = parseFabricOptionalTime(row[4], "latest date"); err != nil {
		return nil, err
	}

	return stats, nil
}

func parseFabricInt64(value interface{}, fieldName string) (int64, error) {
	if n, ok := int64Value(value); ok {
		return n, nil
	}

	switch v := value.(type) {
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case nil:
		return 0, fmt.Errorf("%s is NULL", fieldName)
	default:
		return 0, fmt.Errorf("unexpected %s type: got %T with value %v", fieldName, value, value)
	}
}

func parseFabricOptionalFloat64(value interface{}, fieldName string) (*float64, error) {
	if value == nil {
		return nil, nil
	}

	var parsed float64
	switch v := value.(type) {
	case float32:
		parsed = float64(v)
	case float64:
		parsed = v
	case int:
		parsed = float64(v)
	case int8:
		parsed = float64(v)
	case int16:
		parsed = float64(v)
	case int32:
		parsed = float64(v)
	case int64:
		parsed = float64(v)
	case uint:
		parsed = float64(v)
	case uint8:
		parsed = float64(v)
	case uint16:
		parsed = float64(v)
	case uint32:
		parsed = float64(v)
	case uint64:
		parsed = float64(v)
	case []byte:
		n, err := strconv.ParseFloat(string(v), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s %q: %w", fieldName, string(v), err)
		}
		parsed = n
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s %q: %w", fieldName, v, err)
		}
		parsed = n
	default:
		return nil, fmt.Errorf("unexpected %s type: got %T with value %v", fieldName, value, value)
	}

	return &parsed, nil
}

func parseFabricOptionalTime(value interface{}, fieldName string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}

	parsed, err := diff.ParseDateTime(value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", fieldName, err)
	}

	return parsed, nil
}

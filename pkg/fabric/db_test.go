package fabric

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const expectedCurrentDatabaseColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedWarehouseColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM [warehouse].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedArchiveColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM [archive].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedWarehouseDiffColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM [warehouse].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedNumericalStatsQuery = `
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG([id]) as null_count,
    MIN(CAST([id] AS FLOAT)) as min_val,
    MAX(CAST([id] AS FLOAT)) as max_val,
    AVG(CAST([id] AS FLOAT)) as avg_val,
    SUM(CAST([id] AS FLOAT)) as sum_val,
    STDEV(CAST([id] AS FLOAT)) as stddev_val
FROM [warehouse].[dbo].[orders]`

const expectedStringStatsQuery = `
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG([name]) as null_count,
    COUNT_BIG(DISTINCT CONVERT(NVARCHAR(4000), [name])) as distinct_count,
    COUNT_BIG(CASE WHEN CONVERT(NVARCHAR(4000), [name]) = N'' THEN 1 END) as empty_count,
    MIN(LEN(CONVERT(NVARCHAR(4000), [name]))) as min_length,
    MAX(LEN(CONVERT(NVARCHAR(4000), [name]))) as max_length,
    AVG(CAST(LEN(CONVERT(NVARCHAR(4000), [name])) AS FLOAT)) as avg_length
FROM [warehouse].[dbo].[orders]`

const expectedBooleanStatsQuery = `
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG([is_active]) as null_count,
    COUNT_BIG(CASE WHEN [is_active] = 1 THEN 1 END) as true_count,
    COUNT_BIG(CASE WHEN [is_active] = 0 THEN 1 END) as false_count
FROM [warehouse].[dbo].[orders]`

const expectedDateTimeStatsQuery = `
SELECT
    COUNT_BIG(*) as count,
    COUNT_BIG(*) - COUNT_BIG([created_at]) as null_count,
    COUNT_BIG(DISTINCT [created_at]) as unique_count,
    CONVERT(VARCHAR(33), MIN([created_at]), 126) as earliest_date,
    CONVERT(VARCHAR(33), MAX([created_at]), 126) as latest_date
FROM [warehouse].[dbo].[orders]`

func TestDB_GetColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		databaseName   string
		tableName      string
		mockConnection func(mock sqlmock.Sqlmock)
		want           []*ansisql.DBColumn
		wantErr        string
	}{
		{
			name:         "fetches columns for schema table",
			databaseName: "warehouse",
			tableName:    "sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).
						AddRow("id", "int", "NO", nil, int64(10), int64(0)).
						AddRow("name", "nvarchar", "YES", int64(255), nil, nil).
						AddRow("amount", "decimal", "YES", nil, int64(18), int64(2)).
						AddRow("payload", "varbinary", "YES", int64(-1), nil, nil))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "int", Nullable: false, PrimaryKey: false, Unique: false},
				{Name: "name", Type: "nvarchar(255)", Nullable: true, PrimaryKey: false, Unique: false},
				{Name: "amount", Type: "decimal(18,2)", Nullable: true, PrimaryKey: false, Unique: false},
				{Name: "payload", Type: "varbinary(max)", Nullable: true, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "bare table defaults to dbo schema",
			databaseName: "warehouse",
			tableName:    "orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("dbo", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).AddRow("id", "bigint", "NO", nil, int64(19), int64(0)))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "bigint", Nullable: false, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "three-part table name scopes lookup to table database",
			databaseName: "warehouse",
			tableName:    "archive.sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedArchiveColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).AddRow("id", "bigint", "NO", nil, int64(19), int64(0)))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "bigint", Nullable: false, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "query error",
			databaseName: "warehouse",
			tableName:    "sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnError(errors.New("metadata failed"))
			},
			wantErr: "failed to query columns for table 'warehouse.sales.orders'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			if tt.mockConnection != nil {
				tt.mockConnection(mock)
			}

			db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
			got, err := db.GetColumns(t.Context(), tt.databaseName, tt.tableName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_GetColumnsForTable(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedCurrentDatabaseColumnsQuery).
		WithArgs("finance", "payments").
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"CHARACTER_MAXIMUM_LENGTH",
			"NUMERIC_PRECISION",
			"NUMERIC_SCALE",
		}).AddRow("payment_id", "uniqueidentifier", "NO", nil, nil, nil))

	db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	got, err := db.GetColumnsForTable(t.Context(), "finance", "payments")
	require.NoError(t, err)
	assert.Equal(t, []*ansisql.DBColumn{
		{Name: "payment_id", Type: "uniqueidentifier", Nullable: false, PrimaryKey: false, Unique: false},
	}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetTableSummarySchemaOnly(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedWarehouseDiffColumnsQuery).
		WithArgs("dbo", "DimDateFirstLead").
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"COLUMN_DEFAULT",
			"CHARACTER_MAXIMUM_LENGTH",
			"NUMERIC_PRECISION",
			"NUMERIC_SCALE",
		}).
			AddRow("id", "bigint", "NO", nil, nil, int64(19), int64(0)).
			AddRow("name", "nvarchar", "YES", nil, int64(255), nil, nil).
			AddRow("is_active", "bit", "NO", nil, nil, nil, nil).
			AddRow("created_at", "datetime2", "YES", nil, nil, nil, nil).
			AddRow("payload", "varbinary", "YES", nil, int64(-1), nil, nil))

	db := &DB{
		conn:       sqlx.NewDb(mockDB, "sqlmock"),
		config:     &Config{Database: "warehouse"},
		typeMapper: diff.NewSQLServerTypeMapper(),
	}

	got, err := db.GetTableSummary(t.Context(), "dbo.DimDateFirstLead", true)
	require.NoError(t, err)
	assert.Equal(t, int64(0), got.RowCount)
	require.NotNil(t, got.Table)
	assert.Equal(t, "dbo.DimDateFirstLead", got.Table.Name)
	assert.Equal(t, []*diff.Column{
		{Name: "id", Type: "bigint", NormalizedType: diff.CommonTypeNumeric, Nullable: false, PrimaryKey: false, Unique: false},
		{Name: "name", Type: "nvarchar(255)", NormalizedType: diff.CommonTypeString, Nullable: true, PrimaryKey: false, Unique: false},
		{Name: "is_active", Type: "bit", NormalizedType: diff.CommonTypeBoolean, Nullable: false, PrimaryKey: false, Unique: false},
		{Name: "created_at", Type: "datetime2", NormalizedType: diff.CommonTypeDateTime, Nullable: true, PrimaryKey: false, Unique: false},
		{Name: "payload", Type: "varbinary(max)", NormalizedType: diff.CommonTypeBinary, Nullable: true, PrimaryKey: false, Unique: false},
	}, got.Table.Columns)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetTableSummaryFullNumericalStats(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery("SELECT COUNT_BIG(*) as row_count FROM [warehouse].[dbo].[orders]").
		WillReturnRows(sqlmock.NewRows([]string{"row_count"}).AddRow(int64(3)))
	mock.ExpectQuery(expectedWarehouseDiffColumnsQuery).
		WithArgs("dbo", "orders").
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"COLUMN_DEFAULT",
			"CHARACTER_MAXIMUM_LENGTH",
			"NUMERIC_PRECISION",
			"NUMERIC_SCALE",
		}).AddRow("id", "int", "NO", nil, nil, int64(10), int64(0)))
	mock.ExpectQuery(expectedNumericalStatsQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"count",
			"null_count",
			"min_val",
			"max_val",
			"avg_val",
			"sum_val",
			"stddev_val",
		}).AddRow(int64(3), int64(0), float64(1), float64(3), float64(2), float64(6), float64(1)))

	db := &DB{
		conn:       sqlx.NewDb(mockDB, "sqlmock"),
		config:     &Config{Database: "warehouse"},
		typeMapper: diff.NewSQLServerTypeMapper(),
	}

	got, err := db.GetTableSummary(t.Context(), "orders", false)
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.RowCount)
	require.Len(t, got.Table.Columns, 1)
	stats, ok := got.Table.Columns[0].Stats.(*diff.NumericalStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(3), stats.Count)
	assert.Equal(t, int64(0), stats.NullCount)
	require.NotNil(t, stats.Min)
	require.NotNil(t, stats.Max)
	require.NotNil(t, stats.Avg)
	require.NotNil(t, stats.Sum)
	require.NotNil(t, stats.StdDev)
	assert.InDelta(t, float64(1), *stats.Min, 0)
	assert.InDelta(t, float64(3), *stats.Max, 0)
	assert.InDelta(t, float64(2), *stats.Avg, 0)
	assert.InDelta(t, float64(6), *stats.Sum, 0)
	assert.InDelta(t, float64(1), *stats.StdDev, 0)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_FetchStringStats(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedStringStatsQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"count",
			"null_count",
			"distinct_count",
			"empty_count",
			"min_length",
			"max_length",
			"avg_length",
		}).AddRow(int64(5), int64(1), int64(3), int64(1), int64(0), int64(12), float64(4.5)))

	db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	got, err := db.fetchStringStats(t.Context(), "[warehouse].[dbo].[orders]", "[name]")
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Count)
	assert.Equal(t, int64(1), got.NullCount)
	assert.Equal(t, int64(3), got.DistinctCount)
	assert.Equal(t, int64(1), got.EmptyCount)
	assert.Equal(t, 0, got.MinLength)
	assert.Equal(t, 12, got.MaxLength)
	assert.InDelta(t, float64(4.5), got.AvgLength, 0)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_FetchBooleanStats(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedBooleanStatsQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"count",
			"null_count",
			"true_count",
			"false_count",
		}).AddRow(int64(5), int64(1), int64(3), int64(1)))

	db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	got, err := db.fetchBooleanStats(t.Context(), "[warehouse].[dbo].[orders]", "[is_active]")
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Count)
	assert.Equal(t, int64(1), got.NullCount)
	assert.Equal(t, int64(3), got.TrueCount)
	assert.Equal(t, int64(1), got.FalseCount)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_FetchDateTimeStats(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedDateTimeStatsQuery).
		WillReturnRows(sqlmock.NewRows([]string{
			"count",
			"null_count",
			"unique_count",
			"earliest_date",
			"latest_date",
		}).AddRow(int64(5), int64(1), int64(4), "2024-01-01T10:30:00.1234567", "2024-01-02T11:30:00.1234567"))

	db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	got, err := db.fetchDateTimeStats(t.Context(), "[warehouse].[dbo].[orders]", "[created_at]")
	require.NoError(t, err)
	assert.Equal(t, int64(5), got.Count)
	assert.Equal(t, int64(1), got.NullCount)
	assert.Equal(t, int64(4), got.UniqueCount)
	require.NotNil(t, got.EarliestDate)
	require.NotNil(t, got.LatestDate)
	assert.Equal(t, time.Date(2024, 1, 1, 10, 30, 0, 123456700, time.UTC), *got.EarliestDate)
	assert.Equal(t, time.Date(2024, 1, 2, 11, 30, 0, 123456700, time.UTC), *got.LatestDate)
	require.NoError(t, mock.ExpectationsWereMet())
}

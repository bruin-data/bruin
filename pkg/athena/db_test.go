package athena

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          query.Query
		want           [][]interface{}
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple select query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1, 2, 3`).
					WillReturnRows(sqlmock.NewRows([]string{"one", "two", "three"}).AddRow(1, 2, 3))
			},
			query: query.Query{
				Query: "SELECT 1, 2, 3",
			},
			want: [][]interface{}{{int64(1), int64(2), int64(3)}},
		},
		{
			name: "multi-row select query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some query`).
					WillReturnRows(
						sqlmock.NewRows([]string{"one", "two", "three"}).
							AddRow(1, 2, 3).
							AddRow(4, 5, 6),
					)
			},
			query: query.Query{
				Query: "some query",
			},
			want: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(4), int64(5), int64(6)},
			},
		},
		{
			name: "invalid query is properly handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(errors.New("some actual error"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "some actual error",
		},
		{
			name: "generic errors are just propagated",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(errors.New("something went wrong"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "something went wrong",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			got, err := db.Select(t.Context(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "valid connection test",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			},
			wantErr: false,
		},
		{
			name: "failed connection test",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnError(errors.New("connection error"))
			},
			wantErr:      true,
			errorMessage: "failed to run test query on Athena connection: connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			err = db.Ping(t.Context())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_SelectWithSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          query.Query
		want           *query.QueryResult
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple select with schema query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1, 2, 3").
					WillReturnRows(
						sqlmock.NewRowsWithColumnDefinition(
							sqlmock.NewColumn("one").OfType("BIGINT", int64(1)),
							sqlmock.NewColumn("two").OfType("BIGINT", int64(2)),
							sqlmock.NewColumn("three").OfType("BIGINT", int64(3)),
						).AddRow(1, 2, 3),
					)
			},
			query: query.Query{
				Query: "SELECT 1, 2, 3",
			},
			want: &query.QueryResult{
				Columns:     []string{"one", "two", "three"},
				Rows:        [][]interface{}{{int64(1), int64(2), int64(3)}},
				ColumnTypes: []string{"BIGINT", "BIGINT", "BIGINT"},
			},
			wantErr: false,
		},
		{
			name: "multi-row select with schema query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1, 2, 3").
					WillReturnRows(
						sqlmock.NewRowsWithColumnDefinition(
							sqlmock.NewColumn("one").OfType("BIGINT", int64(1)),
							sqlmock.NewColumn("two").OfType("BIGINT", int64(2)),
							sqlmock.NewColumn("three").OfType("BIGINT", int64(3)),
						).AddRow(1, 2, 3).AddRow(4, 5, 6),
					)
			},
			query: query.Query{
				Query: "SELECT 1, 2, 3",
			},
			want: &query.QueryResult{
				Columns: []string{"one", "two", "three"},
				Rows: [][]interface{}{
					{int64(1), int64(2), int64(3)},
					{int64(4), int64(5), int64(6)},
				},
				ColumnTypes: []string{"BIGINT", "BIGINT", "BIGINT"},
			},
			wantErr: false,
		},
		{
			name: "error in query is properly handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT 1, 2, 3").
					WillReturnError(errors.New("query execution failed"))
			},
			query: query.Query{
				Query: "SELECT 1, 2, 3",
			},
			wantErr:      true,
			errorMessage: "failed to execute query: query execution failed", // Updated expected error message
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a mock database and SQLx wrapper
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			// Configure the mock connection based on test case
			tt.mockConnection(mock)

			// Instantiate the DB client with the mock connection
			db := DB{conn: sqlxDB}

			// Execute the SelectWithSchema method
			got, err := db.SelectWithSchema(t.Context(), &tt.query)

			// Verify results
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}

			// Ensure all expectations were met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_GetTableSummarySchemaOnly(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	mock.ExpectQuery(buildAthenaSchemaQuery("analytics", "orders")).
		WillReturnRows(
			sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
				AddRow("id", "bigint", "NO").
				AddRow("status", "varchar", "YES").
				AddRow("metadata", "json", "YES"),
		)

	db := DB{
		conn:       sqlxDB,
		config:     &Config{Database: "analytics"},
		typeMapper: diff.NewAthenaTypeMapper(),
	}

	got, err := db.GetTableSummary(t.Context(), "orders", true)
	require.NoError(t, err)

	require.Equal(t, int64(0), got.RowCount)
	require.Equal(t, "orders", got.Table.Name)
	require.Len(t, got.Table.Columns, 3)

	assert.Equal(t, "id", got.Table.Columns[0].Name)
	assert.Equal(t, "bigint", got.Table.Columns[0].Type)
	assert.Equal(t, diff.CommonTypeNumeric, got.Table.Columns[0].NormalizedType)
	assert.False(t, got.Table.Columns[0].Nullable)
	assert.Nil(t, got.Table.Columns[0].Stats)

	assert.Equal(t, "status", got.Table.Columns[1].Name)
	assert.Equal(t, diff.CommonTypeString, got.Table.Columns[1].NormalizedType)
	assert.True(t, got.Table.Columns[1].Nullable)
	assert.Nil(t, got.Table.Columns[1].Stats)

	assert.Equal(t, "metadata", got.Table.Columns[2].Name)
	assert.Equal(t, diff.CommonTypeJSON, got.Table.Columns[2].NormalizedType)
	assert.Nil(t, got.Table.Columns[2].Stats)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetTableSummaryFull(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
	mock.ExpectQuery(`SELECT COUNT(*) as row_count FROM "analytics"."orders"`).
		WillReturnRows(sqlmock.NewRows([]string{"row_count"}).AddRow(4))
	mock.ExpectQuery(buildAthenaSchemaQuery("analytics", "orders")).
		WillReturnRows(
			sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
				AddRow("amount", "decimal(10,2)", "YES").
				AddRow("customer_name", "varchar", "YES").
				AddRow("paid", "boolean", "YES").
				AddRow("created_at", "timestamp", "NO").
				AddRow("payload", "json", "YES").
				AddRow("raw_bytes", "varbinary", "YES"),
		)
	mock.ExpectQuery(`
SELECT
    MIN(TRY_CAST("amount" AS DOUBLE)) as min_val,
    MAX(TRY_CAST("amount" AS DOUBLE)) as max_val,
    AVG(TRY_CAST("amount" AS DOUBLE)) as avg_val,
    SUM(TRY_CAST("amount" AS DOUBLE)) as sum_val,
    COUNT("amount") as count_val,
    COUNT(*) - COUNT("amount") as null_count,
    STDDEV(TRY_CAST("amount" AS DOUBLE)) as stddev_val
FROM "analytics"."orders"
`).WillReturnRows(sqlmock.NewRows([]string{"min_val", "max_val", "avg_val", "sum_val", "count_val", "null_count", "stddev_val"}).
		AddRow(1.5, 9.5, 5.5, 22.0, 4, 1, 3.1))
	mock.ExpectQuery(`
SELECT
    MIN(LENGTH(CAST("customer_name" AS VARCHAR))) as min_len,
    MAX(LENGTH(CAST("customer_name" AS VARCHAR))) as max_len,
    AVG(LENGTH(CAST("customer_name" AS VARCHAR))) as avg_len,
    COUNT(DISTINCT "customer_name") as distinct_count,
    COUNT(*) as total_count,
    COUNT(*) - COUNT("customer_name") as null_count,
    SUM(CASE WHEN CAST("customer_name" AS VARCHAR) = '' THEN 1 ELSE 0 END) as empty_count
FROM "analytics"."orders"
`).WillReturnRows(sqlmock.NewRows([]string{"min_len", "max_len", "avg_len", "distinct_count", "total_count", "null_count", "empty_count"}).
		AddRow(3, 12, 7.5, 3, 4, 1, 1))
	mock.ExpectQuery(`
SELECT
    SUM(CASE WHEN "paid" = true THEN 1 ELSE 0 END) as true_count,
    SUM(CASE WHEN "paid" = false THEN 1 ELSE 0 END) as false_count,
    COUNT(*) as total_count,
    COUNT(*) - COUNT("paid") as null_count
FROM "analytics"."orders"
`).WillReturnRows(sqlmock.NewRows([]string{"true_count", "false_count", "total_count", "null_count"}).
		AddRow(2, 1, 4, 1))
	mock.ExpectQuery(`
SELECT
    CAST(MIN("created_at") AS VARCHAR) as min_date,
    CAST(MAX("created_at") AS VARCHAR) as max_date,
    COUNT(DISTINCT "created_at") as unique_count,
    COUNT(*) as count_val,
    COUNT(*) - COUNT("created_at") as null_count
FROM "analytics"."orders"
`).WillReturnRows(sqlmock.NewRows([]string{"min_date", "max_date", "unique_count", "count_val", "null_count"}).
		AddRow("2024-01-01 00:00:00", "2024-01-03 12:00:00", 3, 4, 1))
	mock.ExpectQuery(`
SELECT
    COUNT(*) as count_val,
    COUNT(*) - COUNT("payload") as null_count
FROM "analytics"."orders"
`).WillReturnRows(sqlmock.NewRows([]string{"count_val", "null_count"}).AddRow(4, 1))

	db := DB{
		conn:       sqlxDB,
		config:     &Config{Database: "unused"},
		typeMapper: diff.NewAthenaTypeMapper(),
	}

	got, err := db.GetTableSummary(t.Context(), "analytics.orders", false)
	require.NoError(t, err)

	require.Equal(t, int64(4), got.RowCount)
	require.Equal(t, "analytics.orders", got.Table.Name)
	require.Len(t, got.Table.Columns, 6)

	amountStats, ok := got.Table.Columns[0].Stats.(*diff.NumericalStatistics)
	require.True(t, ok)
	require.NotNil(t, amountStats.Min)
	require.NotNil(t, amountStats.Max)
	require.NotNil(t, amountStats.Avg)
	require.NotNil(t, amountStats.Sum)
	require.NotNil(t, amountStats.StdDev)
	assert.InDelta(t, 1.5, *amountStats.Min, 0.0001)
	assert.InDelta(t, 9.5, *amountStats.Max, 0.0001)
	assert.InDelta(t, 5.5, *amountStats.Avg, 0.0001)
	assert.InDelta(t, 22.0, *amountStats.Sum, 0.0001)
	assert.Equal(t, int64(4), amountStats.Count)
	assert.Equal(t, int64(1), amountStats.NullCount)
	assert.InDelta(t, 3.1, *amountStats.StdDev, 0.0001)

	stringStats, ok := got.Table.Columns[1].Stats.(*diff.StringStatistics)
	require.True(t, ok)
	assert.Equal(t, 3, stringStats.MinLength)
	assert.Equal(t, 12, stringStats.MaxLength)
	assert.InDelta(t, 7.5, stringStats.AvgLength, 0.0001)
	assert.Equal(t, int64(3), stringStats.DistinctCount)
	assert.Equal(t, int64(4), stringStats.Count)
	assert.Equal(t, int64(1), stringStats.NullCount)
	assert.Equal(t, int64(1), stringStats.EmptyCount)

	booleanStats, ok := got.Table.Columns[2].Stats.(*diff.BooleanStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(2), booleanStats.TrueCount)
	assert.Equal(t, int64(1), booleanStats.FalseCount)
	assert.Equal(t, int64(4), booleanStats.Count)
	assert.Equal(t, int64(1), booleanStats.NullCount)

	dateTimeStats, ok := got.Table.Columns[3].Stats.(*diff.DateTimeStatistics)
	require.True(t, ok)
	require.NotNil(t, dateTimeStats.EarliestDate)
	require.NotNil(t, dateTimeStats.LatestDate)
	assert.Equal(t, int64(3), dateTimeStats.UniqueCount)
	assert.Equal(t, int64(4), dateTimeStats.Count)
	assert.Equal(t, int64(1), dateTimeStats.NullCount)

	jsonStats, ok := got.Table.Columns[4].Stats.(*diff.JSONStatistics)
	require.True(t, ok)
	assert.Equal(t, int64(4), jsonStats.Count)
	assert.Equal(t, int64(1), jsonStats.NullCount)

	_, ok = got.Table.Columns[5].Stats.(*diff.UnknownStatistics)
	require.True(t, ok)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetTableSummaryInvalidTableName(t *testing.T) {
	t.Parallel()

	db := DB{config: &Config{Database: "analytics"}}

	_, err := db.GetTableSummary(t.Context(), "a.b.c", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table name must be in table or schema.table format")
}

func TestDB_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		db          *DB
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:        "invalid format - empty component",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   ".test_table",
			wantErr:     true,
			errContains: "table name must be in table format, '.test_table' given",
		},
		{
			name:        "invalid format - empty component 2",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "table name must be in table format, '.' given",
		},
		{
			name:        "invalid format - too many components",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   "schema.table",
			wantErr:     true,
			errContains: "table name must be in table format, 'schema.table' given",
		},
		{
			name:      "invalid format - empty table name",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test_db' AND table_name = ''",
			wantErr:   false,
		},
		{
			name:        "invalid format - too many components",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in table format, 'a.b.c.d' given",
		},
		{
			name:      "valid table format",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "test_table",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test_db' AND table_name = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid table format with mixed case",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "TestTable",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test_db' AND table_name = 'TestTable'",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotQuery, err := tt.db.BuildTableExistsQuery(tt.tableName)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantQuery, gotQuery)
		})
	}
}

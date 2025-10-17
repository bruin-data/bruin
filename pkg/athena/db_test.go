package athena

import (
	"context"
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
					WillReturnRows(sqlmock.NewRows([]string{"one", "two", "three"}).
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

			got, err := db.Select(context.Background(), &tt.query)
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

			err = db.Ping(context.Background())
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
			got, err := db.SelectWithSchema(context.Background(), &tt.query)

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

func TestDB_GetTableSummary_WithStatistics(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	rowCountQuery := `SELECT COUNT(*) AS row_count FROM "default"."orders"`
	mock.ExpectQuery(rowCountQuery).
		WillReturnRows(sqlmock.NewRows([]string{"row_count"}).AddRow(int64(42)))

	schemaQuery := `SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'default' AND table_name = 'orders'
ORDER BY ordinal_position;`

	mock.ExpectQuery(schemaQuery).WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("id", "integer", "YES"),
	)

	numericStatsQuery := `SELECT
    COUNT(*) AS count,
    COUNT(*) - COUNT("id") AS null_count,
    MIN("id") AS min_val,
    MAX("id") AS max_val,
    AVG(CAST("id" AS DOUBLE)) AS avg_val,
    SUM(CAST("id" AS DOUBLE)) AS sum_val,
    STDDEV_POP(CAST("id" AS DOUBLE)) AS stddev_val
FROM "default"."orders"`

	mock.ExpectQuery(numericStatsQuery).WillReturnRows(
		sqlmock.NewRows([]string{
			"count",
			"null_count",
			"min_val",
			"max_val",
			"avg_val",
			"sum_val",
			"stddev_val",
		}).AddRow(
			int64(100),
			int64(0),
			int64(1),
			int64(100),
			float64(50.5),
			float64(5050.0),
			float64(10.0),
		),
	)

	db := NewDB(&Config{Database: "default"})
	db.conn = sqlxDB

	summary, err := db.GetTableSummary(context.Background(), "orders", false)
	require.NoError(t, err)

	require.Equal(t, int64(42), summary.RowCount)
	require.NotNil(t, summary.Table)
	require.Equal(t, "orders", summary.Table.Name)
	require.Len(t, summary.Table.Columns, 1)

	column := summary.Table.Columns[0]
	require.Equal(t, "id", column.Name)
	require.Equal(t, diff.CommonTypeNumeric, column.NormalizedType)
	require.True(t, column.Nullable)
	require.NotNil(t, column.Stats)

	numericStats, ok := column.Stats.(*diff.NumericalStatistics)
	require.True(t, ok)
	require.Equal(t, int64(100), numericStats.Count)
	require.Equal(t, int64(0), numericStats.NullCount)
	require.NotNil(t, numericStats.Min)
	require.Equal(t, 1.0, *numericStats.Min)
	require.NotNil(t, numericStats.Max)
	require.Equal(t, 100.0, *numericStats.Max)
	require.NotNil(t, numericStats.Avg)
	require.Equal(t, 50.5, *numericStats.Avg)
	require.NotNil(t, numericStats.Sum)
	require.Equal(t, 5050.0, *numericStats.Sum)
	require.NotNil(t, numericStats.StdDev)
	require.Equal(t, 10.0, *numericStats.StdDev)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetTableSummary_SchemaOnly(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	schemaQuery := `SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'default' AND table_name = 'orders'
ORDER BY ordinal_position;`

	mock.ExpectQuery(schemaQuery).WillReturnRows(
		sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).
			AddRow("customer_name", "varchar", "NO"),
	)

	db := NewDB(&Config{Database: "default"})
	db.conn = sqlxDB

	summary, err := db.GetTableSummary(context.Background(), "orders", true)
	require.NoError(t, err)

	require.Equal(t, int64(0), summary.RowCount)
	require.Len(t, summary.Table.Columns, 1)

	column := summary.Table.Columns[0]
	require.Equal(t, "customer_name", column.Name)
	require.Equal(t, diff.CommonTypeString, column.NormalizedType)
	require.False(t, column.Nullable)
	require.Nil(t, column.Stats)

	require.NoError(t, mock.ExpectationsWereMet())
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

package athena

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestDB_GetTablesEscapesDatabaseName(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(`
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'dev_o''brien'
    AND table_type IN ('BASE TABLE', 'VIEW')
ORDER BY table_name;
`).WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow("orders"))

	db := DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	tables, err := db.GetTables(t.Context(), "dev_o'brien")

	require.NoError(t, err)
	require.Equal(t, []string{"orders"}, tables)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDB_GetColumnsEscapesDatabaseAndTableNames(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(`
SELECT
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'dev_o''brien' AND table_name = 'order''s'
ORDER BY ordinal_position;
`).WillReturnRows(sqlmock.NewRows([]string{"column_name", "data_type", "is_nullable"}).AddRow("id", "bigint", "NO"))

	db := DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	columns, err := db.GetColumns(t.Context(), "dev_o'brien", "order's")

	require.NoError(t, err)
	require.Len(t, columns, 1)
	require.Equal(t, "id", columns[0].Name)
	require.Equal(t, "bigint", columns[0].Type)
	require.False(t, columns[0].Nullable)
	require.NoError(t, mock.ExpectationsWereMet())
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
			errContains: "contains an empty component",
		},
		{
			name:        "invalid format - empty component 2",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "contains an empty component",
		},
		{
			name:      "valid database.table format",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "schema.table",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'schema' AND table_name = 'table'",
			wantErr:   false,
		},
		{
			name:        "invalid format - empty table name",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   "",
			wantErr:     true,
			errContains: "contains an empty component",
		},
		{
			name:        "invalid format - too many components",
			db:          &DB{config: &Config{Database: "test_db"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "must be in format",
		},
		{
			name:      "valid catalog.database.table format",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "catalog.database.table",
			wantQuery: "SELECT COUNT(*) FROM \"catalog\".information_schema.tables WHERE table_schema = 'database' AND table_name = 'table'",
			wantErr:   false,
		},
		{
			name:      "escapes schema and table string literals",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: "dev_o'brien.order's",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dev_o''brien' AND table_name = 'order''s'",
			wantErr:   false,
		},
		{
			name:      "escapes default database string literal",
			db:        &DB{config: &Config{Database: "dev_o'brien"}},
			tableName: "orders",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dev_o''brien' AND table_name = 'orders'",
			wantErr:   false,
		},
		{
			name:      "quotes and escapes catalog identifier",
			db:        &DB{config: &Config{Database: "test_db"}},
			tableName: `cat"alog.database.table`,
			wantQuery: `SELECT COUNT(*) FROM "cat""alog".information_schema.tables WHERE table_schema = 'database' AND table_name = 'table'`,
			wantErr:   false,
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

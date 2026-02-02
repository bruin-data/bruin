package mssql

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
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

func TestDB_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		config         *Config
		want           *ansisql.DBDatabase
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "successful database summary with multiple schemas",
			mockConnection: func(mock sqlmock.Sqlmock) {
				expectedQuery := `
USE [testdb];
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    v.VIEW_DEFINITION
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.VIEWS v ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "VIEW_DEFINITION"}).
						AddRow("dbo", "users", "BASE TABLE", nil).
						AddRow("dbo", "orders", "BASE TABLE", nil).
						AddRow("sales", "products", "BASE TABLE", nil).
						AddRow("sales", "categories", "BASE TABLE", nil))
			},
			config: &Config{Database: "testdb"},
			want: &ansisql.DBDatabase{
				Name: "testdb",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "dbo",
						Tables: []*ansisql.DBTable{
							{Name: "users", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
							{Name: "orders", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "sales",
						Tables: []*ansisql.DBTable{
							{Name: "products", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
							{Name: "categories", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "successful database summary with single schema",
			mockConnection: func(mock sqlmock.Sqlmock) {
				expectedQuery := `
USE [testdb];
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    v.VIEW_DEFINITION
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.VIEWS v ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "VIEW_DEFINITION"}).
						AddRow("dbo", "users", "BASE TABLE", nil).
						AddRow("dbo", "orders", "BASE TABLE", nil))
			},
			config: &Config{Database: "testdb"},
			want: &ansisql.DBDatabase{
				Name: "testdb",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "dbo",
						Tables: []*ansisql.DBTable{
							{Name: "users", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
							{Name: "orders", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "empty database returns empty schemas",
			mockConnection: func(mock sqlmock.Sqlmock) {
				expectedQuery := `
USE [testdb];
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    v.VIEW_DEFINITION
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.VIEWS v ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "VIEW_DEFINITION"}))
			},
			config: &Config{Database: "testdb"},
			want: &ansisql.DBDatabase{
				Name:    "testdb",
				Schemas: []*ansisql.DBSchema{},
			},
			wantErr: false,
		},
		{
			name: "database query error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				expectedQuery := `
USE [testdb];
SELECT
    t.TABLE_SCHEMA,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    v.VIEW_DEFINITION
FROM
    INFORMATION_SCHEMA.TABLES t
LEFT JOIN
    INFORMATION_SCHEMA.VIEWS v ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND t.TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnError(errors.New("database connection failed"))
			},
			config:       &Config{Database: "testdb"},
			want:         nil,
			wantErr:      true,
			errorMessage: "failed to query SQL Server information_schema: database connection failed",
		},
		{
			name: "missing database name in config",
			mockConnection: func(mock sqlmock.Sqlmock) {
				// No query expected since it should fail before executing
			},
			config:       &Config{Database: ""},
			want:         nil,
			wantErr:      true,
			errorMessage: "database name not configured",
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
			db := DB{conn: sqlxDB, config: tt.config}

			got, err := db.GetDatabaseSummary(t.Context())
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
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
			name: "simple select query with schema",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT id, name FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
						AddRow(1, "John").
						AddRow(2, "Jane"))
			},
			query: query.Query{
				Query: "SELECT id, name FROM users",
			},
			want: &query.QueryResult{
				Columns:     []string{"id", "name"},
				Rows:        [][]interface{}{{int64(1), "John"}, {int64(2), "Jane"}},
				ColumnTypes: []string{"", ""}, // sqlmock doesn't provide real column types
			},
		},
		{
			name: "empty result set",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT id, name FROM users WHERE id = -1`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
			},
			query: query.Query{
				Query: "SELECT id, name FROM users WHERE id = -1",
			},
			want: &query.QueryResult{
				Columns:     []string{"id", "name"},
				Rows:        nil,
				ColumnTypes: []string{"", ""},
			},
		},
		{
			name: "query execution error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM nonexistent_table`).
					WillReturnError(errors.New("table does not exist"))
			},
			query: query.Query{
				Query: "SELECT * FROM nonexistent_table",
			},
			wantErr:      true,
			errorMessage: "failed to execute query: table does not exist",
		},
		{
			name: "column scanning error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT id FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"id"}).
						AddRow("invalid_int").
						RowError(0, errors.New("scan error")))
			},
			query: query.Query{
				Query: "SELECT id FROM users",
			},
			wantErr:      true,
			errorMessage: "error iterating rows: scan error",
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

			got, err := db.SelectWithSchema(t.Context(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}

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
			name: "successful ping",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnRows(sqlmock.NewRows([]string{"column"}).AddRow(1))
			},
			wantErr: false,
		},
		{
			name: "connection failure",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnError(errors.New("connection refused"))
			},
			wantErr:      true,
			errorMessage: "failed to run test query on SQL Server connection: connection refused",
		},
		{
			name: "database unavailable",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnError(errors.New("database is not available"))
			},
			wantErr:      true,
			errorMessage: "failed to run test query on SQL Server connection: database is not available",
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

func TestClient_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		c           *DB
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:        "invalid format - empty component",
			c:           &DB{config: &Config{Database: "test_db"}},
			tableName:   ".test_table",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.test_table' given",
		},
		{
			name:        "invalid format - empty component 2",
			c:           &DB{config: &Config{Database: "test_db"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.' given",
		},
		{
			name:        "invalid format - empty table name",
			c:           &DB{config: &Config{Database: "test_db"}},
			tableName:   "",
			wantQuery:   "",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '' given",
		},
		{
			name:        "invalid format - too many components",
			c:           &DB{config: &Config{Database: "test_db"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, 'a.b.c.d' given",
		},
		{
			name:      "valid table format (defaults to dbo schema)",
			c:         &DB{config: &Config{Database: ""}},
			tableName: "test_table",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dbo' AND table_name = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format",
			c:         &DB{config: &Config{Database: "test_db"}},
			tableName: "test_schema.test_table",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test_schema' AND table_name = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format with mixed case",
			c:         &DB{config: &Config{Database: "test_db"}},
			tableName: "TestSchema.TestTable",
			wantQuery: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'TestSchema' AND table_name = 'TestTable'",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotQuery, err := tt.c.BuildTableExistsQuery(tt.tableName)

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

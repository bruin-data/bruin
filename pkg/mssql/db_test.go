package mssql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
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
    TABLE_SCHEMA,
    TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
						AddRow("dbo", "users").
						AddRow("dbo", "orders").
						AddRow("sales", "products").
						AddRow("sales", "categories"))
			},
			config: &Config{Database: "testdb"},
			want: &ansisql.DBDatabase{
				Name: "testdb",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "dbo",
						Tables: []*ansisql.DBTable{
							{Name: "users", Columns: []*ansisql.DBColumn{}},
							{Name: "orders", Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "sales",
						Tables: []*ansisql.DBTable{
							{Name: "products", Columns: []*ansisql.DBColumn{}},
							{Name: "categories", Columns: []*ansisql.DBColumn{}},
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
    TABLE_SCHEMA,
    TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}).
						AddRow("dbo", "users").
						AddRow("dbo", "orders"))
			},
			config: &Config{Database: "testdb"},
			want: &ansisql.DBDatabase{
				Name: "testdb",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "dbo",
						Tables: []*ansisql.DBTable{
							{Name: "users", Columns: []*ansisql.DBColumn{}},
							{Name: "orders", Columns: []*ansisql.DBColumn{}},
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
    TABLE_SCHEMA,
    TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
`
				mock.ExpectQuery(expectedQuery).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_SCHEMA", "TABLE_NAME"}))
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
    TABLE_SCHEMA,
    TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE IN ('BASE TABLE', 'VIEW')
    AND TABLE_SCHEMA NOT IN ('sys', 'information_schema')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
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
			name:         "missing database name in config",
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

			got, err := db.GetDatabaseSummary(context.Background())
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

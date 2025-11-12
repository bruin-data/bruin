package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Select(t *testing.T) {
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
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnError(errors.New("syntax error"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "failed to execute query: syntax error",
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
			client := Client{conn: sqlxDB}

			got, err := client.Select(context.Background(), &tt.query)
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

func TestClient_SelectWithSchema(t *testing.T) {
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
			name: "simple successful query with schema",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT first_name, last_name, age FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name", "age"}).
						AddRow("jane", "doe", 30).
						AddRow("joe", "doe", 28))
			},
			query: query.Query{
				Query: "SELECT first_name, last_name, age FROM users",
			},
			want: &query.QueryResult{
				Columns: []string{"first_name", "last_name", "age"},
				Rows: [][]interface{}{
					{"jane", "doe", int64(30)},
					{"joe", "doe", int64(28)},
				},
			},
		},
		{
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnError(errors.New("syntax error"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "syntax error",
		},
		{
			name: "empty result set is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM empty_table`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
			},
			query: query.Query{
				Query: "SELECT * FROM empty_table",
			},
			want: &query.QueryResult{
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{},
			},
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
			client := Client{conn: sqlxDB}

			got, err := client.SelectWithSchema(context.Background(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_Ping(t *testing.T) {
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
				mock.ExpectExec(`SELECT 1`).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: false,
		},
		{
			name: "failed ping",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`SELECT 1`).
					WillReturnError(errors.New("connection refused"))
			},
			wantErr:      true,
			errorMessage: "failed to run test query on MySQL connection: failed to execute query: connection refused",
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
			client := Client{conn: sqlxDB}

			err = client.Ping(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, tt.errorMessage, err.Error())
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
		name      string
		tableName string
		want      string
		wantErr   assert.ErrorAssertionFunc
		errMsg    string
	}{
		{
			name:      "table only uses current database",
			tableName: "orders",
			want:      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'orders'",
			wantErr:   assert.NoError,
		},
		{
			name:      "schema and table specified",
			tableName: "analytics.orders",
			want:      "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'orders'",
			wantErr:   assert.NoError,
		},
		{
			name:      "empty component returns error",
			tableName: "analytics.",
			wantErr:   assert.Error,
			errMsg:    "table name must be in format schema.table or table, 'analytics.' given",
		},
		{
			name:      "too many components returns error",
			tableName: "a.b.c",
			wantErr:   assert.Error,
			errMsg:    "table name must be in format schema.table or table, 'a.b.c' given",
		},
	}

	c := &Client{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := c.BuildTableExistsQuery(tt.tableName)

			if !tt.wantErr(t, err) {
				return
			}

			if err != nil {
				assert.EqualError(t, err, tt.errMsg)
				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestClient_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		want           *ansisql.DBDatabase
		wantErr        string
	}{
		{
			name: "successful database summary",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    table_type IN \('BASE TABLE', 'VIEW'\)
    AND table_schema NOT IN \('information_schema', 'performance_schema', 'mysql', 'sys'\)
ORDER BY table_schema, table_name;`).
					WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name"}).
						AddRow("schema1", "table1").
						AddRow("schema1", "table2").
						AddRow("schema2", "table1"))
			},
			want: &ansisql.DBDatabase{
				Name: "mysql",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "schema1",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Columns: []*ansisql.DBColumn{}},
							{Name: "table2", Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "schema2",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
		},
		{
			name: "query error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
    table_type IN \('BASE TABLE', 'VIEW'\)
    AND table_schema NOT IN \('information_schema', 'performance_schema', 'mysql', 'sys'\)
ORDER BY table_schema, table_name;`).
					WillReturnError(errors.New("connection error"))
			},
			wantErr: "failed to query MySQL information_schema: failed to execute query: connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			client := Client{conn: sqlxDB}

			got, err := client.GetDatabaseSummary(context.Background())
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

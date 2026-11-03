package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

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

			got, err := client.Select(t.Context(), &tt.query)
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

type streamingHandler struct {
	t           *testing.T
	columns     []string
	columnTypes []string
	rows        [][]interface{}
	schemaErr   error
	rowErr      error
}

func (h *streamingHandler) Schema(columns, types []string) error {
	require.Nil(h.t, h.columns, "schema must be sent once")
	h.columns, h.columnTypes = columns, types
	return h.schemaErr
}

func (h *streamingHandler) Row(row []interface{}) error {
	require.NotNil(h.t, h.columns, "schema must precede rows")
	h.rows = append(h.rows, row)
	return h.rowErr
}

func TestClient_StreamWithSchema(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "empty", "schema error", "handler error", "read error"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
			rows := sqlmock.NewRowsWithColumnDefinition(
				sqlmock.NewColumn("id").OfType("BIGINT", int64(0)),
				sqlmock.NewColumn("name").OfType("VARCHAR", ""),
			)
			if outcome != "empty" {
				rows.AddRow(7, []byte("a,\"b\n")).AddRow(23, nil)
			}
			h := &streamingHandler{t: t}
			switch outcome {
			case "schema error":
				h.schemaErr = assert.AnError
			case "handler error":
				h.rowErr = assert.AnError
			case "read error":
				rows.RowError(1, assert.AnError)
			}
			mock.ExpectQuery("SELECT id, name FROM users").WillReturnRows(rows).RowsWillBeClosed()
			result, err := client.StreamWithSchema(t.Context(), &query.Query{Query: "SELECT id, name FROM users"}, h)
			assert.Equal(t, []string{"id", "name"}, h.columns)
			assert.Equal(t, []string{"BIGINT", "VARCHAR"}, h.columnTypes)
			switch outcome {
			case "success":
				require.NoError(t, err)
				assert.Equal(t, [][]interface{}{{int64(7), "a,\"b\n"}, {int64(23), nil}}, h.rows)
			case "empty":
				require.NoError(t, err)
				assert.Empty(t, h.rows)
			case "schema error":
				require.ErrorIs(t, err, assert.AnError)
				assert.Empty(t, h.rows)
			default:
				require.ErrorIs(t, err, assert.AnError)
				assert.Equal(t, [][]interface{}{{int64(7), "a,\"b\n"}}, h.rows)
			}
			if err == nil {
				assert.Equal(t, &query.QueryResult{Columns: h.columns, ColumnTypes: h.columnTypes}, result)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_StreamWithSchemaCancelsBeforeClose(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"schema", "row"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			rows := &cancelBeforeCloseRows{closed: make(chan error, 1)}
			db := sql.OpenDB(cancelBeforeCloseConnector{rows: rows})
			defer db.Close()
			client := &Client{conn: sqlx.NewDb(db, "test")}
			h := &streamingHandler{t: t}
			if stage == "schema" {
				h.schemaErr = assert.AnError
			} else {
				h.rowErr = assert.AnError
			}
			result, err := client.StreamWithSchema(t.Context(), &query.Query{Query: "SELECT id FROM users"}, h)
			require.ErrorIs(t, err, assert.AnError)
			require.Nil(t, result)
			require.Equal(t, []string{"id"}, h.columns)
			if stage == "schema" {
				require.Empty(t, h.rows)
			} else {
				require.Equal(t, [][]interface{}{{int64(1)}}, h.rows)
			}
			// database/sql may auto-close on cancellation before the explicit Close.
			select {
			case err := <-rows.closed:
				require.ErrorIs(t, err, context.Canceled, "Close would drain uncanceled results")
			case <-time.After(5 * time.Second):
				t.Fatal("driver rows were not closed")
			}
		})
	}
}

type cancelBeforeCloseConnector struct{ rows *cancelBeforeCloseRows }

func (c cancelBeforeCloseConnector) Connect(context.Context) (driver.Conn, error) {
	return cancelBeforeCloseConn(c), nil
}

func (c cancelBeforeCloseConnector) Driver() driver.Driver { return cancelBeforeCloseDriver{} }

type cancelBeforeCloseDriver struct{}

func (cancelBeforeCloseDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type cancelBeforeCloseConn struct{ rows *cancelBeforeCloseRows }

func (cancelBeforeCloseConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not supported")
}

func (cancelBeforeCloseConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not supported")
}

func (cancelBeforeCloseConn) Close() error { return nil }

func (c cancelBeforeCloseConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	c.rows.contextErr = ctx.Err
	return c.rows, nil
}

type cancelBeforeCloseRows struct {
	contextErr func() error
	closed     chan error
}

func (*cancelBeforeCloseRows) Columns() []string { return []string{"id"} }

func (*cancelBeforeCloseRows) Next(dest []driver.Value) error {
	// Always leave unread results for Close, even after the first row callback.
	dest[0] = int64(1)
	return nil
}

func (r *cancelBeforeCloseRows) Close() error {
	// Record whether a draining driver would block here, without actually
	// blocking: a regression must not strand database/sql's auto-close goroutine.
	r.closed <- r.contextErr()
	return nil
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
					WillReturnRows(sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("first_name").OfType("VARCHAR", ""), sqlmock.NewColumn("last_name").OfType("VARCHAR", ""), sqlmock.NewColumn("age").OfType("INT", 0)).
						AddRow("jane", "doe", 30).
						AddRow("joe", "doe", 28))
			},
			query: query.Query{
				Query: "SELECT first_name, last_name, age FROM users",
			},
			want: &query.QueryResult{
				Columns:     []string{"first_name", "last_name", "age"},
				ColumnTypes: []string{"VARCHAR", "VARCHAR", "INT"},
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
				Columns:     []string{"id", "name"},
				ColumnTypes: []string{"", ""},
				Rows:        [][]interface{}{},
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

			got, err := client.SelectWithSchema(t.Context(), &tt.query)
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

			err = client.Ping(t.Context())
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
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition,
    t.create_time,
    t.update_time,
    t.table_rows,
    t.data_length,
    t.table_comment
FROM
    information_schema.tables t
LEFT JOIN
    information_schema.views v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE
    t.table_type IN \('BASE TABLE', 'VIEW'\)
    AND t.table_schema NOT IN \('information_schema', 'performance_schema', 'mysql', 'sys'\)
ORDER BY t.table_schema, t.table_name;`).
					WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name", "table_type", "view_definition", "create_time", "update_time", "table_rows", "data_length", "table_comment"}).
						AddRow("schema1", "table1", "BASE TABLE", nil, nil, nil, nil, nil, nil).
						AddRow("schema1", "table2", "BASE TABLE", nil, nil, nil, nil, nil, nil).
						AddRow("schema2", "table1", "BASE TABLE", nil, nil, nil, nil, nil, nil))
			},
			want: &ansisql.DBDatabase{
				Name: "mysql",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "schema1",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
							{Name: "table2", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "schema2",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
		},
		{
			name: "query error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    t.table_schema,
    t.table_name,
    t.table_type,
    v.view_definition,
    t.create_time,
    t.update_time,
    t.table_rows,
    t.data_length,
    t.table_comment
FROM
    information_schema.tables t
LEFT JOIN
    information_schema.views v ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE
    t.table_type IN \('BASE TABLE', 'VIEW'\)
    AND t.table_schema NOT IN \('information_schema', 'performance_schema', 'mysql', 'sys'\)
ORDER BY t.table_schema, t.table_name;`).
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

			got, err := client.GetDatabaseSummary(t.Context())
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

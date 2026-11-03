package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockColumnType struct {
	ColumnName string
	TypeName   string
}

func (m MockColumnType) Name() string             { return m.ColumnName }
func (m MockColumnType) Nullable() bool           { return false }
func (m MockColumnType) ScanType() reflect.Type   { return nil }
func (m MockColumnType) DatabaseTypeName() string { return m.TypeName }

type MockRows struct {
	index     *int
	rows      [][]any
	types     []MockColumnType
	scanError error
	readError error
	closed    *bool
}

func (r MockRows) ColumnTypes() []driver.ColumnType {
	result := make([]driver.ColumnType, 0, len(r.types))
	for _, col := range r.types {
		result = append(result, col)
	}
	return result
}

func (r MockRows) Next() bool {
	(*r.index)++
	return (*r.index) <= len(r.rows)
}

func (r MockRows) Scan(dest ...any) error {
	if r.scanError != nil {
		return r.scanError
	}

	data := r.rows[(*r.index)-1]
	for i, d := range dest {
		scanner, ok := d.(interface{ Scan(src any) error })
		if !ok {
			return errors.New("dest argument does not implement Scan(any) error")
		}
		if err := scanner.Scan(data[i]); err != nil {
			return err
		}
	}

	return nil
}
func (r MockRows) ScanStruct(dest any) error { return nil }
func (r MockRows) Totals(dest ...any) error  { return nil }
func (r MockRows) Columns() []string         { return []string{"name", "age"} }
func (r MockRows) Close() error {
	if r.closed != nil {
		*r.closed = true
	}
	return nil
}

func (r MockRows) Err() error { return r.readError }

type MockConn struct {
	mock.Mock
}

func (m *MockConn) Contributors() []string { return []string{""} }

func (m *MockConn) ServerVersion() (*driver.ServerVersion, error) { return nil, nil }

func (m *MockConn) Select(ctx context.Context, dest any, query string, args ...any) error { return nil }

func (m *MockConn) Query(ctx context.Context, query string, _ ...any) (driver.Rows, error) {
	res := m.Called(ctx, query)
	rows := res.Get(0)
	if rows == nil {
		return nil, res.Error(1)
	}
	return res.Get(0).(driver.Rows), res.Error(1)
}

func (m *MockConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row { return nil }

func (m *MockConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, nil
}

func (m *MockConn) Exec(ctx context.Context, query string, args ...any) error {
	res := m.Called(ctx, query)
	return res.Error(0)
}

func (m *MockConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return nil
}
func (m *MockConn) Ping(context.Context) error { return nil }
func (m *MockConn) Stats() driver.Stats        { return driver.Stats{} }
func (m *MockConn) Close() error               { return nil }

func TestClient_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		expected  string
		setupMock func(conn *MockConn)
		wantErr   string
		want      [][]interface{}
	}{
		{
			name:    "test select rows",
			query:   "SELECT * FROM table",
			wantErr: "",
			want:    [][]interface{}{{1, "John Doe"}, {2, "Jane Doe"}},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  [][]any{{1, "John Doe"}, {2, "Jane Doe"}},
					types: []MockColumnType{{ColumnName: "id"}, {ColumnName: "name"}},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
		{
			name:  "test select single row",
			query: "SELECT * FROM table",
			want:  [][]interface{}{{1, "John Doe"}},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  [][]any{{1, "John Doe"}},
					types: []MockColumnType{{ColumnName: "id"}, {ColumnName: "name"}},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
		{
			name:  "test select empty rows",
			query: "SELECT * FROM table",
			want:  [][]interface{}{},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  nil,
					types: []MockColumnType{{ColumnName: "id"}, {ColumnName: "name"}},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
		{
			name:    "test select errors",
			query:   "SELECT * FROM table",
			wantErr: "Some error",
			want:    nil,
			setupMock: func(conn *MockConn) {
				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(nil, errors.New("Some error"))
			},
		},
		{
			name:    "test fail scanning rows errors",
			query:   "SELECT * FROM table",
			wantErr: "failed to scan row: Some scan error",
			want:    nil,
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index:     new(int),
					scanError: errors.New("Some scan error"),
					rows:      [][]any{{1, "John Doe"}},
					types:     []MockColumnType{{ColumnName: "id"}, {ColumnName: "name"}},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockConn := MockConn{}
			tt.setupMock(&mockConn)

			client := Client{connection: &mockConn}

			result, err := client.Select(t.Context(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				assert.Equal(t, tt.wantErr, err.Error())
				require.Error(t, err)
			}

			assert.Equal(t, tt.want, result)
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
	for _, outcome := range []string{"success", "empty", "schema error", "handler error", "read error", "exec"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			conn := &MockConn{}
			closed := false
			rows := MockRows{
				index: new(int), closed: &closed,
				types: []MockColumnType{{ColumnName: "id", TypeName: "Int64"}, {ColumnName: "name", TypeName: "Nullable(String)"}},
			}
			if outcome != "empty" {
				rows.rows = [][]any{{int64(7), "a,\"b\n"}, {int64(23), nil}}
			}
			h := &streamingHandler{t: t}
			switch outcome {
			case "schema error":
				h.schemaErr = assert.AnError
			case "handler error":
				h.rowErr = assert.AnError
			case "read error":
				rows.rows = rows.rows[:1]
				rows.readError = assert.AnError
			}
			sql := "SELECT id, name FROM users"
			if outcome == "exec" {
				sql = "CREATE TABLE users (id Int64)"
				conn.On("Exec", mock.Anything, sql).Return(nil).Once()
			} else {
				conn.On("Query", mock.Anything, sql).Return(rows, nil).Once()
			}
			client := &Client{connection: conn}
			result, err := client.StreamWithSchema(t.Context(), &query.Query{Query: sql}, h)
			if outcome == "exec" {
				require.NoError(t, err)
				assert.Equal(t, []string{}, h.columns)
				assert.Empty(t, h.rows)
				assert.Equal(t, query.NewExecutionSummaryFromStatement("clickhouse", "CREATE TABLE", nil), result.Execution)
			} else {
				assert.True(t, closed)
				assert.Equal(t, []string{"id", "name"}, h.columns)
				assert.Equal(t, []string{"Int64", "Nullable(String)"}, h.columnTypes)
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
			}
			if err == nil {
				assert.Nil(t, result.Rows)
			}
			conn.AssertExpectations(t)
		})
	}
}

func TestClient_SelectWithSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		expected  *query.QueryResult
		setupMock func(conn *MockConn)
		wantErr   string
	}{
		{
			name:  "test select rows with schema",
			query: "SELECT * FROM table",
			expected: &query.QueryResult{
				Columns: []string{"id", "name"},
				Rows: [][]interface{}{
					{1, "John Doe"},
					{2, "Jane Doe"},
				},
				ColumnTypes: []string{"BIGINT", "VARCHAR"},
			},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  [][]any{{1, "John Doe"}, {2, "Jane Doe"}},
					types: []MockColumnType{
						{ColumnName: "id", TypeName: "BIGINT"},
						{ColumnName: "name", TypeName: "VARCHAR"},
					},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
		{
			name:  "test select empty rows with schema",
			query: "SELECT * FROM table",
			expected: &query.QueryResult{
				Columns:     []string{"id", "name"},
				Rows:        [][]interface{}{},
				ColumnTypes: []string{"BIGINT", "VARCHAR"},
			},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  nil,
					types: []MockColumnType{
						{ColumnName: "id", TypeName: "BIGINT"},
						{ColumnName: "name", TypeName: "VARCHAR"},
					},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
		{
			name:  "test ClickHouse-specific result statement uses query",
			query: "KILL QUERY WHERE query_id = 'test-query' TEST",
			expected: &query.QueryResult{
				Columns:     []string{"kill_status"},
				Rows:        [][]interface{}{{"finished"}},
				ColumnTypes: []string{"String"},
			},
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index: new(int),
					rows:  [][]any{{"finished"}},
					types: []MockColumnType{{ColumnName: "kill_status", TypeName: "String"}},
				}

				conn.On("Query", mock.Anything, "KILL QUERY WHERE query_id = 'test-query' TEST").Return(rows, nil)
			},
		},
		{
			name:  "test insert uses exec and returns execution summary",
			query: "INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_7', 77)",
			expected: &query.QueryResult{
				Columns:     []string{},
				Rows:        [][]interface{}{},
				ColumnTypes: []string{},
				Execution:   query.NewExecutionSummaryFromStatement("clickhouse", "INSERT", nil),
			},
			setupMock: func(conn *MockConn) {
				conn.On(
					"Exec",
					mock.Anything,
					"INSERT INTO bug_test_seed (event_date, row_id, amount) VALUES (toDate('2026-07-16'), 'row_7', 77)",
				).Return(nil)
			},
		},
		{
			name:    "test insert execution error",
			query:   "INSERT INTO bug_test_seed (row_id) VALUES ('row_7')",
			wantErr: "failed to execute query: execution error",
			setupMock: func(conn *MockConn) {
				conn.On(
					"Exec",
					mock.Anything,
					"INSERT INTO bug_test_seed (row_id) VALUES ('row_7')",
				).Return(errors.New("execution error"))
			},
		},
		{
			name:    "test select errors with schema",
			query:   "SELECT * FROM table",
			wantErr: "failed to execute query: Some error", // Updated error message
			setupMock: func(conn *MockConn) {
				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(nil, errors.New("Some error"))
			},
		},
		{
			name:    "test fail scanning rows errors with schema",
			query:   "SELECT * FROM table",
			wantErr: "failed to scan row: Some scan error",
			setupMock: func(conn *MockConn) {
				rows := MockRows{
					index:     new(int),
					scanError: errors.New("Some scan error"),
					rows:      [][]any{{1, "John Doe"}},
					types:     []MockColumnType{{ColumnName: "id"}, {ColumnName: "name"}},
				}

				conn.On("Query", mock.Anything, "SELECT * FROM table").Return(rows, nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockConn := MockConn{}
			tt.setupMock(&mockConn)

			client := Client{connection: &mockConn}

			result, err := client.SelectWithSchema(t.Context(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result, "Expected QueryResult does not match actual")
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}

			// Assert column validation for expected schema
			if tt.expected != nil {
				assert.Equal(t, tt.expected.Columns, result.Columns, "Column names do not match")
			}
		})
	}
}

func TestShouldUseExec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "insert", sql: "INSERT INTO events VALUES (1)", want: true},
		{name: "insert into returning table", sql: "INSERT INTO returning VALUES (1)", want: true},
		{name: "insert into returning column", sql: "INSERT INTO events (returning) VALUES (1)", want: true},
		{name: "create", sql: "CREATE TABLE events (id UInt64) ENGINE = Memory", want: true},
		{name: "select", sql: "SELECT * FROM events", want: false},
		{name: "watch", sql: "WATCH events", want: false},
		{name: "kill query test", sql: "KILL QUERY WHERE query_id = 'test-query' TEST", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, shouldUseExec(tt.sql))
		})
	}
}

func TestClient_RunQueryWithoutResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		setupMock func(conn *MockConn)
		wantErr   string
	}{
		{
			name:  "test successful execution",
			query: "DELETE FROM table WHERE id = 1",
			setupMock: func(conn *MockConn) {
				conn.On("Exec", mock.Anything, "DELETE FROM table WHERE id = 1").Return(nil)
			},
			wantErr: "",
		},
		{
			name:  "test execution error",
			query: "DELETE FROM table WHERE id = 1",
			setupMock: func(conn *MockConn) {
				conn.On("Exec", mock.Anything, "DELETE FROM table WHERE id = 1").Return(errors.New("execution error"))
			},
			wantErr: "execution error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockConn := MockConn{}
			tt.setupMock(&mockConn)

			client := Client{connection: &mockConn}

			err := client.RunQueryWithoutResult(t.Context(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(conn *MockConn)
		wantErr   string
	}{
		{
			name: "test successful ping",
			setupMock: func(conn *MockConn) {
				conn.On("Exec", mock.Anything, "SELECT 1").Return(nil)
			},
			wantErr: "",
		},
		{
			name: "test ping with execution error",
			setupMock: func(conn *MockConn) {
				conn.On("Exec", mock.Anything, "SELECT 1").Return(errors.New("ping error"))
			},
			wantErr: "failed to run test query on ClickHouse connection: ping error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockConn := MockConn{}
			tt.setupMock(&mockConn)

			client := Client{connection: &mockConn}

			err := client.Ping(t.Context())

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

func TestClient_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		c           *Client
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:        "invalid format - empty component",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   ".test_table",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.test_table' given",
		},
		{
			name:        "invalid format - empty component 2",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.' given",
		},
		{
			name:        "invalid format - empty table name",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   "",
			wantQuery:   "",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '' given",
		},
		{
			name:        "invalid format - too many components",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, 'a.b.c.d' given",
		},
		{
			name:      "valid table format",
			c:         &Client{config: &Config{Database: ""}},
			tableName: "test_table",
			wantQuery: "SELECT COUNT(*) FROM system.tables WHERE database = 'default' AND name = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format",
			c:         &Client{config: &Config{Database: "test_db"}},
			tableName: "test_schema.test_table",
			wantQuery: "SELECT COUNT(*) FROM system.tables WHERE database = 'test_schema' AND name = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format with mixed case",
			c:         &Client{config: &Config{Database: "test_db"}},
			tableName: "TestSchema.TestTable",
			wantQuery: "SELECT COUNT(*) FROM system.tables WHERE database = 'TestSchema' AND name = 'TestTable'",
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

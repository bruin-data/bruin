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
}

func (m MockColumnType) Name() string             { return m.ColumnName }
func (m MockColumnType) Nullable() bool           { return false }
func (m MockColumnType) ScanType() reflect.Type   { return nil }
func (m MockColumnType) DatabaseTypeName() string { return "" }

type MockRows struct {
	index     *int
	rows      [][]any
	types     []MockColumnType
	scanError error
}

func (r MockRows) ColumnTypes() []driver.ColumnType {
	result := make([]driver.ColumnType, 0)
	for _, col := range r.types {
		result = append(result, col)
	}
	return result
}

func (r MockRows) Next() bool {
	(*r.index)++
	return (*r.index) <= len(r.rows)
}

type scanner interface {
	SetValues(values []any)
}

func (r MockRows) Scan(dest ...any) error {
	if r.scanError != nil {
		return r.scanError
	}

	data := r.rows[(*r.index)-1]
	content := dest[0]
	scr, ok := (content).(scanner)
	if !ok {
		panic("This shouldn't happen")
	}

	scr.SetValues(data)

	return nil
}
func (r MockRows) ScanStruct(dest any) error { return nil }
func (r MockRows) Totals(dest ...any) error  { return nil }
func (r MockRows) Columns() []string         { return []string{"name", "age"} }
func (r MockRows) Close() error              { return nil }
func (r MockRows) Err() error                { return nil }

type MockConn struct {
	mock.Mock
}

func (m *MockConn) Contributors() []string                                                { return []string{""} }
func (m *MockConn) ServerVersion() (*driver.ServerVersion, error)                         { return nil, nil }
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

			result, err := client.Select(context.TODO(), &query.Query{
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
			},
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
			name:  "test select empty rows with schema",
			query: "SELECT * FROM table",
			expected: &query.QueryResult{
				Columns: []string{"id", "name"},
				Rows:    [][]interface{}{},
			},
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

			result, err := client.SelectWithSchema(context.TODO(), &query.Query{
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

			err := client.RunQueryWithoutResult(context.TODO(), &query.Query{
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
			wantErr: "failed to run test query on Postgres connection: ping error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockConn := MockConn{}
			tt.setupMock(&mockConn)

			client := Client{connection: &mockConn}

			err := client.Ping(context.TODO())

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}
		})
	}
}

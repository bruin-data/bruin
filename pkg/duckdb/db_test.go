//nolint:ireturn
package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scanFailingRows struct {
	nextCalled bool
}

func (r *scanFailingRows) Close() error {
	return nil
}

func (r *scanFailingRows) Columns() ([]string, error) {
	return []string{"result"}, nil
}

func (r *scanFailingRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return []*sql.ColumnType{{}}, nil
}

func (r *scanFailingRows) Err() error {
	return nil
}

func (r *scanFailingRows) Next() bool {
	if r.nextCalled {
		return false
	}
	r.nextCalled = true
	return true
}

func (r *scanFailingRows) Scan(dest ...any) error {
	return errors.New("Not Implemented: not yet implemented populating from columns of type list<l: utf8, nullable>")
}

type castedStringRows struct {
	nextCalled bool
}

func (r *castedStringRows) Close() error {
	return nil
}

func (r *castedStringRows) Columns() ([]string, error) {
	return []string{"result"}, nil
}

func (r *castedStringRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return []*sql.ColumnType{{}}, nil
}

func (r *castedStringRows) Err() error {
	return nil
}

func (r *castedStringRows) Next() bool {
	if r.nextCalled {
		return false
	}
	r.nextCalled = true
	return true
}

func (r *castedStringRows) Scan(dest ...any) error {
	ptr, ok := dest[0].(*interface{})
	if !ok {
		return errors.New("unexpected scan destination type")
	}
	*ptr = "main"
	return nil
}

type listScanErrorThenCastConnection struct {
	queries []string
}

//nolint:ireturn
func (c *listScanErrorThenCastConnection) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	c.queries = append(c.queries, query)
	if len(c.queries) == 1 {
		return &scanFailingRows{}, nil
	}
	return &castedStringRows{}, nil
}

func (c *listScanErrorThenCastConnection) ExecContext(ctx context.Context, sql string, arguments ...any) (sql.Result, error) {
	return nil, errors.New("not implemented")
}

//nolint:ireturn
func (c *listScanErrorThenCastConnection) QueryRowContext(ctx context.Context, query string, args ...any) Row {
	return nil
}

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
			db := Client{connection: newSqlxWrapper(sqlxDB), config: Config{Path: "some/path.db"}}

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
			errorMessage: "query execution failed",
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
			db := Client{connection: newSqlxWrapper(sqlxDB), config: Config{Path: "some/path.db"}}

			got, err := db.SelectWithSchema(t.Context(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_SelectWithSchema_ListColumnScanErrorFallsBackToCastedQuery(t *testing.T) {
	t.Parallel()

	conn := &listScanErrorThenCastConnection{}
	db := Client{
		connection: conn,
		config:     Config{Path: "some/path.db"},
	}

	result, err := db.SelectWithSchema(t.Context(), &query.Query{Query: "SHOW;"})
	require.NoError(t, err)
	require.Equal(t, []string{"result"}, result.Columns)
	require.Equal(t, [][]interface{}{{"main"}}, result.Rows)
	require.Len(t, conn.queries, 2)
	require.Equal(t, "SHOW;", conn.queries[0])
	assert.Contains(t, conn.queries[1], `CAST("result" AS VARCHAR) AS "result"`)
}

func TestIsUnsupportedComplexTypeScanError_MatchesDriverMessage(t *testing.T) {
	t.Parallel()
	e := errors.New("Not Implemented: not yet implemented populating from columns of type list<l: utf8, nullable>")
	assert.True(t, isUnsupportedComplexTypeScanError(e))
}

func TestClient_SelectWithSchema_SHOW_AfterCreateTable(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "proof.db")
	c, err := NewClient(Config{Path: path})
	require.NoError(t, err)
	err = c.RunQueryWithoutResult(ctx, &query.Query{Query: "CREATE TABLE t1 (id INT);"})
	require.NoError(t, err)
	res, err := c.SelectWithSchema(ctx, &query.Query{Query: "SHOW;"})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
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
    dv.sql as view_definition
FROM
    information_schema.tables t
LEFT JOIN
    duckdb_views\(\) dv ON t.table_schema = dv.schema_name AND t.table_name = dv.view_name
WHERE
    t.table_type IN \('BASE TABLE', 'VIEW'\)
    AND t.table_schema NOT IN \('information_schema', 'pg_catalog'\)
ORDER BY t.table_schema, t.table_name;`).
					WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name", "table_type", "view_definition"}).
						AddRow("schema1", "table1", "BASE TABLE", nil).
						AddRow("schema1", "table2", "BASE TABLE", nil).
						AddRow("schema2", "table1", "BASE TABLE", nil))
			},
			want: &ansisql.DBDatabase{
				Name: "duckdb",
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
    dv.sql as view_definition
FROM
    information_schema.tables t
LEFT JOIN
    duckdb_views\(\) dv ON t.table_schema = dv.schema_name AND t.table_name = dv.view_name
WHERE
    t.table_type IN \('BASE TABLE', 'VIEW'\)
    AND t.table_schema NOT IN \('information_schema', 'pg_catalog'\)
ORDER BY t.table_schema, t.table_name;`).
					WillReturnError(errors.New("connection error"))
			},
			wantErr: "failed to query DuckDB information_schema: connection error",
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
			client := Client{connection: newSqlxWrapper(sqlxDB), config: Config{Path: "some/path.db"}}

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

// delayedConnection is a mock connection that sleeps for a configurable duration
// on ExecContext calls. Used to test parallel vs serial execution behavior.
type delayedConnection struct {
	delay time.Duration
}

//nolint:ireturn
func (d *delayedConnection) QueryContext(_ context.Context, _ string, _ ...any) (Rows, error) {
	time.Sleep(d.delay)
	return &emptyRows{}, nil
}

func (d *delayedConnection) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	time.Sleep(d.delay)
	return driver.RowsAffected(0), nil
}

//nolint:ireturn
func (d *delayedConnection) QueryRowContext(_ context.Context, _ string, _ ...any) Row {
	time.Sleep(d.delay)
	return &errorRow{err: sql.ErrNoRows}
}

// emptyRows implements Rows with no data.
type emptyRows struct{}

func (r *emptyRows) Close() error                            { return nil }
func (r *emptyRows) Columns() ([]string, error)              { return nil, nil }
func (r *emptyRows) ColumnTypes() ([]*sql.ColumnType, error) { return nil, nil }
func (r *emptyRows) Err() error                              { return nil }
func (r *emptyRows) Next() bool                              { return false }
func (r *emptyRows) Scan(_ ...any) error                     { return sql.ErrNoRows }

func TestClient_ReadOnly_AllowsParallelQueries(t *testing.T) {
	t.Parallel()

	const (
		queryDelay  = 150 * time.Millisecond
		concurrency = 3
	)

	// Use a unique path to avoid conflicts with other tests using the global lock map.
	path := "test_readonly_parallel_" + t.Name() + ".db"

	clients := make([]*Client, concurrency)
	for i := range concurrency {
		clients[i] = &Client{
			connection: &delayedConnection{delay: queryDelay},
			config:     Config{Path: path, ReadOnly: true},
			readOnly:   true,
		}
	}

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func() {
			defer wg.Done()
			err := clients[i].RunQueryWithoutResult(t.Context(), &query.Query{Query: "SELECT 1"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)

	// If queries ran in parallel, total time should be ~1x queryDelay.
	// If serialized, it would be ~3x queryDelay (450ms).
	// Use 2x as the threshold to leave margin for scheduling jitter.
	maxExpected := queryDelay * 2
	assert.Less(t, elapsed, maxExpected,
		"readonly queries should run in parallel: expected < %v, got %v", maxExpected, elapsed)
}

func TestClient_NonReadOnly_SerializesQueries(t *testing.T) {
	t.Parallel()

	const (
		queryDelay  = 100 * time.Millisecond
		concurrency = 3
	)

	path := "test_serial_nonreadonly_" + t.Name() + ".db"

	clients := make([]*Client, concurrency)
	for i := range concurrency {
		clients[i] = &Client{
			connection: &delayedConnection{delay: queryDelay},
			config:     Config{Path: path, ReadOnly: false},
			readOnly:   false,
		}
	}

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := range concurrency {
		go func() {
			defer wg.Done()
			err := clients[i].RunQueryWithoutResult(t.Context(), &query.Query{Query: "SELECT 1"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)

	// If queries were serialized by the lock, total time should be >= 3x queryDelay.
	// Allow some tolerance for timing.
	minExpected := queryDelay*time.Duration(concurrency) - 50*time.Millisecond
	assert.GreaterOrEqual(t, elapsed, minExpected,
		"non-readonly queries should be serialized: expected >= %v, got %v", minExpected, elapsed)
}

func TestRoundToScale(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    float64
		scale    int64
		expected float64
	}{
		{
			name:     "positive value with scale 2",
			value:    123.45,
			scale:    2,
			expected: 123.45,
		},
		{
			name:     "negative value with scale 2",
			value:    -123.45,
			scale:    2,
			expected: -123.45,
		},
		{
			name:     "positive value with scale 3",
			value:    1.235,
			scale:    3,
			expected: 1.235,
		},
		{
			name:     "negative value with scale 3",
			value:    -1.235,
			scale:    3,
			expected: -1.235,
		},
		{
			name:     "zero value",
			value:    0.0,
			scale:    2,
			expected: 0.0,
		},
		{
			name:     "positive value with scale 0",
			value:    12345.0,
			scale:    0,
			expected: 12345.0,
		},
		{
			name:     "negative value with scale 0",
			value:    -12345.0,
			scale:    0,
			expected: -12345.0,
		},
		{
			name:     "small positive value with high scale",
			value:    1.23456789,
			scale:    8,
			expected: 1.23456789,
		},
		{
			name:     "small negative value with high scale",
			value:    -1.23456789,
			scale:    8,
			expected: -1.23456789,
		},
		{
			name:     "value at rounding boundary positive",
			value:    1.25,
			scale:    2,
			expected: 1.25,
		},
		{
			name:     "value at rounding boundary negative",
			value:    -1.25,
			scale:    2,
			expected: -1.25,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := roundToScale(tt.value, tt.scale)

			assert.InDelta(t, tt.expected, result, 0.0000001, "expected %v but got %v", tt.expected, result)
		})
	}
}

func TestRoundToScale_SymmetricRounding(t *testing.T) {
	t.Parallel()

	// This test specifically verifies that negative numbers are rounded symmetrically
	// (away from zero), not toward zero as the bug previously caused.
	//
	// The original bug was:
	// floatVal = float64(int64(floatVal*multiplier+0.5)) / multiplier
	//
	// Example with scale 2 (multiplier 100):
	// Positive: 1.235 -> 123.5 + 0.5 = 124.0 -> int64(124.0) = 124 -> 1.24 (correct)
	// Negative: -1.235 -> -123.5 + 0.5 = -123.0 -> int64(-123.0) = -123 -> -1.23 (wrong!)
	//
	// The fix uses math.Round which handles both positive and negative correctly:
	// Positive: 1.235 -> 123.5 -> round(123.5) = 124 -> 1.24 (correct)
	// Negative: -1.235 -> -123.5 -> round(-123.5) = -124 -> -1.24 (correct)

	tests := []struct {
		name     string
		value    float64
		scale    int64
		expected float64
	}{
		{
			name:     "positive rounds up at 0.5",
			value:    1.235,
			scale:    2,
			expected: 1.24,
		},
		{
			name:     "negative rounds away from zero at 0.5 (symmetric)",
			value:    -1.235,
			scale:    2,
			expected: -1.24,
		},
		{
			name:     "positive rounds down below 0.5",
			value:    1.234,
			scale:    2,
			expected: 1.23,
		},
		{
			name:     "negative rounds toward zero below 0.5 (symmetric)",
			value:    -1.234,
			scale:    2,
			expected: -1.23,
		},
		{
			name:     "large positive value with rounding",
			value:    99999999.995,
			scale:    2,
			expected: 100000000.0,
		},
		{
			name:     "large negative value with rounding",
			value:    -99999999.995,
			scale:    2,
			expected: -100000000.0,
		},
		{
			name:     "positive value rounding with scale 1",
			value:    1.55,
			scale:    1,
			expected: 1.6,
		},
		{
			name:     "negative value rounding with scale 1",
			value:    -1.55,
			scale:    1,
			expected: -1.6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := roundToScale(tt.value, tt.scale)

			assert.InDelta(t, tt.expected, result, 0.0000001, "expected %v but got %v", tt.expected, result)
		})
	}
}

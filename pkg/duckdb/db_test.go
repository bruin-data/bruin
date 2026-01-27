package duck

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

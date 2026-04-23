package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQueryVars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rawVars  []string
		expected map[string]any
		wantErr  string
	}{
		{
			name:     "single string var",
			rawVars:  []string{"name=alice"},
			expected: map[string]any{"name": "alice"},
		},
		{
			name:     "date value stays as string",
			rawVars:  []string{"start_date=2026-01-23"},
			expected: map[string]any{"start_date": "2026-01-23"},
		},
		{
			name:     "numeric value stays as string",
			rawVars:  []string{"limit=100"},
			expected: map[string]any{"limit": "100"},
		},
		{
			name:     "value with equals sign",
			rawVars:  []string{"filter=a=b"},
			expected: map[string]any{"filter": "a=b"},
		},
		{
			name:     "multiple vars",
			rawVars:  []string{"a=1", "b=two", "c=2026-01-01"},
			expected: map[string]any{"a": "1", "b": "two", "c": "2026-01-01"},
		},
		{
			name:     "empty value is allowed",
			rawVars:  []string{"key="},
			expected: map[string]any{"key": ""},
		},
		{
			name:     "key with spaces is trimmed",
			rawVars:  []string{"  key  =value"},
			expected: map[string]any{"key": "value"},
		},
		{
			name:     "no vars returns empty map",
			expected: map[string]any{},
		},
		{
			name:     "later var overrides earlier with same key",
			rawVars:  []string{"x=first", "x=second"},
			expected: map[string]any{"x": "second"},
		},
		{
			name:     "value with spaces is trimmed",
			rawVars:  []string{"key = hello "},
			expected: map[string]any{"key": "hello"},
		},
		{
			name:    "empty key errors",
			rawVars: []string{"=value"},
			wantErr: `invalid variable "=value": key must not be empty`,
		},
		{
			name:    "missing equals sign errors",
			rawVars: []string{"invalid"},
			wantErr: `invalid variable "invalid": must be in key=value format`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseQueryVars(tt.rawVars)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseQueryVarsWithJinjaRendering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		template string
		rawVars  []string
		expected string
	}{
		{
			name:     "string var renders in template",
			template: "SELECT '{{ name }}' AS val",
			rawVars:  []string{"name=alice"},
			expected: "SELECT 'alice' AS val",
		},
		{
			name:     "date var renders correctly",
			template: "SELECT DATE('{{ start_date }}') AS d",
			rawVars:  []string{"start_date=2026-01-23"},
			expected: "SELECT DATE('2026-01-23') AS d",
		},
		{
			name:     "numeric string renders as text in template",
			template: "SELECT * FROM t LIMIT {{ limit }}",
			rawVars:  []string{"limit=10"},
			expected: "SELECT * FROM t LIMIT 10",
		},
		{
			name:     "multiple vars render together",
			template: "SELECT * FROM {{ table }} WHERE date = '{{ dt }}' LIMIT {{ n }}",
			rawVars:  []string{"table=users", "dt=2026-01-01", "n=100"},
			expected: "SELECT * FROM users WHERE date = '2026-01-01' LIMIT 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			vars, err := parseQueryVars(tt.rawVars)
			require.NoError(t, err)

			now := time.Now()
			renderer := jinja.NewRendererWithStartEndDates(&now, &now, &now, "test-pipeline", "test-run", nil)
			for k, v := range vars {
				renderer.SetContextValue(k, v)
			}

			result, err := renderer.Render(tt.template)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		connection   string
		query        string
		asset        string
		environment  string
		limit        int64
		expectError  bool
		errorMsg     string
		limitedQuery string // new field to test the query after limit is applied
	}{
		{
			name:         "valid direct query mode with limit",
			connection:   "my-conn",
			query:        "SELECT * FROM table",
			limit:        10,
			expectError:  false,
			limitedQuery: "SELECT * FROM (\nSELECT * FROM table\n) as t LIMIT 10",
		},
		{
			name:         "valid direct query mode with mssql and limit",
			connection:   "my-conn",
			query:        "SELECT * FROM users",
			limit:        5,
			expectError:  false,
			limitedQuery: "SELECT TOP 5 * FROM (\nSELECT * FROM users\n) as t",
		},
		{
			name:         "valid asset mode with limit",
			asset:        "path/to/asset.sql",
			limit:        20,
			expectError:  false,
			limitedQuery: "SELECT * FROM (\nSELECT * FROM asset_table\n) as t LIMIT 20",
		},
		{
			name:         "valid asset mode with environment and limit",
			asset:        "path/to/asset.sql",
			environment:  "prod",
			limit:        15,
			expectError:  false,
			limitedQuery: "SELECT * FROM (\nSELECT * FROM asset_table\n) as t LIMIT 15",
		},
		{
			name:         "direct query with complex query and limit",
			connection:   "my-conn",
			query:        "SELECT u.*, p.status FROM users u JOIN payments p ON u.id = p.user_id",
			limit:        25,
			expectError:  false,
			limitedQuery: "SELECT * FROM (\nSELECT u.*, p.status FROM users u JOIN payments p ON u.id = p.user_id\n) as t LIMIT 25",
		},
		{
			name:        "missing query in direct mode",
			connection:  "my-conn",
			expectError: true,
			errorMsg:    "direct query mode requires both --connection and --query flags",
		},
		{
			name:        "missing connection in direct mode",
			query:       "SELECT * FROM table",
			expectError: true,
			errorMsg:    "must use either:\n1. Direct query mode (--connection and --query), or\n2. Asset mode (--asset with optional --environment), or\n3. Auto-detect mode (--asset to detect the connection and --query to run arbitrary queries)",
		},
		{
			name:        "mixing direct query and asset modes",
			connection:  "my-conn",
			query:       "SELECT * FROM table",
			asset:       "path/to/asset.sql",
			expectError: true,
			errorMsg:    "direct query mode (--connection and --query) cannot be combined with asset mode (--asset and --environment)",
		},
		{
			name:        "no flags provided",
			expectError: true,
			errorMsg:    "must use either:\n1. Direct query mode (--connection and --query), or\n2. Asset mode (--asset with optional --environment), or\n3. Auto-detect mode (--asset to detect the connection and --query to run arbitrary queries)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// First validate the flags
			err := validateFlags(tt.connection, tt.query, tt.asset)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Equal(t, tt.errorMsg, err.Error())
				}
				return
			}

			require.NoError(t, err)

			// If validation passed and we have a query, test the limit functionality
			if tt.query != "" {
				var conn interface{}
				if strings.Contains(tt.name, "mssql") {
					conn = &MockMSSQLDB{}
				}
				limitedQuery := addLimitToQuery(tt.query, tt.limit, conn, nil, "")
				assert.Equal(t, tt.limitedQuery, limitedQuery)
			}
		})
	}
}

// MockMSSQLDB implements the Limiter interface like mssql.DB does.
type MockMSSQLDB struct{}

func (m *MockMSSQLDB) Limit(query string, limit int64) string {
	query = strings.TrimRight(query, "; \n\t")
	return fmt.Sprintf("SELECT TOP %d * FROM (\n%s\n) as t", limit, query)
}

func TestAddLimitToQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		limit    int64
		conn     interface{}
		expected string
	}{
		{
			name:     "basic query without limiter",
			query:    "SELECT * FROM table",
			limit:    10,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT * FROM table\n) as t LIMIT 10",
		},
		{
			name:     "query with semicolon and whitespace",
			query:    "SELECT * FROM table; \n\t",
			limit:    5,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT * FROM table\n) as t LIMIT 5",
		},
		{
			name:     "query with mssql connection",
			query:    "SELECT * FROM table",
			limit:    20,
			conn:     &MockMSSQLDB{},
			expected: "SELECT TOP 20 * FROM (\nSELECT * FROM table\n) as t",
		},
		{
			name:     "complex query with joins",
			query:    "SELECT a.*, b.name FROM table_a a JOIN table_b b ON a.id = b.id",
			limit:    15,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT a.*, b.name FROM table_a a JOIN table_b b ON a.id = b.id\n) as t LIMIT 15",
		},
		{
			name:     "query with multiple semicolons",
			query:    "SELECT * FROM table;;;;  \n\t",
			limit:    7,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT * FROM table\n) as t LIMIT 7",
		},
		{
			name:     "query with subquery",
			query:    "SELECT * FROM (SELECT id, name FROM users WHERE active = true) u",
			limit:    25,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT * FROM (SELECT id, name FROM users WHERE active = true) u\n) as t LIMIT 25",
		},
		{
			name:     "mssql with complex query",
			query:    "SELECT u.*, p.status FROM users u LEFT JOIN payments p ON u.id = p.user_id WHERE p.amount > 100",
			limit:    30,
			conn:     &MockMSSQLDB{},
			expected: "SELECT TOP 30 * FROM (\nSELECT u.*, p.status FROM users u LEFT JOIN payments p ON u.id = p.user_id WHERE p.amount > 100\n) as t",
		},
		{
			name:     "zero limit query",
			query:    "SELECT * FROM table",
			limit:    0,
			conn:     nil,
			expected: "SELECT * FROM (\nSELECT * FROM table\n) as t LIMIT 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := addLimitToQuery(tt.query, tt.limit, tt.conn, nil, "")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatBigRatAsDecimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "integer value",
			input:    "42",
			expected: "42",
		},
		{
			name:     "terminating decimal",
			input:    "32097247/500000",
			expected: "64.194494",
		},
		{
			name:     "negative terminating decimal",
			input:    "-111/100",
			expected: "-1.11",
		},
		{
			name:     "trailing zeros are trimmed",
			input:    "12500/1000",
			expected: "12.5",
		},
		{
			name:     "non-terminating decimal fallback",
			input:    "1/3",
			expected: "0.33333333333333333333333333333333333333",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rat, ok := new(big.Rat).SetString(tt.input)
			require.True(t, ok)

			assert.Equal(t, tt.expected, formatBigRatAsDecimal(rat))
		})
	}
}

func TestFormatQueryRowsForJSON(t *testing.T) {
	t.Parallel()

	decimalRat, ok := new(big.Rat).SetString("32097247/500000")
	require.True(t, ok)

	repeatingRat, ok := new(big.Rat).SetString("1/3")
	require.True(t, ok)

	input := [][]interface{}{
		{"abc", decimalRat, 100},
		{nil, repeatingRat, true},
	}

	formatted := formatQueryRowsForJSON(input)

	require.Len(t, formatted, 2)
	assert.Equal(t, json.Number("64.194494"), formatted[0][1])
	assert.Equal(t, json.Number("0.33333333333333333333333333333333333333"), formatted[1][1])
	assert.Nil(t, formatted[1][0])
	assert.Equal(t, 100, formatted[0][2])
	assert.Equal(t, true, formatted[1][2])

	// Original result rows should stay untouched.
	assert.Same(t, decimalRat, input[0][1])
	assert.Same(t, repeatingRat, input[1][1])
}

func TestFormatQueryRowsForJSON_Marshal(t *testing.T) {
	t.Parallel()

	rat, ok := new(big.Rat).SetString("32097247/500000")
	require.True(t, ok)

	var nilRat *big.Rat

	tests := []struct {
		name     string
		cell     interface{}
		expected string
	}{
		{
			name:     "big rat marshals as number",
			cell:     rat,
			expected: `[[64.194494]]`,
		},
		{
			name:     "nil big rat marshals as null",
			cell:     nilRat,
			expected: `[[null]]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows := formatQueryRowsForJSON([][]interface{}{{tt.cell}})

			payload, err := json.Marshal(rows)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(payload))
		})
	}
}

func TestWriteCSVFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		columns  []string
		rows     [][]interface{}
		expected [][]string
	}{
		{
			name:    "basic data",
			columns: []string{"id", "name"},
			rows: [][]interface{}{
				{1, "alice"},
				{2, "bob"},
			},
			expected: [][]string{
				{"id", "name"},
				{"1", "alice"},
				{"2", "bob"},
			},
		},
		{
			name:    "with nil values",
			columns: []string{"id", "value"},
			rows: [][]interface{}{
				{1, nil},
				{2, "test"},
			},
			expected: [][]string{
				{"id", "value"},
				{"1", ""},
				{"2", "test"},
			},
		},
		{
			name:     "empty rows",
			columns:  []string{"col1", "col2"},
			rows:     nil,
			expected: [][]string{{"col1", "col2"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.csv")

			err := writeCSVFile(tmpFile, tt.columns, tt.rows)
			require.NoError(t, err)

			file, err := os.Open(tmpFile)
			require.NoError(t, err)
			defer file.Close()

			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			require.NoError(t, err)

			assert.Equal(t, tt.expected, records)
		})
	}
}

func TestExportResultsToMultipleCSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numRows       int
		splitRows     int
		expectedFiles int
		rowsPerFile   []int
	}{
		{
			name:          "exact split",
			numRows:       10,
			splitRows:     5,
			expectedFiles: 2,
			rowsPerFile:   []int{5, 5},
		},
		{
			name:          "with remainder",
			numRows:       10,
			splitRows:     4,
			expectedFiles: 3,
			rowsPerFile:   []int{4, 4, 2},
		},
		{
			name:          "single file when rows less than split",
			numRows:       3,
			splitRows:     10,
			expectedFiles: 1,
			rowsPerFile:   []int{3},
		},
		{
			name:          "empty result creates single file",
			numRows:       0,
			splitRows:     10,
			expectedFiles: 1,
			rowsPerFile:   []int{0},
		},
		{
			name:          "one million rows example from issue",
			numRows:       1000000,
			splitRows:     400000,
			expectedFiles: 3,
			rowsPerFile:   []int{400000, 400000, 200000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpDir := t.TempDir()

			err := os.MkdirAll(filepath.Join(tmpDir, ".git"), 0o755)
			require.NoError(t, err)

			columns := []string{"id", "name", "value"}
			rows := make([][]interface{}, tt.numRows)
			for i := range tt.numRows {
				rows[i] = []interface{}{i, fmt.Sprintf("name_%d", i), i * 100}
			}

			result := &query.QueryResult{
				Columns: columns,
				Rows:    rows,
			}

			paths, err := exportResultsToMultipleCSV(result, tmpDir, tt.splitRows)
			require.NoError(t, err)
			assert.Len(t, paths, tt.expectedFiles)

			for i, path := range paths {
				assert.FileExists(t, path)

				file, err := os.Open(path)
				require.NoError(t, err)
				defer file.Close()

				reader := csv.NewReader(file)
				records, err := reader.ReadAll()
				require.NoError(t, err)

				expectedRowsWithHeader := tt.rowsPerFile[i] + 1
				assert.Len(t, records, expectedRowsWithHeader, "file %d should have %d rows (including header)", i+1, expectedRowsWithHeader)

				assert.Equal(t, columns, records[0], "header should match columns")

				assert.Contains(t, filepath.Base(path), fmt.Sprintf("_part%d.csv", i+1))
			}
		})
	}
}

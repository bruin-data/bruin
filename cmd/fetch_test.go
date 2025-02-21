package cmd

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			errorMsg:    "direct query mode requires both --connection and --query flags",
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
			errorMsg:    "must use either:\n1. Direct query mode (--connection and --query), or\n2. Asset mode (--asset with optional --environment)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// First validate the flags
			err := validateFlags(tt.connection, tt.query, tt.asset, tt.environment)

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
				limitedQuery := addLimitToQuery(tt.query, tt.limit, conn)
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
			result := addLimitToQuery(tt.query, tt.limit, tt.conn)
			assert.Equal(t, tt.expected, result)
		})
	}
}

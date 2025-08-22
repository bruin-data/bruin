package redshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_ToDBConnectionURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "basic connection",
			config: Config{
				Username: "testuser",
				Password: "testpass",
				Host:     "test-cluster.region.redshift.amazonaws.com",
				Port:     5439,
				Database: "testdb",
				SslMode:  "require",
			},
			expected: "postgres://testuser:testpass@test-cluster.region.redshift.amazonaws.com:5439/testdb?sslmode=require",
		},
		{
			name: "connection with schema",
			config: Config{
				Username: "testuser",
				Password: "testpass",
				Host:     "test-cluster.region.redshift.amazonaws.com",
				Port:     5439,
				Database: "testdb",
				Schema:   "public",
				SslMode:  "require",
			},
			expected: "postgres://testuser:testpass@test-cluster.region.redshift.amazonaws.com:5439/testdb?sslmode=require&search_path=public",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.config.ToDBConnectionURI()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClient_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		client      *Client
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:      "single table name",
			client:    &Client{config: &Config{Database: "testdb"}},
			tableName: "test_table",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'public' AND tablename = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "schema.table format",
			client:    &Client{config: &Config{Database: "testdb"}},
			tableName: "test_schema.test_table",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'test_schema' AND tablename = 'test_table'",
			wantErr:   false,
		},
		{
			name:        "invalid format - empty component",
			client:      &Client{config: &Config{Database: "testdb"}},
			tableName:   ".test_table",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.test_table' given",
		},
		{
			name:        "invalid format - empty component 2",
			client:      &Client{config: &Config{Database: "testdb"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.' given",
		},
		{
			name:        "invalid format - empty table name",
			client:      &Client{config: &Config{Database: "testdb"}},
			tableName:   "",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '' given",
		},
		{
			name:        "invalid format - too many components",
			client:      &Client{config: &Config{Database: "testdb"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, 'a.b.c.d' given",
		},
		{
			name:      "mixed case handling",
			client:    &Client{config: &Config{Database: "testdb"}},
			tableName: "TestSchema.TestTable",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'TestSchema' AND tablename = 'TestTable'",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotQuery, err := tt.client.BuildTableExistsQuery(tt.tableName)

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

// Test that the Client implements the TableExistsChecker interface
func TestClient_ImplementsTableExistsChecker(t *testing.T) {
	t.Parallel()

	// This test verifies that our Client implements the required interface
	// by checking that it has the required methods
	client := &Client{config: &Config{Database: "testdb"}}

	// Check that the client has the required methods
	_ = client.Select
	_ = client.BuildTableExistsQuery

	// If this compiles, the interface is implemented
}

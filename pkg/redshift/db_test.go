package redshift

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableSensorClient_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	// Create a mock table sensor client
	client := &TableSensorClient{}

	tests := []struct {
		name        string
		tableName   string
		expected    string
		expectError bool
	}{
		{
			name:        "simple table name",
			tableName:   "users",
			expected:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'users'",
			expectError: false,
		},
		{
			name:        "schema.table format",
			tableName:   "data.users",
			expected:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'data' AND table_name = 'users'",
			expectError: false,
		},
		{
			name:        "custom schema.table format",
			tableName:   "analytics.events",
			expected:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'events'",
			expectError: false,
		},
		{
			name:        "empty table name",
			tableName:   "",
			expected:    "table name must be in format schema.table or table, '' given",
			expectError: true,
		},
		{
			name:        "too many components",
			tableName:   "schema.table.extra",
			expected:    "table name must be in format schema.table or table, 'schema.table.extra' given",
			expectError: true,
		},
		{
			name:        "table name with underscores",
			tableName:   "analytics.user_events",
			expected:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = 'user_events'",
			expectError: false,
		},
		{
			name:        "table name with numbers",
			tableName:   "public.table_2024",
			expected:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'table_2024'",
			expectError: false,
		},
		{
			name:        "empty component in table name",
			tableName:   "schema.",
			expected:    "table name must be in format schema.table or table, 'schema.' given",
			expectError: true,
		},
		{
			name:        "empty component at start",
			tableName:   ".table",
			expected:    "table name must be in format schema.table or table, '.table' given",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := client.BuildTableExistsQuery(tt.tableName)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				assert.Equal(t, tt.expected, err.Error(), "Error message should match expected")
				return
			}

			require.NoError(t, err, "Unexpected error")
			assert.Equal(t, tt.expected, result, "Query should match expected output")
		})
	}
}

package redshift

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/stretchr/testify/assert"
)

func TestClient_BuildTableExistsQuery(t *testing.T) {
	client := &TableSensorClient{}

	tests := []struct {
		name      string
		tableName string
		wantQuery string
		wantErr   bool
	}{
		{
			name:      "simple table name",
			tableName: "users",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'public' AND tablename = 'users'",
			wantErr:   false,
		},
		{
			name:      "schema.table format",
			tableName: "public.users",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'public' AND tablename = 'users'",
			wantErr:   false,
		},
		{
			name:      "custom schema",
			tableName: "analytics.events",
			wantQuery: "SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = 'analytics' AND tablename = 'events'",
			wantErr:   false,
		},
		{
			name:      "empty table name",
			tableName: "",
			wantErr:   true,
		},
		{
			name:      "empty schema",
			tableName: ".users",
			wantErr:   true,
		},
		{
			name:      "empty table",
			tableName: "public.",
			wantErr:   true,
		},
		{
			name:      "too many components",
			tableName: "db.schema.table",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := client.BuildTableExistsQuery(tt.tableName)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantQuery, query)
			}
		})
	}
}

func TestClient_ImplementsTableExistsChecker(t *testing.T) {
	// This test ensures that TableSensorClient implements the TableExistsChecker interface
	var _ ansisql.TableExistsChecker = (*TableSensorClient)(nil)
}

func TestConfig_ToPostgresConfig(t *testing.T) {
	config := Config{
		Username: "testuser",
		Password: "testpass",
		Host:     "test-cluster.redshift.amazonaws.com",
		Port:     5439,
		Database: "testdb",
		Schema:   "public",
		SslMode:  "require",
	}

	postgresConfig := config.ToPostgresConfig()

	assert.Equal(t, config.Username, postgresConfig.Username)
	assert.Equal(t, config.Password, postgresConfig.Password)
	assert.Equal(t, config.Host, postgresConfig.Host)
	assert.Equal(t, config.Port, postgresConfig.Port)
	assert.Equal(t, config.Database, postgresConfig.Database)
	assert.Equal(t, config.Schema, postgresConfig.Schema)
	assert.Equal(t, config.SslMode, postgresConfig.SslMode)
}

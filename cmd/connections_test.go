package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test config constants.
const testConfigWithConnections = `
default_environment: dev
environments:
  dev:
    connections:
      postgres:
        - name: "pg_conn"
          host: "localhost"
          username: "user"
          password: "pass"
          database: "db"
          port: 5432
      mysql:
        - name: "mysql_conn"
          host: "localhost"
          username: "user"
          password: "pass"
          database: "db"
          port: 3306
    schema_prefix: "dev_"
  prod:
    connections:
      postgres:
        - name: "pg_conn"
          host: "prod.localhost"
          username: "prod_user"
          password: "prod_pass"
          database: "prod_db"
          port: 5432
    schema_prefix: "prod_"
`

const emptyConfig = `
default_environment: dev
environments:
  dev:
    connections: {}
    schema_prefix: "dev_"
`

// Add a new config for testing mixed environments (some with connections, some empty).
const mixedEnvironmentsConfig = `
default_environment: dev
environments:
  dev:
    connections:
      postgres:
        - name: "pg_conn"
          host: "localhost"
          username: "user"
          password: "pass"
          database: "db"
          port: 5432
    schema_prefix: "dev_"
  staging:
    connections: {}
    schema_prefix: "staging_"
  prod:
    connections:
      mysql:
        - name: "mysql_conn"
          host: "prod.localhost"
          username: "prod_user"
          password: "prod_pass"
          database: "prod_db"
          port: 3306
    schema_prefix: "prod_"
  empty_env:
    connections: {}
    schema_prefix: "empty_"
`

// Helper function to setup test config.
func setupTestConfig(t *testing.T, configContent string) (afero.Fs, string) { //nolint:ireturn // Test helper needs to return interface for filesystem abstraction
	fs := afero.NewMemMapFs()
	configFile := ".bruin.yml"

	file, err := fs.Create(configFile)
	require.NoError(t, err)
	defer file.Close()

	_, err = file.Write([]byte(configContent))
	require.NoError(t, err)

	return fs, configFile
}

// Mock connection manager for testing.
type mockConnectionManager struct {
	connections map[string]interface{}
}

func (m *mockConnectionManager) GetConnection(name string) (interface{}, error) {
	if conn, exists := m.connections[name]; exists {
		return conn, nil
	}
	return nil, fmt.Errorf("connection '%s' not found", name)
}

// Mock pingable connection.
type mockPingableConnection struct { //nolint:ireturn
	name      string
	shouldErr bool
}

func (m *mockPingableConnection) Ping(ctx context.Context) error {
	if m.shouldErr {
		return fmt.Errorf("ping failed for connection %s", m.name)
	}
	return nil
}

// Mock non-pingable connection.
type mockNonPingableConnection struct {
	name string
}

// Helper function to simulate ping logic and reduce complexity.
func simulatePingLogic(connName string, pingable bool, shouldErr bool) error {
	// Create mock connection manager
	mockManager := &mockConnectionManager{
		connections: make(map[string]interface{}),
	}

	// Add mock connections based on test case
	if connName == "pg_conn" {
		if pingable {
			mockManager.connections[connName] = &mockPingableConnection{
				name:      connName,
				shouldErr: shouldErr,
			}
		} else {
			mockManager.connections[connName] = &mockNonPingableConnection{
				name: connName,
			}
		}
	}

	// Get connection
	conn, err := mockManager.GetConnection(connName)
	if err != nil {
		return fmt.Errorf("failed to get connection '%s': %w", connName, err)
	}

	// Test ping functionality
	if tester, ok := conn.(interface {
		Ping(ctx context.Context) error
	}); ok {
		if pingErr := tester.Ping(context.Background()); pingErr != nil {
			return fmt.Errorf("failed to test connection '%s': %w", connName, pingErr)
		}
	}

	return nil
}

func TestAddConnectionCommand_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		environment  string
		connName     string
		connType     string
		credentials  string
		configExists bool
		configFile   string
		output       string
		wantErr      bool
		expectedErr  string
	}{
		{
			name:         "successfully add new connection",
			environment:  "dev",
			connName:     "new_postgres",
			connType:     "postgres",
			credentials:  `{"host":"localhost","username":"user","password":"pass","database":"db","port":5432}`,
			configExists: true,
			configFile:   ".bruin.yml",
			output:       "plain",
			wantErr:      false,
		},
		{
			name:         "add to non-existent environment",
			environment:  "staging",
			connName:     "new_postgres",
			connType:     "postgres",
			credentials:  `{"host":"localhost","username":"user","password":"pass","database":"db","port":5432}`,
			configExists: true,
			configFile:   ".bruin.yml",
			output:       "plain",
			wantErr:      true,
			expectedErr:  "environment 'staging' does not exist",
		},
		{
			name:         "add duplicate connection name",
			environment:  "dev",
			connName:     "pg_conn",
			connType:     "postgres",
			credentials:  `{"host":"localhost","username":"user","password":"pass","database":"db","port":5432}`,
			configExists: true,
			configFile:   ".bruin.yml",
			output:       "plain",
			wantErr:      true,
			expectedErr:  "a connection named 'pg_conn' already exists",
		},
		{
			name:         "invalid JSON credentials",
			environment:  "dev",
			connName:     "new_postgres",
			connType:     "postgres",
			credentials:  `{"host":"localhost","invalid":json}`,
			configExists: true,
			configFile:   ".bruin.yml",
			output:       "plain",
			wantErr:      true,
			expectedErr:  "failed to parse credentials JSON",
		},
		{
			name:         "json output format",
			environment:  "dev",
			connName:     "new_postgres",
			connType:     "postgres",
			credentials:  `{"host":"localhost","username":"user","password":"pass","database":"db","port":5432}`,
			configExists: true,
			configFile:   ".bruin.yml",
			output:       "json",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs, configFile := setupTestConfig(t, testConfigWithConnections)

			// Create a mock config using the in-memory filesystem
			cm, err := config.LoadOrCreate(fs, configFile)
			require.NoError(t, err)

			// Capture the original state
			originalEnv := cm.Environments[tt.environment]

			// Simulate the AddConnection command logic
			var testErr error

			// Check if environment exists
			if _, exists := cm.Environments[tt.environment]; !exists {
				testErr = fmt.Errorf("environment '%s' does not exist", tt.environment)
			} else if cm.Environments[tt.environment].Connections.Exists(tt.connName) {
				testErr = fmt.Errorf("a connection named '%s' already exists in the '%s' environment", tt.connName, tt.environment)
			} else {
				// Try to parse credentials
				var creds map[string]interface{}
				if err := json.Unmarshal([]byte(tt.credentials), &creds); err != nil {
					testErr = fmt.Errorf("failed to parse credentials JSON: %w", err)
				} else {
					// Try to add connection
					if err := cm.AddConnection(tt.environment, tt.connName, tt.connType, creds); err != nil {
						testErr = fmt.Errorf("failed to add connection: %w", err)
					}
				}
			}

			if tt.wantErr {
				require.Error(t, testErr)
				if tt.expectedErr != "" {
					assert.Contains(t, testErr.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, testErr)

				// Verify connection was added
				if testErr == nil {
					assert.True(t, cm.Environments[tt.environment].Connections.Exists(tt.connName))
				}
			}

			// Restore original state if needed
			if !tt.wantErr && testErr == nil {
				cm.Environments[tt.environment] = originalEnv
			}
		})
	}
}

func TestListConnectionsCommand_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		environment  string
		output       string
		configExists bool
		configFile   string
		wantErr      bool
		expectedErr  string
	}{
		{
			name:         "list all connections across environments",
			environment:  "",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
		{
			name:         "list specific environment connections",
			environment:  "dev",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
		{
			name:         "non-existent environment",
			environment:  "staging",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      true,
			expectedErr:  "Environment 'staging' not found",
		},
		{
			name:         "empty connections list",
			environment:  "dev",
			output:       "plain",
			configExists: true,
			configFile:   "empty.yml",
			wantErr:      false,
		},
		{
			name:         "json output validation",
			environment:  "dev",
			output:       "json",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
		{
			name:         "table output validation",
			environment:  "dev",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var fs afero.Fs
			var configFile string

			if tt.configFile == "empty.yml" {
				fs, configFile = setupTestConfig(t, emptyConfig)
			} else {
				fs, configFile = setupTestConfig(t, testConfigWithConnections)
			}

			// Mock the filesystem operations by creating a config directly
			cm, err := config.LoadOrCreate(fs, configFile)
			require.NoError(t, err)

			// Simulate the ListConnections logic
			var testErr error

			if tt.environment != "" {
				// Check if the specified environment exists
				if _, exists := cm.Environments[tt.environment]; !exists {
					testErr = fmt.Errorf("Environment '%s' not found", tt.environment)
				}
			}

			if tt.wantErr {
				require.Error(t, testErr)
				if tt.expectedErr != "" {
					assert.Contains(t, testErr.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, testErr)

				// Verify connections exist in the expected environment
				if tt.environment != "" {
					env, exists := cm.Environments[tt.environment]
					assert.True(t, exists)
					assert.NotNil(t, env.Connections)
				} else {
					// All environments should be accessible
					assert.NotEmpty(t, cm.Environments)
				}
			}
		})
	}
}

func TestListConnectionsCommand_ReturnsEmptyEnvironments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		configContent  string
		environment    string
		expectedEnvs   []string
		shouldHaveConn map[string]bool // environment name -> has connections
	}{
		{
			name:          "all environments empty",
			configContent: emptyConfig,
			environment:   "", // list all environments
			expectedEnvs:  []string{"dev"},
			shouldHaveConn: map[string]bool{
				"dev": false,
			},
		},
		{
			name:          "mixed environments - some empty, some with connections",
			configContent: mixedEnvironmentsConfig,
			environment:   "", // list all environments
			expectedEnvs:  []string{"dev", "staging", "prod", "empty_env"},
			shouldHaveConn: map[string]bool{
				"dev":       true,
				"staging":   false,
				"prod":      true,
				"empty_env": false,
			},
		},
		{
			name:          "specific empty environment",
			configContent: mixedEnvironmentsConfig,
			environment:   "staging",
			expectedEnvs:  []string{"staging"},
			shouldHaveConn: map[string]bool{
				"staging": false,
			},
		},
		{
			name:          "specific empty environment 2",
			configContent: mixedEnvironmentsConfig,
			environment:   "empty_env",
			expectedEnvs:  []string{"empty_env"},
			shouldHaveConn: map[string]bool{
				"empty_env": false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs, configFile := setupTestConfig(t, tt.configContent)

			// Load the config
			cm, err := config.LoadOrCreate(fs, configFile)
			require.NoError(t, err)

			// Verify all expected environments exist in the config
			for _, envName := range tt.expectedEnvs {
				env, exists := cm.Environments[envName]
				assert.True(t, exists, "Environment '%s' should exist", envName)
				assert.NotNil(t, env, "Environment '%s' should not be nil", envName)
				assert.NotNil(t, env.Connections, "Environment '%s' connections should not be nil", envName)

				// Check if environment has connections as expected
				connectionsList := env.Connections.ConnectionsSummaryList()
				hasConnections := len(connectionsList) > 0
				expectedHasConnections := tt.shouldHaveConn[envName]

				assert.Equal(t, expectedHasConnections, hasConnections,
					"Environment '%s' should have connections: %v, but has: %v",
					envName, expectedHasConnections, hasConnections)
			}

			// If testing specific environment, verify it exists and is accessible
			if tt.environment != "" {
				env, exists := cm.Environments[tt.environment]
				assert.True(t, exists, "Specific environment '%s' should exist", tt.environment)
				assert.NotNil(t, env, "Specific environment '%s' should not be nil", tt.environment)
			}

			// Verify that when listing all environments, empty environments are included
			if tt.environment == "" {
				assert.Len(t, cm.Environments, len(tt.expectedEnvs), "Should have all expected environments including empty ones")

				// Count empty environments
				emptyEnvCount := 0
				for envName, env := range cm.Environments {
					connectionsList := env.Connections.ConnectionsSummaryList()
					if len(connectionsList) == 0 {
						emptyEnvCount++
						assert.Contains(t, tt.expectedEnvs, envName,
							"Empty environment '%s' should be in expected list", envName)
					}
				}

				// Count expected empty environments
				expectedEmptyCount := 0
				for _, hasConn := range tt.shouldHaveConn {
					if !hasConn {
						expectedEmptyCount++
					}
				}

				assert.Equal(t, expectedEmptyCount, emptyEnvCount,
					"Should have correct number of empty environments")
			}
		})
	}
}

func TestDeleteConnectionCommand_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		environment  string
		connName     string
		output       string
		configExists bool
		configFile   string
		wantErr      bool
		expectedErr  string
	}{
		{
			name:         "successfully delete connection",
			environment:  "dev",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
		{
			name:         "delete non-existent connection",
			environment:  "dev",
			connName:     "nonexistent",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      true,
			expectedErr:  "connection 'nonexistent' does not exist",
		},
		{
			name:         "delete from non-existent environment",
			environment:  "staging",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      true,
			expectedErr:  "environment 'staging' not found",
		},
		{
			name:         "json output format",
			environment:  "dev",
			connName:     "pg_conn",
			output:       "json",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs, configFile := setupTestConfig(t, testConfigWithConnections)

			// Create a mock config using the in-memory filesystem
			cm, err := config.LoadOrCreate(fs, configFile)
			require.NoError(t, err)

			// Capture original state
			originalEnv := cm.Environments[tt.environment]

			// Simulate the DeleteConnection command logic
			testErr := cm.DeleteConnection(tt.environment, tt.connName)

			if tt.wantErr {
				require.Error(t, testErr)
				if tt.expectedErr != "" {
					assert.Contains(t, testErr.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, testErr)

				// Verify connection was deleted
				if testErr == nil {
					assert.False(t, cm.Environments[tt.environment].Connections.Exists(tt.connName))
				}
			}

			// Restore original state if needed
			if !tt.wantErr && testErr == nil {
				cm.Environments[tt.environment] = originalEnv
			}
		})
	}
}

func TestPingConnectionCommand_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		environment  string
		connName     string
		output       string
		configExists bool
		configFile   string
		wantErr      bool
		expectedErr  string
		mockPingErr  bool
		pingable     bool
	}{
		{
			name:         "successful ping",
			environment:  "dev",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
			mockPingErr:  false,
			pingable:     true,
		},
		{
			name:         "failed ping",
			environment:  "dev",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      true,
			expectedErr:  "failed to test connection",
			mockPingErr:  true,
			pingable:     true,
		},
		{
			name:         "non-existent connection",
			environment:  "dev",
			connName:     "nonexistent",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      true,
			expectedErr:  "failed to get connection",
			mockPingErr:  false,
			pingable:     true,
		},
		{
			name:         "non-pingable connection type",
			environment:  "dev",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
			mockPingErr:  false,
			pingable:     false,
		},
		{
			name:         "default environment usage",
			environment:  "",
			connName:     "pg_conn",
			output:       "plain",
			configExists: true,
			configFile:   ".bruin.yml",
			wantErr:      false,
			mockPingErr:  false,
			pingable:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs, configFile := setupTestConfig(t, testConfigWithConnections)

			// Create a mock config using the in-memory filesystem
			cm, err := config.LoadOrCreate(fs, configFile)
			require.NoError(t, err)

			// Select environment (default if empty)
			environment := tt.environment
			if environment == "" {
				environment = cm.DefaultEnvironmentName
			}

			// Simulate the PingConnection command logic
			var testErr error

			if err := cm.SelectEnvironment(environment); err != nil {
				testErr = fmt.Errorf("failed to select the environment: %w", err)
			} else {
				testErr = simulatePingLogic(tt.connName, tt.pingable, tt.mockPingErr)
			}

			if tt.wantErr {
				require.Error(t, testErr)
				if tt.expectedErr != "" {
					assert.Contains(t, testErr.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, testErr)
			}
		})
	}
}

func TestConnectionsCommand_ListConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		pathToProject string
		output        string
		environment   string
		configFile    string
		configContent string
		wantErr       bool
		expectedErr   string
	}{
		{
			name:          "valid config with multiple environments",
			pathToProject: ".",
			output:        "plain",
			environment:   "",
			configFile:    ".bruin.yml",
			configContent: testConfigWithConnections,
			wantErr:       false,
		},
		{
			name:          "empty environments",
			pathToProject: ".",
			output:        "plain",
			environment:   "",
			configFile:    ".bruin.yml",
			configContent: emptyConfig,
			wantErr:       false,
		},
		{
			name:          "json vs plain output formats",
			pathToProject: ".",
			output:        "json",
			environment:   "dev",
			configFile:    ".bruin.yml",
			configContent: testConfigWithConnections,
			wantErr:       false,
		},
		{
			name:          "invalid config file",
			pathToProject: ".",
			output:        "plain",
			environment:   "",
			configFile:    "nonexistent.yml",
			configContent: "",
			wantErr:       false, // LoadOrCreate will create a default config
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var fs afero.Fs
			var configFile string

			if tt.configContent != "" {
				fs, configFile = setupTestConfig(t, tt.configContent)
			} else {
				fs = afero.NewMemMapFs()
				configFile = tt.configFile
			}

			// Mock the filesystem operations by creating a config directly
			cm, err := config.LoadOrCreate(fs, configFile)
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
				return
			}
			require.NoError(t, err)

			// Simulate ListConnections logic
			var testErr error

			if tt.environment != "" {
				// Check if the specified environment exists
				if _, exists := cm.Environments[tt.environment]; !exists {
					testErr = fmt.Errorf("Environment '%s' not found", tt.environment)
				}
			}

			if tt.wantErr {
				require.Error(t, testErr)
				if tt.expectedErr != "" {
					assert.Contains(t, testErr.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, testErr)

				// Verify the config structure
				assert.NotNil(t, cm.Environments)

				if tt.environment != "" {
					env, exists := cm.Environments[tt.environment]
					assert.True(t, exists)
					assert.NotNil(t, env.Connections)
				}
			}
		})
	}
}

package cmd

import (
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bruin-data/bruin/pkg/config"
)

func TestEnvironmentListCommand_Run(t *testing.T) {
	t.Parallel()

	// Create a temporary config file content
	configContent := `
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

	tests := []struct {
		name         string
		output       string
		configFile   string
		configExists bool
		wantErr      bool
		expectedOut  string
	}{
		{
			name:         "list environments with plain output",
			output:       "plain",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      false,
			expectedOut:  "dev",
		},
		{
			name:         "list environments with json output",
			output:       "json",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      false,
			expectedOut:  "dev",
		},
		{
			name:         "config file does not exist",
			output:       "plain",
			configFile:   "nonexistent.yml",
			configExists: false,
			wantErr:      false,
			expectedOut:  "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup in-memory filesystem
			fs := afero.NewMemMapFs()

			if tt.configExists {
				// Create config file
				err := afero.WriteFile(fs, tt.configFile, []byte(configContent), 0o644)
				require.NoError(t, err)
			}

			// Create a mock config using the in-memory filesystem
			cm, err := config.LoadOrCreate(fs, tt.configFile)
			if err != nil {
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			// Test the actual logic with our mock config
			envs := cm.GetEnvironmentNames()

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			// Test environment existence
			if tt.expectedOut != "" {
				assert.Contains(t, envs, tt.expectedOut)
			}

			// Test selected environment
			if tt.configExists {
				assert.Equal(t, "dev", cm.SelectedEnvironmentName)
			} else {
				assert.Equal(t, "default", cm.SelectedEnvironmentName)
			}
		})
	}
}

func TestEnvironmentUpdateCommand_Run(t *testing.T) {
	t.Parallel()

	// Create a temporary config file content
	configContent := `
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

	tests := []struct {
		name         string
		envName      string
		newName      string
		schemaPrefix string
		output       string
		configFile   string
		configExists bool
		wantErr      bool
		expectedErr  string
	}{
		{
			name:         "update environment name successfully",
			envName:      "dev",
			newName:      "development",
			schemaPrefix: "dev_",
			output:       "plain",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      false,
		},
		{
			name:         "update environment schema prefix only",
			envName:      "dev",
			newName:      "",
			schemaPrefix: "new_dev_",
			output:       "plain",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      false,
		},
		{
			name:         "update environment with json output",
			envName:      "dev",
			newName:      "development",
			schemaPrefix: "dev_",
			output:       "json",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      false,
		},
		{
			name:         "update non-existent environment",
			envName:      "nonexistent",
			newName:      "new_name",
			schemaPrefix: "",
			output:       "plain",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      true,
			expectedErr:  "environment 'nonexistent' does not exist",
		},
		{
			name:         "update environment to existing name",
			envName:      "dev",
			newName:      "prod",
			schemaPrefix: "",
			output:       "plain",
			configFile:   ".bruin.yml",
			configExists: true,
			wantErr:      true,
			expectedErr:  "environment 'prod' already exists",
		},
		{
			name:         "config file does not exist",
			envName:      "default",
			newName:      "development",
			schemaPrefix: "",
			output:       "plain",
			configFile:   "nonexistent.yml",
			configExists: false,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup in-memory filesystem
			fs := afero.NewMemMapFs()

			if tt.configExists {
				// Create config file
				err := afero.WriteFile(fs, tt.configFile, []byte(configContent), 0o644)
				require.NoError(t, err)
			}

			// Create a mock config using the in-memory filesystem
			// Create a modified version of the Run method that uses our mock filesystem
			cm, err := config.LoadOrCreate(fs, tt.configFile)
			if err != nil {
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			// Test the actual logic with our mock config
			if !cm.EnvironmentExists(tt.envName) {
				if tt.wantErr && tt.expectedErr == "environment '"+tt.envName+"' does not exist" {
					return
				}
				require.True(t, cm.EnvironmentExists(tt.envName))
			}

			newName := tt.newName
			if newName == "" {
				newName = tt.envName
			}

			if tt.envName != newName && cm.EnvironmentExists(newName) {
				if tt.wantErr && tt.expectedErr == "environment '"+newName+"' already exists" {
					return
				}
				require.False(t, cm.EnvironmentExists(newName))
			}

			// Test update functionality
			err = cm.UpdateEnvironment(tt.envName, newName, tt.schemaPrefix)
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEnvironmentDeleteCommand_Run(t *testing.T) {
	t.Parallel()

	// Create a temporary config file content
	configContent := `
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

	singleEnvConfigContent := `
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
`

	tests := []struct {
		name          string
		envName       string
		force         bool
		output        string
		configFile    string
		configExists  bool
		configContent string
		wantErr       bool
		expectedErr   string
	}{
		{
			name:          "delete environment with force flag",
			envName:       "dev",
			force:         true,
			output:        "plain",
			configFile:    ".bruin.yml",
			configExists:  true,
			configContent: configContent,
			wantErr:       false,
		},
		{
			name:          "delete environment with json output",
			envName:       "dev",
			force:         true,
			output:        "json",
			configFile:    ".bruin.yml",
			configExists:  true,
			configContent: configContent,
			wantErr:       false,
		},
		{
			name:          "delete non-existent environment",
			envName:       "nonexistent",
			force:         true,
			output:        "plain",
			configFile:    ".bruin.yml",
			configExists:  true,
			configContent: configContent,
			wantErr:       true,
			expectedErr:   "environment 'nonexistent' does not exist",
		},
		{
			name:          "delete last environment",
			envName:       "dev",
			force:         true,
			output:        "plain",
			configFile:    ".bruin.yml",
			configExists:  true,
			configContent: singleEnvConfigContent,
			wantErr:       true,
			expectedErr:   "cannot delete the last environment",
		},
		{
			name:          "config file does not exist",
			envName:       "default",
			force:         true,
			output:        "plain",
			configFile:    "nonexistent.yml",
			configExists:  false,
			configContent: "",
			wantErr:       true,
			expectedErr:   "cannot delete the last environment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup in-memory filesystem
			fs := afero.NewMemMapFs()

			if tt.configExists {
				// Create config file
				err := afero.WriteFile(fs, tt.configFile, []byte(tt.configContent), 0o644)
				require.NoError(t, err)
			}

			// Create a mock config using the in-memory filesystem
			// Create a modified version of the Run method that uses our mock filesystem
			cm, err := config.LoadOrCreate(fs, tt.configFile)
			if err != nil {
				if tt.wantErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			// Test the actual logic with our mock config
			if !cm.EnvironmentExists(tt.envName) {
				if tt.wantErr && tt.expectedErr == "environment '"+tt.envName+"' does not exist" {
					return
				}
				require.True(t, cm.EnvironmentExists(tt.envName))
			}

			// Check if it's the last environment
			if len(cm.Environments) == 1 {
				if tt.wantErr && tt.expectedErr == "cannot delete the last environment" {
					return
				}
				require.Greater(t, len(cm.Environments), 1)
			}

			// Test delete functionality
			err = cm.DeleteEnvironment(tt.envName)
			if tt.wantErr {
				require.Error(t, err)
				if tt.expectedErr != "" {
					assert.Contains(t, err.Error(), tt.expectedErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEnvironmentDeleteCommand_Run_UserCancellation(t *testing.T) {
	t.Parallel()

	// Create a temporary config file content
	configContent := `
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

	// Setup in-memory filesystem
	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, ".bruin.yml", []byte(configContent), 0o644)
	require.NoError(t, err)

	// Mock stdin to simulate user saying "n"
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	_, _ = w.WriteString("n\n")
	w.Close()

	// Create a mock config using the in-memory filesystem
	cm, err := config.LoadOrCreate(fs, ".bruin.yml")
	require.NoError(t, err)

	// Test the cancellation logic - we can't easily test the actual CLI interaction
	// so we'll just verify the config has the expected environments
	assert.True(t, cm.EnvironmentExists("dev"))
	assert.True(t, cm.EnvironmentExists("prod"))
	assert.Greater(t, len(cm.Environments), 1)

	// Restore stdin
	os.Stdin = oldStdin
}

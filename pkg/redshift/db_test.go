package redshift

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockPostgresClientCreator struct {
	mock.Mock
}

func (m *MockPostgresClientCreator) NewClient(ctx context.Context, config postgres.RedShiftConfig) (*postgres.Client, error) {
	args := m.Called(ctx, config)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*postgres.Client), args.Error(1)
}

func TestNewTableSensorClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		config         postgres.RedShiftConfig
		mockSetup      func(*MockPostgresClientCreator)
		expectError    bool
		expectedError  string
		validateClient func(t *testing.T, client *TableSensorClient)
	}{
		{
			name: "successful client creation",
			config: postgres.RedShiftConfig{
				Username: "testuser",
				Password: "testpass",
				Host:     "localhost",
				Port:     5439,
				Database: "testdb",
				Schema:   "public",
				SslMode:  "require",
			},
			mockSetup: func(mockCreator *MockPostgresClientCreator) {
				mockClient := &postgres.Client{}
				mockCreator.On("NewClient", mock.Anything, mock.Anything).Return(mockClient, nil)
			},
			expectError: false,
			validateClient: func(t *testing.T, client *TableSensorClient) {
				assert.NotNil(t, client, "TableSensorClient should not be nil")
				assert.NotNil(t, client.Client, "Wrapped PostgreSQL client should not be nil")
			},
		},
		{
			name: "postgres client creation fails",
			config: postgres.RedShiftConfig{
				Username: "testuser",
				Password: "testpass",
				Host:     "invalid-host",
				Port:     5439,
				Database: "testdb",
				Schema:   "public",
				SslMode:  "require",
			},
			mockSetup: func(mockCreator *MockPostgresClientCreator) {
				mockCreator.On("NewClient", mock.Anything, mock.Anything).Return(nil, errors.New("connection failed"))
			},
			expectError:   true,
			expectedError: "failed to create Redshift table sensor client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockCreator := &MockPostgresClientCreator{}
			tt.mockSetup(mockCreator)

			client, err := NewTableSensorClient(context.TODO(), tt.config, mockCreator)

			if tt.expectError {
				require.Error(t, err, "Expected an error")
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError, "Error should contain expected message")
				}
				assert.Nil(t, client, "Client should be nil when error occurs")
			} else {
				require.NoError(t, err, "Expected no error")
				if tt.validateClient != nil {
					tt.validateClient(t, client)
				}
			}

			mockCreator.AssertExpectations(t)
		})
	}
}

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := client.BuildTableExistsQuery(tt.tableName)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				return
			}

			require.NoError(t, err, "Unexpected error")
			assert.Equal(t, tt.expected, result, "Query should match expected output")
		})
	}
}

package cmd

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockConnection struct {
	mock.Mock
}

func (m *MockConnection) SelectWithSchema(ctx context.Context, q *query.Query) (*query.QueryResult, error) {
	args := m.Called(ctx, q)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*query.QueryResult), args.Error(1)
}

type MockConnectionManager struct {
	mock.Mock
}

func (m *MockConnectionManager) GetConnection(name string) (interface{}, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func TestFillColumnsFromDB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		existingCols   []pipeline.Column
		dbColumns      []string
		dbColumnTypes  []string
		expectedStatus string
		expectError    bool
		errorMsg       string
		expectedCols   []pipeline.Column
	}{
		{
			name:           "New asset with columns",
			existingCols:   []pipeline.Column{},
			dbColumns:      []string{"id", "name", "created_at"},
			dbColumnTypes:  []string{"INTEGER", "STRING", "TIMESTAMP"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "created_at", Type: "TIMESTAMP", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "Existing asset with new columns",
			existingCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "name", "created_at", "updated_at"},
			dbColumnTypes:  []string{"INTEGER", "STRING", "TIMESTAMP", "TIMESTAMP"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "created_at", Type: "TIMESTAMP", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "updated_at", Type: "TIMESTAMP", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "Skip special columns",
			existingCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "_is_current", "_valid_from", "_valid_until", "name"},
			dbColumnTypes:  []string{"INTEGER", "BOOLEAN", "TIMESTAMP", "TIMESTAMP", "STRING"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "Case insensitive column comparison",
			existingCols: []pipeline.Column{
				{Name: "ID", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "Name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "name", "CREATED_AT"},
			dbColumnTypes:  []string{"INTEGER", "STRING", "TIMESTAMP"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "ID", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "Name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "CREATED_AT", Type: "TIMESTAMP", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name:           "No columns in database",
			existingCols:   []pipeline.Column{},
			dbColumns:      []string{},
			dbColumnTypes:  []string{},
			expectedStatus: fillStatusFailed,
			expectError:    true,
			errorMsg:       "no columns found for asset",
		},
		{
			name: "No new columns to add",
			existingCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "name"},
			dbColumnTypes:  []string{"INTEGER", "STRING"},
			expectedStatus: fillStatusSkipped,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup mock connection
			mockConn := new(MockConnection)
			mockConn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
				Columns:     tt.dbColumns,
				ColumnTypes: tt.dbColumnTypes,
			}, nil)

			// Setup mock connection manager
			mockManager := new(MockConnectionManager)
			mockManager.On("GetConnection", "test_connection").Return(mockConn, nil)

			// Setup test asset
			asset := &pipeline.Asset{
				Name:       "test_asset",
				Columns:    tt.existingCols,
				Type:       pipeline.AssetTypePostgresQuery,
				Connection: "test_connection",
			}

			// Setup pipeline info with mock config
			pp := &ppInfo{
				Asset: asset,
				Pipeline: &pipeline.Pipeline{
					Name: "test_pipeline",
				},
				Config: &config.Config{
					Environments: map[string]config.Environment{
						"test": {
							Connections: &config.Connections{
								Postgres: []config.PostgresConnection{
									{
										Name:     "test_connection",
										Host:     "localhost",
										Port:     5432,
										Database: "testdb",
										Username: "testuser",
										Password: "testpass",
									},
								},
							},
						},
					},
				},
			}

			// Create a memory filesystem for testing
			fs := afero.NewMemMapFs()

			// Execute the function with mock manager
			status, err := fillColumnsFromDB(pp, fs, "test", mockManager)

			// Verify results
			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedStatus, status)
				assert.Equal(t, tt.expectedCols, asset.Columns)
			}

			mockConn.AssertExpectations(t)
			mockManager.AssertExpectations(t)
		})
	}
}

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

func (m *MockConnectionManager) GetConnection(name string) any {
	args := m.Called(name)
	return args.Get(0)
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
		{
			name: "Type change for existing column",
			existingCols: []pipeline.Column{
				{Name: "price", Type: "DECIMAL(5,2)", Checks: []pipeline.ColumnCheck{{Name: "positive"}}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "stock", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"price", "stock"},
			dbColumnTypes:  []string{"DOUBLE", "BIGINT"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "price", Type: "DOUBLE", Checks: []pipeline.ColumnCheck{{Name: "positive"}}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "stock", Type: "BIGINT", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "Mixed scenario: type changes and new columns",
			existingCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "price", Type: "DECIMAL(5,2)", Checks: []pipeline.ColumnCheck{{Name: "positive"}}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "price", "description", "created_at"},
			dbColumnTypes:  []string{"INTEGER", "DOUBLE", "VARCHAR", "TIMESTAMP"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "price", Type: "DOUBLE", Checks: []pipeline.ColumnCheck{{Name: "positive"}}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "description", Type: "VARCHAR", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "created_at", Type: "TIMESTAMP", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
		},
		{
			name: "Case insensitive type change",
			existingCols: []pipeline.Column{
				{Name: "ID", Type: "INT", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "Name", Type: "VARCHAR(50)", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
			},
			dbColumns:      []string{"id", "name"},
			dbColumnTypes:  []string{"BIGINT", "TEXT"},
			expectedStatus: fillStatusUpdated,
			expectError:    false,
			expectedCols: []pipeline.Column{
				{Name: "ID", Type: "BIGINT", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				{Name: "Name", Type: "TEXT", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
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
			status, columns, err := fillColumnsFromDB(pp, fs, "test", mockManager, false)

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
				assert.Equal(t, tt.expectedCols, columns)
			}

			mockConn.AssertExpectations(t)
			mockManager.AssertExpectations(t)
		})
	}
}

func TestFillColumnsFromDB_DryRun(t *testing.T) {
	t.Parallel()

	// Setup mock connection
	mockConn := new(MockConnection)
	mockConn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
		Columns:     []string{"id", "name", "created_at"},
		ColumnTypes: []string{"INTEGER", "STRING", "TIMESTAMP"},
	}, nil)

	// Setup mock connection manager
	mockManager := new(MockConnectionManager)
	mockManager.On("GetConnection", "test_connection").Return(mockConn, nil)

	// Setup test asset with no existing columns
	asset := &pipeline.Asset{
		Name:       "test_asset",
		Columns:    []pipeline.Column{},
		Type:       pipeline.AssetTypePostgresQuery,
		Connection: "test_connection",
	}

	// Setup pipeline info
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

	// Test dry-run mode
	status, columns, err := fillColumnsFromDB(pp, fs, "test", mockManager, true)

	// Verify results
	require.NoError(t, err)
	assert.Equal(t, fillStatusUpdated, status)
	assert.Len(t, columns, 3)
	assert.Equal(t, "id", columns[0].Name)
	assert.Equal(t, "INTEGER", columns[0].Type)
	
	// Verify that asset.Columns were updated in memory
	assert.Len(t, asset.Columns, 3)
	
	// Verify that no file was created (dry-run mode)
	files, err := afero.ReadDir(fs, "/")
	require.NoError(t, err)
	assert.Len(t, files, 0, "No files should be created in dry-run mode")

	mockConn.AssertExpectations(t)
	mockManager.AssertExpectations(t)
}

func TestFillColumnsFromDB_NonDryRun(t *testing.T) {
	t.Parallel()

	// Setup mock connection
	mockConn := new(MockConnection)
	mockConn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
		Columns:     []string{"id", "name"},
		ColumnTypes: []string{"INTEGER", "STRING"},
	}, nil)

	// Setup mock connection manager
	mockManager := new(MockConnectionManager)
	mockManager.On("GetConnection", "test_connection").Return(mockConn, nil)

	// Setup test asset with no existing columns
	asset := &pipeline.Asset{
		Name:       "test_asset",
		Columns:    []pipeline.Column{},
		Type:       pipeline.AssetTypePostgresQuery,
		Connection: "test_connection",
		DefinitionFile: pipeline.TaskDefinitionFile{
			Path: "/test_asset.sql",
		},
	}

	// Setup pipeline info
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

	// Test non-dry-run mode
	status, columns, err := fillColumnsFromDB(pp, fs, "test", mockManager, false)

	// Verify results
	require.NoError(t, err)
	assert.Equal(t, fillStatusUpdated, status)
	assert.Len(t, columns, 2)
	
	// Verify that asset.Columns were updated
	assert.Len(t, asset.Columns, 2)
	
	// In non-dry-run mode, Persist() would be called, but since we're using a mock asset,
	// we can't easily test file creation without more complex mocking

	mockConn.AssertExpectations(t)
	mockManager.AssertExpectations(t)
}

func TestFillColumnsFromDB_JSONOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dryRun      bool
		status      string
		columns     []string
		columnTypes []string
		expectedMsg string
	}{
		{
			name:        "JSON output with dry-run",
			dryRun:      true,
			status:      fillStatusUpdated,
			columns:     []string{"id", "name"},
			columnTypes: []string{"INTEGER", "STRING"},
			expectedMsg: "Would update columns for asset 'test_asset' (dry-run)",
		},
		{
			name:        "JSON output without dry-run",
			dryRun:      false,
			status:      fillStatusUpdated,
			columns:     []string{"id", "name"},
			columnTypes: []string{"INTEGER", "STRING"},
			expectedMsg: "Columns filled from DB for asset 'test_asset'",
		},
		{
			name:        "JSON output skipped",
			dryRun:      false,
			status:      fillStatusSkipped,
			columns:     []string{"id", "name"},
			columnTypes: []string{"INTEGER", "STRING"},
			expectedMsg: "No changes needed for asset 'test_asset'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup mock connection
			mockConn := new(MockConnection)
			mockConn.On("SelectWithSchema", mock.Anything, mock.Anything).Return(&query.QueryResult{
				Columns:     tt.columns,
				ColumnTypes: tt.columnTypes,
			}, nil)

			// Setup mock connection manager
			mockManager := new(MockConnectionManager)
			mockManager.On("GetConnection", "test_connection").Return(mockConn, nil)

			// Setup test asset
			asset := &pipeline.Asset{
				Name:       "test_asset",
				Type:       pipeline.AssetTypePostgresQuery,
				Connection: "test_connection",
			}

			// For skipped status, pre-populate with existing columns
			if tt.status == fillStatusSkipped {
				asset.Columns = []pipeline.Column{
					{Name: "id", Type: "INTEGER", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
					{Name: "name", Type: "STRING", Checks: []pipeline.ColumnCheck{}, Upstreams: []*pipeline.UpstreamColumn{}},
				}
			}

			// Setup pipeline info
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

			// Execute the function
			status, columns, err := fillColumnsFromDB(pp, fs, "test", mockManager, tt.dryRun)

			// Verify core results
			require.NoError(t, err)
			assert.Equal(t, tt.status, status)
			assert.Len(t, columns, len(tt.columns))

			// Test that the columns contain the expected data structure for JSON
			for i, col := range columns {
				assert.Equal(t, tt.columns[i], col.Name)
				assert.Equal(t, tt.columnTypes[i], col.Type)
				assert.NotNil(t, col.Checks, "Checks should be initialized")
				assert.NotNil(t, col.Upstreams, "Upstreams should be initialized")
			}

			// Verify the message would be correct for JSON output
			var expectedMessage string
			switch status {
			case fillStatusUpdated:
				if tt.dryRun {
					expectedMessage = "Would update columns for asset 'test_asset' (dry-run)"
				} else {
					expectedMessage = "Columns filled from DB for asset 'test_asset'"
				}
			case fillStatusSkipped:
				expectedMessage = "No changes needed for asset 'test_asset'"
			case fillStatusFailed:
				expectedMessage = "Failed to fill columns from DB for asset 'test_asset'"
			}
			assert.Equal(t, tt.expectedMsg, expectedMessage)

			mockConn.AssertExpectations(t)
			mockManager.AssertExpectations(t)
		})
	}
}

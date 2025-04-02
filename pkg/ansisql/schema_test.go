package ansisql

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	args := m.Called(ctx, query)
	return args.Error(0)
}

func TestSchemaCreator_CreateSchemaIfNotExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		mockSetup     func(mock *mockDB, cache *sync.Map)
		expectedError string
	}{
		{
			name: "schema already exists in cache",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(mock *mockDB, cache *sync.Map) {
				// Simulate schema being already cached
				cache.Store("TEST_SCHEMA", true)
			},
		},
		{
			name: "schema does not exist, create successfully",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(db *mockDB, cache *sync.Map) {
				db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA"}).Return(nil)
			},
		},
		{
			name: "schema creation fails",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(db *mockDB, cache *sync.Map) {
				// Simulate schema not in cache and error during creation
				db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA"}).
					Return(errors.New("creation failed"))
			},
			expectedError: "failed to create or ensure database: TEST_SCHEMA: creation failed",
		},
		{
			name: "asset name with 1 component",
			asset: &pipeline.Asset{
				Name: "test_table",
			},
			mockSetup: func(mock *mockDB, cache *sync.Map) {
				// No query expected, function should return early
			},
		},
		{
			name: "asset name with 4 components",
			asset: &pipeline.Asset{
				Name: "project.dataset.schema.table",
			},
			mockSetup: func(mock *mockDB, cache *sync.Map) {
				// No query expected, function should return early
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Initialize the DB struct with a schema cache
			cache := &sync.Map{}
			creator := SchemaCreator{
				schemaNameCache: cache,
			}

			db := new(mockDB)

			// Apply the mock setup
			tt.mockSetup(db, cache)

			// Call the function under test
			err := creator.CreateSchemaIfNotExist(context.Background(), db, tt.asset)

			// Validate the result
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

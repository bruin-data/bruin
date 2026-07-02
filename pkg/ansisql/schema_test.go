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
			expectedError: "failed to create or ensure schema: TEST_SCHEMA: creation failed",
		},
		{
			name: "three-part name qualifies schema with the database/catalog",
			asset: &pipeline.Asset{
				Name: "other_db.test_schema.test_table",
			},
			mockSetup: func(db *mockDB, cache *sync.Map) {
				// The schema must be created in the database/catalog named in the
				// asset, not the connection's default, so it has to be qualified.
				db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS OTHER_DB.TEST_SCHEMA"}).Return(nil)
			},
		},
		{
			name: "same schema name in different databases is not deduped by the cache",
			asset: &pipeline.Asset{
				Name: "other_db.test_schema.test_table",
			},
			mockSetup: func(db *mockDB, cache *sync.Map) {
				// A bare "TEST_SCHEMA" was already created in the default database;
				// the cache must not skip creating it in OTHER_DB.
				cache.Store("TEST_SCHEMA", true)
				db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS OTHER_DB.TEST_SCHEMA"}).Return(nil)
			},
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
			err := creator.CreateSchemaIfNotExist(t.Context(), db, tt.asset)

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

func TestSchemaCreator_CreateSchemaIfNotExist_WithContainer(t *testing.T) {
	t.Parallel()

	t.Run("three-part name creates the database then the schema", func(t *testing.T) {
		t.Parallel()
		db := new(mockDB)
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE DATABASE IF NOT EXISTS OTHER_DB"}).Return(nil).Once()
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS OTHER_DB.TEST_SCHEMA"}).Return(nil).Once()

		creator := NewSchemaCreatorWithContainer("DATABASE")
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "other_db.test_schema.test_table"}))
		db.AssertExpectations(t)
	})

	t.Run("two-part name does not create a database", func(t *testing.T) {
		t.Parallel()
		db := new(mockDB)
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA"}).Return(nil).Once()

		creator := NewSchemaCreatorWithContainer("DATABASE")
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "test_schema.test_table"}))
		db.AssertExpectations(t)
		db.AssertNotCalled(t, "RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE DATABASE IF NOT EXISTS TEST_SCHEMA"})
	})

	t.Run("catalog keyword is used for databricks-style platforms", func(t *testing.T) {
		t.Parallel()
		db := new(mockDB)
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE CATALOG IF NOT EXISTS MAIN"}).Return(nil).Once()
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS MAIN.SILVER"}).Return(nil).Once()

		creator := NewSchemaCreatorWithContainer("CATALOG")
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "main.silver.orders"}))
		db.AssertExpectations(t)
	})

	t.Run("database creation is cached across assets in the same database", func(t *testing.T) {
		t.Parallel()
		db := new(mockDB)
		// CREATE DATABASE must run only once for two assets in the same database.
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE DATABASE IF NOT EXISTS RAW"}).Return(nil).Once()
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS RAW.S1"}).Return(nil).Once()
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS RAW.S2"}).Return(nil).Once()

		creator := NewSchemaCreatorWithContainer("DATABASE")
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "raw.s1.t1"}))
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "raw.s2.t2"}))
		db.AssertExpectations(t)
		db.AssertNumberOfCalls(t, "RunQueryWithoutResult", 3)
	})

	t.Run("default creator does not create a database", func(t *testing.T) {
		t.Parallel()
		db := new(mockDB)
		db.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE SCHEMA IF NOT EXISTS OTHER_DB.TEST_SCHEMA"}).Return(nil).Once()

		creator := NewSchemaCreator()
		require.NoError(t, creator.CreateSchemaIfNotExist(t.Context(), db, &pipeline.Asset{Name: "other_db.test_schema.test_table"}))
		db.AssertExpectations(t)
		db.AssertNumberOfCalls(t, "RunQueryWithoutResult", 1)
	})
}

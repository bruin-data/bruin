package snowflake

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          query.Query
		want           bool
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple valid select query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`EXPLAIN SELECT 1;`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"}))
			},
			query: query.Query{
				Query: "SELECT 1",
			},
			want: true,
		},
		{
			name: "invalid query is properly handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`EXPLAIN some broken query;`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(fmt.Errorf("%s\nsome actual error", invalidQueryError))
			},
			query: query.Query{
				Query: "some broken query",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "some actual error",
		},
		{
			name: "generic errors are just propagated",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`EXPLAIN some broken query;`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(errors.New("something went wrong"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "something went wrong",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			got, err := db.IsValid(context.Background(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          query.Query
		want           [][]interface{}
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple select query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1, 2, 3`).
					WillReturnRows(sqlmock.NewRows([]string{"one", "two", "three"}).AddRow(1, 2, 3))
			},
			query: query.Query{
				Query: "SELECT 1, 2, 3",
			},
			want: [][]interface{}{{int64(1), int64(2), int64(3)}},
		},
		{
			name: "multi-row select query is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some query`).
					WillReturnRows(sqlmock.NewRows([]string{"one", "two", "three"}).
						AddRow(1, 2, 3).
						AddRow(4, 5, 6),
					)
			},
			query: query.Query{
				Query: "some query",
			},
			want: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(4), int64(5), int64(6)},
			},
		},
		{
			name: "invalid query is properly handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(fmt.Errorf("%s\nsome actual error", invalidQueryError))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: invalidQueryError + "  -  some actual error",
		},
		{
			name: "generic errors are just propagated",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnRows(sqlmock.NewRows([]string{"rows", "filtered"})).
					WillReturnError(errors.New("something went wrong"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "something went wrong",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			got, err := db.Select(context.Background(), &tt.query)
			if tt.wantErr {
				require.Error(t, err)
				require.Equal(t, tt.errorMessage, err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "valid connection test",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
			},
			wantErr: false,
		},
		{
			name: "failed connection test",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT 1`).
					WillReturnError(errors.New("connection error"))
			},
			wantErr:      true,
			errorMessage: "failed to run test query on Snowflake connection: connection error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			err = db.Ping(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_SelectWithSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          query.Query
		want           *query.QueryResult
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple successful query with schema",
			mockConnection: func(mock sqlmock.Sqlmock) {
				// Mocking the query response with schema and data rows
				mock.ExpectQuery(`SELECT first_name, last_name, age FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name", "age"}).
						AddRow("jane", "doe", 30).
						AddRow("joe", "doe", 28))
			},
			query: query.Query{
				Query: "SELECT first_name, last_name, age FROM users",
			},
			want: &query.QueryResult{
				Columns: []string{"first_name", "last_name", "age"},
				Rows: [][]interface{}{
					{"jane", "doe", int64(30)},
					{"joe", "doe", int64(28)},
				},
			},
		},
		{
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				// Mocking a syntax error in the SQL query
				mock.ExpectQuery(`some broken query`).
					WillReturnError(errors.New("SQL compilation error: syntax error at position 1"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "SQL compilation error: syntax error at position 1",
		},
		{
			name: "generic error propagation",
			mockConnection: func(mock sqlmock.Sqlmock) {
				// Simulating a generic connection error
				mock.ExpectQuery(`SELECT first_name FROM users`).
					WillReturnError(errors.New("connection issue"))
			},
			query: query.Query{
				Query: "SELECT first_name FROM users",
			},
			wantErr:      true,
			errorMessage: "connection issue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setting up sqlmock
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			// Apply the mock connection setup
			tt.mockConnection(mock)
			db := DB{conn: sqlxDB}

			// Execute SelectWithSchema
			got, err := db.SelectWithSchema(context.Background(), &tt.query)

			// Validate the error expectations
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}

			// Ensure all expectations were met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_RecreateTableOnMaterializationTypeMismatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		mockSetup     func(mock sqlmock.Sqlmock)
		asset         *pipeline.Asset
		expectedError string
	}{
		{
			name: "materialization type mismatch, table dropped and recreated",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock the SELECT query to check the table type
				mock.ExpectQuery(`SELECT TABLE_TYPE FROM MYDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE"}).AddRow("VIEW"))

				mock.ExpectQuery(`DROP VIEW IF EXISTS TEST_SCHEMA.TEST_TABLE`).
					WillReturnRows(sqlmock.NewRows(nil))
			},
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
		},
		{
			name: "table or view does not exist",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock the SELECT query to return no rows
				mock.ExpectQuery(`SELECT TABLE_TYPE FROM MYDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE"}))
			},
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			expectedError: "table or view TEST_SCHEMA.TEST_TABLE does not exist",
		},
		{
			name: "error during table type retrieval",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock the SELECT query to return an error
				mock.ExpectQuery(`SELECT TABLE_TYPE FROM MYDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`).
					WillReturnError(errors.New("query error"))
			},
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			expectedError: "unable to retrieve table metadata for 'TEST_SCHEMA.TEST_TABLE': query error",
		},
		{
			name: "materialization type matches, no action taken",
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Mock the SELECT query to return the same type
				mock.ExpectQuery(`SELECT TABLE_TYPE FROM MYDB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`).
					WillReturnRows(sqlmock.NewRows([]string{"TABLE_TYPE"}).AddRow("BASE TABLE"))
			},
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
		},
		{
			name: "asset name with 1 component",
			asset: &pipeline.Asset{
				Name: "test_table",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// No query expected, function should return early
			},
		},
		{
			name: "asset name with 4 components",
			asset: &pipeline.Asset{
				Name: "project.dataset.schema.table",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// No query expected, function should return early
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Setup sqlmock
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			// Initialize the DB struct with the mock connection
			db := &DB{
				conn: sqlxDB,
				config: &Config{ // Pass a pointer to Config
					Database: "MYDB",
				},
			}

			// Apply the mock setup for this test
			tt.mockSetup(mock)

			// Call the function under test
			err = db.RecreateTableOnMaterializationTypeMismatch(context.Background(), tt.asset)

			// Validate the expected outcome
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Ensure all expectations were met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_CreateSchemaIfNotExist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		mockSetup     func(mock sqlmock.Sqlmock, cache *sync.Map)
		expectedError string
	}{
		{
			name: "schema already exists in cache",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(mock sqlmock.Sqlmock, cache *sync.Map) {
				// Simulate schema being already cached
				cache.Store("TEST_SCHEMA", true)
			},
		},
		{
			name: "schema does not exist, create successfully",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(mock sqlmock.Sqlmock, cache *sync.Map) {
				// Simulate schema not in cache
				mock.ExpectQuery("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA").
					WillReturnRows(sqlmock.NewRows(nil)) // Simulate success with an empty result
			},
		},
		{
			name: "schema creation fails",
			asset: &pipeline.Asset{
				Name: "test_schema.test_table",
			},
			mockSetup: func(mock sqlmock.Sqlmock, cache *sync.Map) {
				// Simulate schema not in cache and error during creation
				mock.ExpectQuery("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA").
					WillReturnError(errors.New("creation failed"))
			},
			expectedError: "failed to create or ensure database: TEST_SCHEMA: creation failed",
		},
		{
			name: "asset name with 1 component",
			asset: &pipeline.Asset{
				Name: "test_table",
			},
			mockSetup: func(mock sqlmock.Sqlmock, cache *sync.Map) {
				// No query expected, function should return early
			},
		},
		{
			name: "asset name with 4 components",
			asset: &pipeline.Asset{
				Name: "project.dataset.schema.table",
			},
			mockSetup: func(mock sqlmock.Sqlmock, cache *sync.Map) {
				// No query expected, function should return early
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Setup sqlmock
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

			// Initialize the DB struct with a schema cache
			cache := &sync.Map{}
			db := &DB{
				conn:            sqlxDB,
				schemaNameCache: cache,
			}

			// Apply the mock setup
			tt.mockSetup(mock, cache)

			// Call the function under test
			err = db.CreateSchemaIfNotExist(context.Background(), tt.asset)

			// Validate the result
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}

			// Ensure all expectations were met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_PushColumnDescriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedError string
	}{
		{
			name: "no metadata to push",
			asset: &pipeline.Asset{
				Name:    "test_schema.test_table",
				Columns: []pipeline.Column{},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// No database interaction expected since there is no metadata to push
			},
			expectedError: "no metadata to push: table and columns have no descriptions",
		},
		{
			name: "successfully update column descriptions",
			asset: &pipeline.Asset{
				Name:        "test_schema.test_table",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: "Description 1"},
					{Name: "col2", Description: "Description 2"},
				},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Simulate querying existing metadata
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
                     FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
                     WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COMMENT"}).
					AddRow("COL1", ""). // No description exists
					AddRow("COL2", ""), // No description exists
				)

				// Simulate updating column descriptions
				mock.ExpectQuery(`ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE MODIFY COLUMN col1 COMMENT 'Description 1'`).
					WillReturnRows(sqlmock.NewRows(nil))
				mock.ExpectQuery(`ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE MODIFY COLUMN col2 COMMENT 'Description 2'`).
					WillReturnRows(sqlmock.NewRows(nil))
			},
		},
		{
			name: "successfully update table description",
			asset: &pipeline.Asset{
				Name:        "test_schema.test_table",
				Description: "Table description",
				Columns:     []pipeline.Column{},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Simulate querying existing metadata
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
                     FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
                     WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnRows(sqlmock.NewRows(nil)) // No columns exist

				// Simulate updating table description
				mock.ExpectQuery(`COMMENT ON TABLE MYDB.TEST_SCHEMA.TEST_TABLE IS 'Table description'`).
					WillReturnRows(sqlmock.NewRows(nil))
			},
		},
		{
			name: "error during querying existing metadata",
			asset: &pipeline.Asset{
				Name:        "test_schema.test_table",
				Description: "Table description", // Add a description to ensure it doesn't return early
				Columns:     []pipeline.Column{},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				// Simulate an error during querying the column metadata
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
			 FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
			 WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnError(errors.New("query error")) // Simulate the query error
			},
			expectedError: "failed to query column metadata for TEST_SCHEMA.TEST_TABLE: query error", // Expected error
		},
		{
			name: "error during updating column description",
			asset: &pipeline.Asset{
				Name:        "test_schema.test_table",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: "Description 1"},
				},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
                     FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
                     WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COMMENT"}).
					AddRow("COL1", "")) // No description exists

				mock.ExpectQuery(`ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE MODIFY COLUMN col1 COMMENT 'Description 1'`).
					WillReturnError(errors.New("update error"))
			},
			expectedError: "failed to update description for column col1: update error",
		},
		{
			name: "error during updating table description",
			asset: &pipeline.Asset{
				Name:        "test_schema.test_table",
				Description: "Table description",
				Columns:     []pipeline.Column{},
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
                     FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
                     WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnRows(sqlmock.NewRows(nil)) // No columns exist
				mock.ExpectQuery(`COMMENT ON TABLE MYDB.TEST_SCHEMA.TEST_TABLE IS 'Table description'`).
					WillReturnError(errors.New("update error"))
			},
			expectedError: "failed to update table description: update error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			sqlxDB := sqlx.NewDb(mockDB, "sqlmock")
			db := &DB{
				conn: sqlxDB,
				config: &Config{
					Database: "MYDB",
				},
			}
			tt.mockSetup(mock)
			err = db.PushColumnDescriptions(context.Background(), tt.asset)

			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

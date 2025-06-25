package postgres

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/jmoiron/sqlx"
	"testing"

	_ "github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		expected  string
		setupMock func(mock pgxmock.PgxPoolIface)
		wantErr   string
		want      [][]interface{}
	}{
		{
			name:    "test select rows",
			query:   "SELECT * FROM table",
			wantErr: "",
			want:    [][]interface{}{{1, "John Doe"}, {2, "Jane Doe"}},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe").AddRow(2, "Jane Doe")
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:  "test select single row",
			query: "SELECT * FROM table",
			want:  [][]interface{}{{1, "John Doe"}},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:  "test select empty rows",
			query: "SELECT * FROM table",
			want:  [][]interface{}{},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				)
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:    "test select errors",
			query:   "SELECT * FROM table",
			wantErr: "Some error",
			want:    nil,
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnError(errors.New("Some error"))
			},
		},
		{
			name:    "test fail scanning rows errors",
			query:   "SELECT * FROM table",
			wantErr: "failed to collect row values: Some scan error",
			want:    nil,
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				rows.RowError(1, errors.New("Some scan error"))
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			result, err := client.Select(context.TODO(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				assert.Equal(t, tt.wantErr, err.Error())
				require.Error(t, err)
			}

			assert.Equal(t, tt.want, result)
		})
	}
}

func TestClient_SelectWithSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		expected  *query.QueryResult
		setupMock func(mock pgxmock.PgxPoolIface)
		wantErr   string
	}{
		{
			name:  "test select rows with schema",
			query: "SELECT * FROM table",
			expected: &query.QueryResult{
				Columns: []string{"id", "name"},
				Rows: [][]interface{}{
					{1, "John Doe"},
					{2, "Jane Doe"},
				},
				ColumnTypes: []string{"int8", "varchar"},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id", DataTypeOID: 20},     // BIGINT
					pgconn.FieldDescription{Name: "name", DataTypeOID: 1043}, // VARCHAR
				).AddRow(1, "John Doe").AddRow(2, "Jane Doe")
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:  "test select empty rows with schema",
			query: "SELECT * FROM table",
			expected: &query.QueryResult{
				Columns:     []string{"id", "name"},
				Rows:        [][]interface{}{},
				ColumnTypes: []string{"int8", "varchar"},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id", DataTypeOID: 20},     // BIGINT
					pgconn.FieldDescription{Name: "name", DataTypeOID: 1043}, // VARCHAR
				)
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:    "test select errors with schema",
			query:   "SELECT * FROM table",
			wantErr: "failed to execute query: Some error", // Updated error message
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnError(errors.New("Some error"))
			},
		},
		{
			name:    "test fail scanning rows errors with schema",
			query:   "SELECT * FROM table",
			wantErr: "failed to collect row values: Some scan error",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				rows.RowError(1, errors.New("Some scan error"))
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			result, err := client.SelectWithSchema(context.TODO(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result, "Expected QueryResult does not match actual")
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}

			// Assert column validation for expected schema
			if tt.expected != nil {
				assert.Equal(t, tt.expected.Columns, result.Columns, "Column names do not match")
			}
		})
	}
}

func TestClient_RunQueryWithoutResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		setupMock func(mock pgxmock.PgxPoolIface)
		wantErr   string
	}{
		{
			name:  "test successful execution",
			query: "DELETE FROM table WHERE id = 1",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectExec("DELETE FROM table WHERE id = 1").WillReturnResult(pgxmock.NewResult("DELETE", 1))
			},
			wantErr: "",
		},
		{
			name:  "test execution error",
			query: "DELETE FROM table WHERE id = 1",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectExec("DELETE FROM table WHERE id = 1").WillReturnError(errors.New("execution error"))
			},
			wantErr: "execution error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			err = client.RunQueryWithoutResult(context.TODO(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}

			// Verify all expectations are met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(mock pgxmock.PgxPoolIface)
		wantErr   string
	}{
		{
			name: "test successful ping",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectExec("SELECT 1").WillReturnResult(pgxmock.NewResult("SELECT", 1))
			},
			wantErr: "",
		},
		{
			name: "test ping with execution error",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectExec("SELECT 1").WillReturnError(errors.New("ping error"))
			},
			wantErr: "failed to run test query on Postgres connection: ping error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			err = client.Ping(context.TODO())

			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
			}

			// Verify all expectations are met
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupMock    func(mock pgxmock.PgxPoolIface)
		query        query.Query
		want         bool
		wantErr      bool
		errorMessage string
	}{
		{
			name: "simple valid select query is handled",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				mock.ExpectQuery("EXPLAIN SELECT 1;").WillReturnRows(rows)
			},
			query: query.Query{
				Query: "SELECT 1",
			},
			want: true,
		},
		{
			name: "invalid query is properly handled",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery(`EXPLAIN some broken query;`).
					WillReturnError(errors.New("some actual error"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "some actual error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			got, err := client.IsValid(context.Background(), &tt.query)
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

func TestDB_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setupMock func(mock pgxmock.PgxPoolIface)
		want      *ansisql.DBDatabase
		wantErr   string
	}{
		{
			name: "test successful database summary",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "schema_name"},
					pgconn.FieldDescription{Name: "table_name"},
				).
					AddRow("schema1", "table1").
					AddRow("schema1", "table2").
					AddRow("schema2", "table2")

				mock.ExpectQuery(".*").WithArgs("database1").WillReturnRows(rows)
			},
			want: &ansisql.DBDatabase{
				Name: "database1",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "schema1",
						Tables: []*ansisql.DBTable{
							{
								Name: "table1",
							},
							{
								Name: "table2",
							},
						},
					},
					{
						Name: "schema2",
						Tables: []*ansisql.DBTable{
							{
								Name: "table2",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock, config: Config{Database: "database1"}}

			got, err := client.GetDatabaseSummary(context.Background())
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, tt.wantErr, err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
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
			name: "successfully update column descriptions with concatenated queries",
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

				mock.ExpectQuery(
					`ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE 
             MODIFY COLUMN col1 COMMENT 'Description 1'; 
             ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE 
             MODIFY COLUMN col2 COMMENT 'Description 2'`,
				).WillReturnRows(sqlmock.NewRows(nil)) // Expect 2 rows to be affected
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
				// Simulate querying existing metadata
				mock.ExpectQuery(
					`SELECT COLUMN_NAME, COMMENT 
             FROM MYDB.INFORMATION_SCHEMA.COLUMNS 
             WHERE TABLE_SCHEMA = 'TEST_SCHEMA' AND TABLE_NAME = 'TEST_TABLE'`,
				).WillReturnRows(sqlmock.NewRows([]string{"COLUMN_NAME", "COMMENT"}).
					AddRow("COL1", "")) // No description exists

				// Simulate an error during column description update
				mock.ExpectQuery(
					`ALTER TABLE MYDB.TEST_SCHEMA.TEST_TABLE MODIFY COLUMN col1 COMMENT 'Description 1'`,
				).WillReturnError(errors.New("update error"))
			},
			expectedError: "failed to update column descriptions: update error",
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
			db := &Client{
				connection: sqlxDB,
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

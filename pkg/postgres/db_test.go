package postgres

import (
	"context"
	"errors"
	"github.com/bruin-data/bruin/pkg/pipeline"
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

func TestClient_PushColumnDescriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		asset         *pipeline.Asset
		setupMock     func(mock pgxmock.PgxPoolIface)
		expectedError string
	}{
		{
			name: "table formatted incorrectly",
			asset: &pipeline.Asset{
				Name:    "mytable",
				Columns: []pipeline.Column{},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				// No DB interaction expected
			},
			expectedError: "table name must be in schema.table or table format, 'mytable' given",
		},
		{
			name: "no metadata to push",
			asset: &pipeline.Asset{
				Name:    "database.myschema.mytable",
				Columns: []pipeline.Column{},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				// No DB interaction expected
			},
			expectedError: "no metadata to push: table and columns have no descriptions",
		},
		{
			name: "update column descriptions",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: ""},
					{Name: "col2", Description: "desc2"},
				},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				// Mock query for existing column comments
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", nil).
					AddRow("col2", nil)
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
				// Mock update for column comments
				mock.ExpectExec("COMMENT ON COLUMN MYSCHEMA\\.MYTABLE\\.col2 IS 'desc2';").
					WillReturnResult(pgxmock.NewResult("UPDATE", 2))
			},
		},
		{
			name: "update column descriptions",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: "desc1"},
					{Name: "col2", Description: "desc2"},
				},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				// Mock query for existing column comments
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", nil).
					AddRow("col2", nil)
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
				// Mock update for column comments
				mock.ExpectExec("COMMENT ON COLUMN MYSCHEMA\\.MYTABLE\\.col1 IS 'desc1';" +
					"\nCOMMENT ON COLUMN MYSCHEMA\\.MYTABLE\\.col2 IS 'desc2'").
					WillReturnResult(pgxmock.NewResult("UPDATE", 2))
			},
		},
		{
			name: "no new metadata to push",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: "desc1"},
					{Name: "col2", Description: "desc2"},
				},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				// Mock query for existing column comments
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", "desc1").
					AddRow("col2", "desc2")
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
			},
		},
		{
			name: "update table description",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "table desc",
				Columns: []pipeline.Column{
					{Name: "col1", Description: ""},
				},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", nil)
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
				mock.ExpectExec("COMMENT ON TABLE MYSCHEMA\\.MYTABLE IS 'table desc';").WillReturnResult(pgxmock.NewResult("UPDATE", 1))
			},
		},
		{
			name: "error querying column metadata",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "desc",
				Columns:     []pipeline.Column{},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnError(errors.New("query error"))
			},
			expectedError: "failed to query column metadata for MYSCHEMA.MYTABLE: query error",
		},
		{
			name: "error updating column descriptions",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "",
				Columns: []pipeline.Column{
					{Name: "col1", Description: "desc1"},
				},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", nil)
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
				mock.ExpectExec("COMMENT ON COLUMN MYSCHEMA\\.MYTABLE\\.col1 IS 'desc1'").
					WillReturnError(errors.New("update error"))
			},
			expectedError: "failed to update column descriptions: update error",
		},
		{
			name: "error updating table description",
			asset: &pipeline.Asset{
				Name:        "myschema.mytable",
				Description: "desc",
				Columns:     []pipeline.Column{},
			},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRows([]string{"column_name", "description"}).
					AddRow("col1", nil)
				mock.ExpectQuery("SELECT" +
					"\n\tcols.column_name," +
					"\n\tpgd.description" +
					"\nFROM" +
					"\n\tpg_catalog.pg_statio_all_tables AS st" +
					"\nJOIN" +
					"\n\tpg_catalog.pg_description pgd" +
					"\n\tON pgd.objoid = st.relid" +
					"\nJOIN" +
					"\n\tinformation_schema.columns cols" +
					"\n\tON cols.table_schema = st.schemaname" +
					"\n\tAND cols.table_name = st.relname" +
					"\n\tAND cols.ordinal_position = pgd.objsubid" +
					"\nWHERE" +
					"\n\tcols.table_name = 'MYTABLE'" +
					"\n\tAND cols.table_schema = 'MYSCHEMA';").WillReturnRows(rows)
				mock.ExpectExec("COMMENT ON TABLE MYSCHEMA\\.MYTABLE IS 'desc'").
					WillReturnError(errors.New("update error"))
			},
			expectedError: "failed to update table description: update error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()

			tt.setupMock(mock)
			client := Client{connection: mock, config: Config{Database: "db"}}

			err = client.PushColumnDescriptions(context.Background(), tt.asset)
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

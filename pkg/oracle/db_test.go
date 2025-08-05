package oracle

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid service name configuration",
			config: Config{
				Host:        "localhost",
				Port:        "1521",
				Username:    "testuser",
				Password:    "testpass",
				ServiceName: "ORCL",
			},
			expectError: false,
		},
		{
			name: "valid SID configuration",
			config: Config{
				Host:     "localhost",
				Port:     "1521",
				Username: "testuser",
				Password: "testpass",
				SID:      "ORCL",
			},
			expectError: false,
		},
		{
			name: "missing service name and SID",
			config: Config{
				Host:     "localhost",
				Port:     "1521",
				Username: "testuser",
				Password: "testpass",
			},
			expectError: true,
			errorMsg:    "failed to create DSN: either ServiceName or SID must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient(tt.config)

			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, client)
				assert.Equal(t, &tt.config, client.config)
			}
		})
	}
}

func TestClient_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          *query.Query
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
			query: &query.Query{
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
			query: &query.Query{
				Query: "some query",
			},
			want: [][]interface{}{
				{int64(1), int64(2), int64(3)},
				{int64(4), int64(5), int64(6)},
			},
		},
		{
			name: "query with semicolon is trimmed",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))
			},
			query: &query.Query{
				Query: "SELECT * FROM users;",
			},
			want: [][]interface{}{{int64(1), "John"}},
		},
		{
			name: "query with whitespace is trimmed",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))
			},
			query: &query.Query{
				Query: "  SELECT * FROM users  ",
			},
			want: [][]interface{}{{int64(1), "John"}},
		},
		{
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnError(errors.New("syntax error"))
			},
			query: &query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "failed to execute select query: syntax error",
		},
		{
			name: "empty result set",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM empty_table`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
			},
			query: &query.Query{
				Query: "SELECT * FROM empty_table",
			},
			want: [][]interface{}(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB}

			got, err := client.Select(context.Background(), tt.query)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_SelectWithSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          *query.Query
		want           *query.QueryResult
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "simple successful query with schema",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT first_name, last_name, age FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"first_name", "last_name", "age"}).
						AddRow("jane", "doe", 30).
						AddRow("joe", "doe", 28))
			},
			query: &query.Query{
				Query: "SELECT first_name, last_name, age FROM users",
			},
			want: &query.QueryResult{
				Columns:     []string{"first_name", "last_name", "age"},
				ColumnTypes: []string{"VARCHAR2", "VARCHAR2", "NUMBER"},
				Rows: [][]interface{}{
					{"jane", "doe", int64(30)},
					{"joe", "doe", int64(28)},
				},
			},
		},
		{
			name: "query with semicolon is trimmed",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM users`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John"))
			},
			query: &query.Query{
				Query: "SELECT * FROM users;",
			},
			want: &query.QueryResult{
				Columns:     []string{"id", "name"},
				ColumnTypes: []string{"NUMBER", "VARCHAR2"},
				Rows:        [][]interface{}{{int64(1), "John"}},
			},
		},
		{
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`some broken query`).
					WillReturnError(errors.New("syntax error"))
			},
			query: &query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "syntax error",
		},
		{
			name: "empty result set is handled",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM empty_table`).
					WillReturnRows(sqlmock.NewRows([]string{"id", "name"}))
			},
			query: &query.Query{
				Query: "SELECT * FROM empty_table",
			},
			want: &query.QueryResult{
				Columns:     []string{"id", "name"},
				ColumnTypes: []string{"NUMBER", "VARCHAR2"},
				Rows:        [][]interface{}{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB}

			got, err := client.SelectWithSchema(context.Background(), tt.query)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want.Columns, got.Columns)
				assert.Equal(t, tt.want.Rows, got.Rows)
				// Column types might vary by driver, so we just check they exist
				assert.Len(t, got.ColumnTypes, len(tt.want.ColumnTypes))
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_RunQueryWithoutResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          *query.Query
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "successful insert query",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`INSERT INTO users (name) VALUES ('John')`).
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			query: &query.Query{
				Query: "INSERT INTO users (name) VALUES ('John')",
			},
			wantErr: false,
		},
		{
			name: "successful update query",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`UPDATE users SET name = 'Jane' WHERE id = 1`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			query: &query.Query{
				Query: "UPDATE users SET name = 'Jane' WHERE id = 1",
			},
			wantErr: false,
		},
		{
			name: "query with semicolon is trimmed",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`DELETE FROM users WHERE id = 1`).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			query: &query.Query{
				Query: "DELETE FROM users WHERE id = 1;",
			},
			wantErr: false,
		},
		{
			name: "query with whitespace is trimmed",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`CREATE TABLE test (id NUMBER)`).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			query: &query.Query{
				Query: "  CREATE TABLE test (id NUMBER)  ",
			},
			wantErr: false,
		},
		{
			name: "invalid query returns error",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`some broken query`).
					WillReturnError(errors.New("syntax error"))
			},
			query: &query.Query{
				Query: "some broken query",
			},
			wantErr:      true,
			errorMessage: "failed to execute query: syntax error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB}

			err = client.RunQueryWithoutResult(context.Background(), tt.query)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		wantErr        bool
		errorMessage   string
	}{
		{
			name: "successful ping",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`SELECT 1 FROM DUAL`).
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			wantErr: false,
		},
		{
			name: "failed ping",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec(`SELECT 1 FROM DUAL`).
					WillReturnError(errors.New("connection refused"))
			},
			wantErr:      true,
			errorMessage: "failed to execute query: connection refused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB}

			err = client.Ping(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		config         *Config
		want           *ansisql.DBDatabase
		wantErr        string
	}{
		{
			name:   "successful database summary",
			config: &Config{ServiceName: "ORCL"},
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    owner as schema_name,
    table_name
FROM
    all_tables
WHERE
    owner NOT IN \(
        'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'APPQOSSYS', 'DBSNMP', 'CTXSYS', 'XDB', 'ANONYMOUS', 'EXFSYS', 'MDDATA', 'DBSFWUSER', 'REMOTE_SCHEDULER_AGENT', 'SI_INFORMTN_SCHEMA', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'APEX_040000', 'APEX_PUBLIC_USER', 'FLOWS_FILES', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'HR', 'OE', 'PM', 'IX', 'SH', 'BI', 'SCOTT',
        'DVSYS', 'LBACSYS', 'OJVMSYS', 'VECSYS', 'AUDSYS', 'GSMADMIN_INTERNAL',
        'DGPDB_INT', 'DVF', 'GGSHAREDCAP', 'GGSYS', 'GSMCATUSER', 'GSMUSER', 'SYS\$UMF', 'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC', 'XS\$NULL', 'PDBADMIN'
    \)
ORDER BY owner, table_name`).
					WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name"}).
						AddRow("schema1", "table1").
						AddRow("schema1", "table2").
						AddRow("schema2", "table1"))
			},
			want: &ansisql.DBDatabase{
				Name: "ORCL",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "schema1",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Columns: []*ansisql.DBColumn{}},
							{Name: "table2", Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "schema2",
						Tables: []*ansisql.DBTable{
							{Name: "table1", Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
		},
		{
			name:   "query error",
			config: &Config{ServiceName: "ORCL"},
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    owner as schema_name,
    table_name
FROM
    all_tables
WHERE
    owner NOT IN \(
        'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'APPQOSSYS', 'DBSNMP', 'CTXSYS', 'XDB', 'ANONYMOUS', 'EXFSYS', 'MDDATA', 'DBSFWUSER', 'REMOTE_SCHEDULER_AGENT', 'SI_INFORMTN_SCHEMA', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'APEX_040000', 'APEX_PUBLIC_USER', 'FLOWS_FILES', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'HR', 'OE', 'PM', 'IX', 'SH', 'BI', 'SCOTT',
        'DVSYS', 'LBACSYS', 'OJVMSYS', 'VECSYS', 'AUDSYS', 'GSMADMIN_INTERNAL',
        'DGPDB_INT', 'DVF', 'GGSHAREDCAP', 'GGSYS', 'GSMCATUSER', 'GSMUSER', 'SYS\$UMF', 'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC', 'XS\$NULL', 'PDBADMIN'
    \)
ORDER BY owner, table_name`).
					WillReturnError(errors.New("connection error"))
			},
			wantErr: "failed to query Oracle all_tables: failed to execute select query: connection error",
		},
		{
			name:   "empty result set",
			config: &Config{ServiceName: "ORCL"},
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT
    owner as schema_name,
    table_name
FROM
    all_tables
WHERE
    owner NOT IN \(
        'SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'APPQOSSYS', 'DBSNMP', 'CTXSYS', 'XDB', 'ANONYMOUS', 'EXFSYS', 'MDDATA', 'DBSFWUSER', 'REMOTE_SCHEDULER_AGENT', 'SI_INFORMTN_SCHEMA', 'ORDDATA', 'ORDSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'APEX_040000', 'APEX_PUBLIC_USER', 'FLOWS_FILES', 'SPATIAL_CSW_ADMIN_USR', 'SPATIAL_WFS_ADMIN_USR', 'HR', 'OE', 'PM', 'IX', 'SH', 'BI', 'SCOTT',
        'DVSYS', 'LBACSYS', 'OJVMSYS', 'VECSYS', 'AUDSYS', 'GSMADMIN_INTERNAL',
        'DGPDB_INT', 'DVF', 'GGSHAREDCAP', 'GGSYS', 'GSMCATUSER', 'GSMUSER', 'SYS\$UMF', 'SYSBACKUP', 'SYSDG', 'SYSKM', 'SYSRAC', 'XS\$NULL', 'PDBADMIN'
    \)
ORDER BY owner, table_name`).
					WillReturnRows(sqlmock.NewRows([]string{"schema_name", "table_name"}))
			},
			want: &ansisql.DBDatabase{
				Name:    "ORCL",
				Schemas: []*ansisql.DBSchema{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB, config: tt.config}

			got, err := client.GetDatabaseSummary(context.Background())
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				// Check that we have the same number of schemas and tables
				assert.Len(t, got.Schemas, len(tt.want.Schemas))
				assert.Equal(t, tt.want.Name, got.Name)

				// Create a map to check schemas regardless of order
				expectedSchemas := make(map[string]*ansisql.DBSchema)
				for _, schema := range tt.want.Schemas {
					expectedSchemas[schema.Name] = schema
				}

				actualSchemas := make(map[string]*ansisql.DBSchema)
				for _, schema := range got.Schemas {
					actualSchemas[schema.Name] = schema
				}

				for name, expectedSchema := range expectedSchemas {
					actualSchema, exists := actualSchemas[name]
					assert.True(t, exists, "Schema %s not found", name)
					if exists {
						assert.Equal(t, expectedSchema.Name, actualSchema.Name)
						assert.Len(t, actualSchema.Tables, len(expectedSchema.Tables))

						// Check tables
						expectedTables := make(map[string]*ansisql.DBTable)
						for _, table := range expectedSchema.Tables {
							expectedTables[table.Name] = table
						}

						actualTables := make(map[string]*ansisql.DBTable)
						for _, table := range actualSchema.Tables {
							actualTables[table.Name] = table
						}

						for tableName, expectedTable := range expectedTables {
							actualTable, exists := actualTables[tableName]
							assert.True(t, exists, "Table %s not found in schema %s", tableName, name)
							if exists {
								assert.Equal(t, expectedTable.Name, actualTable.Name)
							}
						}
					}
				}
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_Select_ErrorHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mockConnection func(mock sqlmock.Sqlmock)
		query          *query.Query
		errorMessage   string
	}{
		{
			name: "error getting column names",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(`SELECT * FROM users`).
					WillReturnError(errors.New("column error"))
			},
			query: &query.Query{
				Query: "SELECT * FROM users",
			},
			errorMessage: "failed to execute select query: column error",
		},
		{
			name: "error scanning row",
			mockConnection: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John")
				rows.RowError(0, errors.New("scan error"))
				mock.ExpectQuery(`SELECT * FROM users`).WillReturnRows(rows)
			},
			query: &query.Query{
				Query: "SELECT * FROM users",
			},
			errorMessage: "error during row iteration: scan error",
		},
		{
			name: "error during row iteration",
			mockConnection: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John")
				rows.CloseError(errors.New("iteration error"))
				mock.ExpectQuery(`SELECT * FROM users`).WillReturnRows(rows)
			},
			query: &query.Query{
				Query: "SELECT * FROM users",
			},
			errorMessage: "error during row iteration: iteration error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			tt.mockConnection(mock)
			client := &Client{conn: mockDB}

			_, err = client.Select(context.Background(), tt.query)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errorMessage)

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

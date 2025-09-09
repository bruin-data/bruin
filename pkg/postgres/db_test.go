package postgres

import (
	"context"
	"errors"
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
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT 1
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "SELECT 1",
			},
			want: true,
		},
		{
			name: "complex valid query with multiple statements",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT id, name FROM users WHERE active = true
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "SELECT id, name FROM users WHERE active = true",
			},
			want: true,
		},
		{
			name: "valid INSERT query",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    INSERT INTO users \(name, email\) VALUES \('John', 'john@example\.com'\)
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
			},
			want: true,
		},
		{
			name: "valid UPDATE query",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    UPDATE users SET active = false WHERE last_login < NOW\(\) - INTERVAL '1 year'
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "UPDATE users SET active = false WHERE last_login < NOW() - INTERVAL '1 year'",
			},
			want: true,
		},
		{
			name: "valid DELETE query",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    DELETE FROM logs WHERE created_at < NOW\(\) - INTERVAL '30 days'
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days'",
			},
			want: true,
		},
		{
			name: "invalid query with syntax error",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT \* FORM users
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).
					WillReturnError(errors.New("syntax error at or near \"FORM\""))
			},
			query: query.Query{
				Query: "SELECT * FORM users",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "syntax error at or near \"FORM\"",
		},
		{
			name: "invalid query with missing table",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT \* FROM non_existent_table
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).
					WillReturnError(errors.New("relation \"non_existent_table\" does not exist"))
			},
			query: query.Query{
				Query: "SELECT * FROM non_existent_table",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "relation \"non_existent_table\" does not exist",
		},
		{
			name: "invalid query with wrong column reference",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT invalid_column FROM users
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).
					WillReturnError(errors.New("column \"invalid_column\" does not exist"))
			},
			query: query.Query{
				Query: "SELECT invalid_column FROM users",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "column \"invalid_column\" does not exist",
		},
		{
			name: "query with CTE (WITH clause)",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    WITH active_users AS \(SELECT \* FROM users WHERE active = true\) SELECT \* FROM active_users
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
			},
			want: true,
		},
		{
			name: "query with JOIN",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT u\.name, p\.title FROM users u JOIN posts p ON u\.id = p\.user_id
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
			},
			want: true,
		},
		{
			name: "invalid query with broken SQL",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    some broken query
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).
					WillReturnError(errors.New("syntax error"))
			},
			query: query.Query{
				Query: "some broken query",
			},
			want:         false,
			wantErr:      true,
			errorMessage: "syntax error",
		},
		{
			name: "query with multiline formatting",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT
        id,
        name,
        email
    FROM users
    WHERE active = true
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: `SELECT
        id,
        name,
        email
    FROM users
    WHERE active = true`,
			},
			want: true,
		},
		{
			name: "CREATE TABLE query validation",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    CREATE TABLE test_table \(id SERIAL PRIMARY KEY, name VARCHAR\(100\)\)
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "CREATE TABLE test_table (id SERIAL PRIMARY KEY, name VARCHAR(100))",
			},
			want: true,
		},
		{
			name: "ALTER TABLE query validation",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    ALTER TABLE users ADD COLUMN age INTEGER
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "ALTER TABLE users ADD COLUMN age INTEGER",
			},
			want: true,
		},
		{
			name: "DROP TABLE query validation",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    DROP TABLE IF EXISTS temp_table
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "DROP TABLE IF EXISTS temp_table",
			},
			want: true,
		},
		{
			name: "query with special characters in strings",
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "result"},
				).AddRow("DO")
				expectedQuery := `DO \$TEST\$ BEGIN RETURN;
    SELECT \* FROM users WHERE name = 'O''Brien' AND email LIKE '%@example\.com'
END; \$TEST\$;`
				mock.ExpectQuery(expectedQuery).WillReturnRows(rows)
			},
			query: query.Query{
				Query: "SELECT * FROM users WHERE name = 'O''Brien' AND email LIKE '%@example.com'",
			},
			want: true,
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
								Name:    "table1",
								Columns: []*ansisql.DBColumn{},
							},
							{
								Name:    "table2",
								Columns: []*ansisql.DBColumn{},
							},
						},
					},
					{
						Name: "schema2",
						Tables: []*ansisql.DBTable{
							{
								Name:    "table2",
								Columns: []*ansisql.DBColumn{},
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

func TestDB_BuildTableExistsQuery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		c           *Client
		tableName   string
		wantQuery   string
		wantErr     bool
		errContains string
	}{
		{
			name:        "invalid format - empty component",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   ".test_table",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.test_table' given",
		},
		{
			name:        "invalid format - empty component 2",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   ".",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '.' given",
		},
		{
			name:        "invalid format - empty table name",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   "",
			wantQuery:   "",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, '' given",
		},
		{
			name:        "invalid format - too many components",
			c:           &Client{config: &Config{Database: "test_db"}},
			tableName:   "a.b.c.d",
			wantErr:     true,
			errContains: "table name must be in format schema.table or table, 'a.b.c.d' given",
		},
		{
			name:      "valid schema.table format",
			c:         &Client{config: &Config{Database: "test_db"}},
			tableName: "test_table",
			wantQuery: "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format",
			c:         &Client{config: &Config{Database: "test_db"}},
			tableName: "test_schema.test_table",
			wantQuery: "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = 'test_schema' AND tablename = 'test_table'",
			wantErr:   false,
		},
		{
			name:      "valid schema.table format with mixed case",
			c:         &Client{config: &Config{Database: "test_db"}},
			tableName: "TestSchema.TestTable",
			wantQuery: "SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname = 'TestSchema' AND tablename = 'TestTable'",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotQuery, err := tt.c.BuildTableExistsQuery(tt.tableName)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantQuery, gotQuery)
		})
	}
}

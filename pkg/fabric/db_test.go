package fabric

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const expectedCurrentDatabaseColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedWarehouseColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM [warehouse].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

const expectedArchiveColumnsQuery = `
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM [archive].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
ORDER BY ORDINAL_POSITION;
`

func TestDB_GetColumns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		databaseName   string
		tableName      string
		mockConnection func(mock sqlmock.Sqlmock)
		want           []*ansisql.DBColumn
		wantErr        string
	}{
		{
			name:         "fetches columns for schema table",
			databaseName: "warehouse",
			tableName:    "sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).
						AddRow("id", "int", "NO", nil, int64(10), int64(0)).
						AddRow("name", "nvarchar", "YES", int64(255), nil, nil).
						AddRow("amount", "decimal", "YES", nil, int64(18), int64(2)).
						AddRow("payload", "varbinary", "YES", int64(-1), nil, nil))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "int", Nullable: false, PrimaryKey: false, Unique: false},
				{Name: "name", Type: "nvarchar(255)", Nullable: true, PrimaryKey: false, Unique: false},
				{Name: "amount", Type: "decimal(18,2)", Nullable: true, PrimaryKey: false, Unique: false},
				{Name: "payload", Type: "varbinary(max)", Nullable: true, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "bare table defaults to dbo schema",
			databaseName: "warehouse",
			tableName:    "orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("dbo", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).AddRow("id", "bigint", "NO", nil, int64(19), int64(0)))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "bigint", Nullable: false, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "three-part table name scopes lookup to table database",
			databaseName: "warehouse",
			tableName:    "archive.sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedArchiveColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnRows(sqlmock.NewRows([]string{
						"COLUMN_NAME",
						"DATA_TYPE",
						"IS_NULLABLE",
						"CHARACTER_MAXIMUM_LENGTH",
						"NUMERIC_PRECISION",
						"NUMERIC_SCALE",
					}).AddRow("id", "bigint", "NO", nil, int64(19), int64(0)))
			},
			want: []*ansisql.DBColumn{
				{Name: "id", Type: "bigint", Nullable: false, PrimaryKey: false, Unique: false},
			},
		},
		{
			name:         "query error",
			databaseName: "warehouse",
			tableName:    "sales.orders",
			mockConnection: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery(expectedWarehouseColumnsQuery).
					WithArgs("sales", "orders").
					WillReturnError(errors.New("metadata failed"))
			},
			wantErr: "failed to query columns for table 'warehouse.sales.orders'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			if tt.mockConnection != nil {
				tt.mockConnection(mock)
			}

			db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
			got, err := db.GetColumns(t.Context(), tt.databaseName, tt.tableName)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_GetColumnsForTable(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedCurrentDatabaseColumnsQuery).
		WithArgs("finance", "payments").
		WillReturnRows(sqlmock.NewRows([]string{
			"COLUMN_NAME",
			"DATA_TYPE",
			"IS_NULLABLE",
			"CHARACTER_MAXIMUM_LENGTH",
			"NUMERIC_PRECISION",
			"NUMERIC_SCALE",
		}).AddRow("payment_id", "uniqueidentifier", "NO", nil, nil, nil))

	db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock")}
	got, err := db.GetColumnsForTable(t.Context(), "finance", "payments")
	require.NoError(t, err)
	assert.Equal(t, []*ansisql.DBColumn{
		{Name: "payment_id", Type: "uniqueidentifier", Nullable: false, PrimaryKey: false, Unique: false},
	}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

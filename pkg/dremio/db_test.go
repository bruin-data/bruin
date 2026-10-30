package dremio

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const expectedDatabaseSummaryQuery = `
SELECT t.table_schema, t.table_name, t.table_type, v.view_definition
FROM information_schema."tables" t
LEFT JOIN information_schema.views v
    ON t.table_schema = v.table_schema AND t.table_name = v.table_name
WHERE t.table_schema NOT IN ('information_schema', 'sys', 'INFORMATION_SCHEMA')
`

func TestClient_GetDatabaseSummary(t *testing.T) {
	t.Parallel()

	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()

	mock.ExpectQuery(expectedDatabaseSummaryQuery).
		WillReturnRows(sqlmock.NewRows([]string{"table_schema", "table_name", "table_type", "view_definition"}).
			AddRow("analytics", "events", "TABLE", nil).
			AddRow("analytics", "active_users", "VIEW", "SELECT * FROM analytics.users WHERE active"))

	client := &Client{
		connection: mockDB,
		config:     Config{Database: "warehouse"},
	}

	got, err := client.GetDatabaseSummary(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "warehouse", got.Name)
	require.Len(t, got.Schemas, 1)
	require.Len(t, got.Schemas[0].Tables, 2)
	assert.Equal(t, ansisql.DBTableTypeTable, got.Schemas[0].Tables[0].Type)
	assert.Equal(t, ansisql.DBTableTypeView, got.Schemas[0].Tables[1].Type)
	assert.Equal(t, "SELECT * FROM analytics.users WHERE active", got.Schemas[0].Tables[1].ViewDefinition)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFolderPathForAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset string
		want  string
	}{
		{name: "folder.table", asset: "my_folder.my_table", want: "my_folder"},
		{name: "deeper paths are not supported (flat only)", asset: "my_source.my_folder.my_table", want: ""},
		{name: "bare table has no folder", asset: "my_table", want: ""},
		{name: "empty name", asset: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, folderPathForAsset(tt.asset))
		})
	}
}

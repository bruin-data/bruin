package vertica

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*DB)(nil)

func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	const anySchema = `SELECT COUNT(*) FROM (
SELECT table_schema, table_name FROM v_catalog.tables
UNION ALL
SELECT table_schema, table_name FROM v_catalog.views
UNION ALL
SELECT table_schema, table_name FROM v_catalog.system_tables
) relations
WHERE LOWER(table_name) = LOWER(?)`
	const inSchema = anySchema + " AND LOWER(table_schema) = LOWER(?)"

	tests := []struct {
		name  string
		table string
		query string
		args  []driver.Value
		count int64
		want  bool
	}{
		{name: "schema.table", table: "Analytics.Events", query: inSchema, args: []driver.Value{"Events", "Analytics"}, count: 1, want: true},
		{name: "a one-part name counts in every schema", table: "events", query: anySchema, args: []driver.Value{"events"}, count: 1, want: true},
		{name: "missing table or schema", table: "analytics.events", query: inSchema, args: []driver.Value{"events", "analytics"}, want: false},
		{name: "missing one-part table", table: "events", query: anySchema, args: []driver.Value{"events"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			mock.ExpectQuery(tt.query).WithArgs(tt.args...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.count))

			db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock"), config: &Config{}}
			got, err := db.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDB_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	for _, table := range []string{"", "db.schema.table", "analytics.", `"analytics".events`, "analytics. events", "my-schema.events"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock"), config: &Config{}}
			_, err = db.RelationExists(context.Background(), table)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()
		mock.ExpectQuery("v_catalog").WillReturnError(errors.New("permission denied"))

		db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock"), config: &Config{}}
		_, err = db.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "permission denied")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

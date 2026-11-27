package starrocks

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

const (
	catalogQuery   = "SELECT CATALOG()"
	defaultCatalog = "default_catalog"
)

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		table string
		query string
		count int64
		want  bool
	}{
		{
			name:  "database.table",
			table: "Analytics.Events",
			query: "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER('Events') AND LOWER(table_schema) = LOWER('Analytics')",
			count: 1,
			want:  true,
		},
		{
			name:  "a one-part name resolves in the default database",
			table: "events",
			query: "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER('events') AND LOWER(table_schema) = LOWER(DATABASE())",
			count: 1,
			want:  true,
		},
		{
			name:  "missing table or database",
			table: "analytics.daily-events",
			query: "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER('daily-events') AND LOWER(table_schema) = LOWER('analytics')",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery(catalogQuery).WillReturnRows(sqlmock.NewRows([]string{"catalog"}).AddRow(defaultCatalog))
			mock.ExpectQuery(tt.query).WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.count))

			client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	for _, table := range []string{"", "catalog.analytics.events", "analytics.", "analytics..events", "`analytics`.events", "analytics. events", `analytics.ev'ents`, `analytics.ev\ents`} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
			_, err = client.RelationExists(context.Background(), table)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(catalogQuery).WillReturnRows(sqlmock.NewRows([]string{"catalog"}).AddRow(defaultCatalog))
		mock.ExpectQuery("information_schema.tables").WillReturnError(errors.New("access denied"))

		client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
		_, err = client.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "access denied")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("sessions in another catalog are not checked", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(catalogQuery).WillReturnRows(sqlmock.NewRows([]string{"catalog"}).AddRow("lakehouse"))

		client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
		exists, err := client.RelationExists(context.Background(), "analytics.events")
		require.NoError(t, err)
		require.True(t, exists, "unchecked catalogs keep the incremental load")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("catalog lookup error", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectQuery(catalogQuery).WillReturnError(errors.New("unknown function"))

		client := &Client{conn: sqlx.NewDb(db, "sqlmock")}
		_, err = client.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "unknown function")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

package mysql

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

var _ autocreate.RelationChecker = (*Client)(nil)

type otherMySQLConfig struct{}

func (otherMySQLConfig) GetIngestrURI() string     { return "" }
func (otherMySQLConfig) ToDBConnectionURI() string { return "" }

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	const (
		qualified = "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?) AND LOWER(table_schema) = LOWER(?)"
		current   = "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?) AND LOWER(table_schema) = LOWER(DATABASE())"
	)

	tests := []struct {
		name     string
		config   MySQLConfig
		table    string
		query    string
		args     []any
		count    int64
		want     bool
		readOnly bool
	}{
		{name: "database.table", table: "Analytics.Events", query: qualified, args: []any{"Events", "Analytics"}, count: 1, want: true},
		{name: "a one-part name resolves in the default database", config: &Config{Database: "app"}, table: "events", query: current, args: []any{"events"}, count: 1, want: true},
		{name: "missing table or database", table: "analytics.events", query: qualified, args: []any{"events", "analytics"}, want: false},
		{name: "names may use $ and non-ASCII letters", table: "an$lytics.événements", query: qualified, args: []any{"événements", "an$lytics"}, want: false},
		{name: "read-only connections check inside a read-only transaction", config: Config{ReadOnly: true}, table: "analytics.events", query: qualified, args: []any{"events", "analytics"}, count: 1, want: true, readOnly: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()

			config := tt.config
			if config == nil {
				config = Config{}
			}
			client := &Client{conn: sqlx.NewDb(db, "sqlmock"), config: config}

			if tt.readOnly {
				mock.ExpectBegin()
			}
			args := make([]driver.Value, 0, len(tt.args))
			for _, arg := range tt.args {
				args = append(args, arg)
			}
			mock.ExpectQuery(tt.query).WithArgs(args...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.count))
			if tt.readOnly {
				mock.ExpectRollback()
			}

			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	for _, table := range []string{"", "a.b.c", "analytics.", "`analytics`.events", `"events"`, "analytics. events", "my-db.events"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			client := &Client{conn: sqlx.NewDb(db, "sqlmock"), config: Config{}}

			_, err = client.RelationExists(context.Background(), table)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("other MySQL-compatible connections are not checked", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		client := &Client{conn: sqlx.NewDb(db, "sqlmock"), config: otherMySQLConfig{}}

		exists, err := client.RelationExists(context.Background(), "analytics.events")
		require.NoError(t, err)
		assert.True(t, exists, "unchecked connections keep the incremental load")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()
		client := &Client{conn: sqlx.NewDb(db, "sqlmock"), config: Config{}}
		mock.ExpectQuery("information_schema.tables").WillReturnError(errors.New("access denied"))

		_, err = client.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "access denied")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

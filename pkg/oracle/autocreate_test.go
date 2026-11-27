package oracle

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	const (
		qualified = "SELECT COUNT(*) FROM all_objects WHERE object_name = UPPER(:1) AND owner = UPPER(:2) " +
			"AND object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SYNONYM')"
		current = "SELECT COUNT(*) FROM all_objects WHERE object_name = UPPER(:1) " +
			"AND object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SYNONYM') " +
			"AND (owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') OR (owner = 'PUBLIC' AND object_type = 'SYNONYM'))"
	)

	tests := []struct {
		name  string
		table string
		query string
		args  []driver.Value
		count int64
		want  bool
	}{
		{name: "schema.table is folded to upper case by the query", table: "analytics.Events", query: qualified, args: []driver.Value{"Events", "analytics"}, count: 1, want: true},
		{name: "a one-part name resolves in the current schema or a public synonym", table: "events", query: current, args: []driver.Value{"events"}, count: 1, want: true},
		{name: "missing table or schema", table: "analytics.events", query: qualified, args: []driver.Value{"events", "analytics"}, want: false},
		{name: "missing one-part table", table: "events", query: current, args: []driver.Value{"events"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			mock.ExpectQuery(tt.query).WithArgs(tt.args...).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(tt.count))

			client := &Client{conn: mockDB}
			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestClient_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	for _, table := range []string{"", "a.b.c", "analytics.", `"analytics".events`, "analytics. events", "my-schema.events"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer mockDB.Close()

			client := &Client{conn: mockDB}
			_, err = client.RelationExists(context.Background(), table)
			require.Error(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()
		mock.ExpectQuery("all_objects").WillReturnError(errors.New("ORA-12541: no listener"))

		client := &Client{conn: mockDB}
		_, err = client.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "ORA-12541")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

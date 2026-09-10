package snowflake

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestDBReadOnly(t *testing.T) {
	t.Parallel()

	methods := []struct {
		name    string
		run     func(context.Context, *DB, *query.Query) error
		explain bool
	}{
		{name: "Select", run: func(ctx context.Context, db *DB, q *query.Query) error { _, err := db.Select(ctx, q); return err }},
		{name: "SelectOnlyLastResult", run: func(ctx context.Context, db *DB, q *query.Query) error {
			_, err := db.SelectOnlyLastResult(ctx, q)
			return err
		}},
		{name: "SelectWithSchema", run: func(ctx context.Context, db *DB, q *query.Query) error {
			_, err := db.SelectWithSchema(ctx, q)
			return err
		}},
		{name: "RunQueryWithoutResult", run: func(ctx context.Context, db *DB, q *query.Query) error { return db.RunQueryWithoutResult(ctx, q) }},
		{name: "IsValid", explain: true, run: func(ctx context.Context, db *DB, q *query.Query) error { _, err := db.IsValid(ctx, q); return err }},
		{name: "DryRunQuery", explain: true, run: func(ctx context.Context, db *DB, q *query.Query) error { _, err := db.DryRunQuery(ctx, q); return err }},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			t.Parallel()

			for _, sql := range []string{"DELETE FROM t", "SELECT 1; DROP TABLE t", "SELECT FROM", "SELECT SYSTEM$CANCEL_QUERY('id')", "SELECT TIME_TO_STR(1, 'x')", "SELECT READ_CSV('x')", "SELECT 1; SELECT READ_CSV('x')", `SELECT "ABS"(1)`, "SELECT IDENTIFIER('my_udf')(1)"} {
				db := &DB{config: &Config{ReadOnly: true}, connect: func(context.Context) (*sqlx.DB, error) {
					t.Fatal("read-only validation must reject the query before connecting")
					return nil, nil
				}}
				require.ErrorContains(t, method.run(t.Context(), db, &query.Query{Query: sql}), "read-only")
			}
			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()
			db := &DB{config: &Config{ReadOnly: true}, conn: sqlx.NewDb(mockDB, "sqlmock")}
			sql := "SELECT 1"
			if method.explain {
				sql = "EXPLAIN SELECT 1;"
			}
			mock.ExpectQuery(sql).WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(1))
			require.NoError(t, method.run(t.Context(), db, &query.Query{Query: "SELECT 1"}))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestDBReadOnlyDisabledAllowsWrites(t *testing.T) {
	t.Parallel()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer mockDB.Close()
	db := &DB{config: &Config{}, conn: sqlx.NewDb(mockDB, "sqlmock")}
	mock.ExpectQuery("DELETE FROM t").WillReturnRows(sqlmock.NewRows([]string{"rows"}).AddRow(1))
	require.NoError(t, db.RunQueryWithoutResult(t.Context(), &query.Query{Query: "DELETE FROM t"}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBReadOnlyExplainVariableDefinitions(t *testing.T) {
	t.Parallel()

	db := &DB{config: &Config{ReadOnly: true}}
	_, err := db.IsValid(t.Context(), &query.Query{Query: "SELECT 1", VariableDefinitions: []string{"DELETE FROM t"}})
	require.ErrorContains(t, err, "read-only")
}

func TestReadOnlyIngestrURI(t *testing.T) {
	t.Parallel()
	_, err := (Config{ReadOnly: true}).GetIngestrURI()
	require.ErrorContains(t, err, "read-only")
}

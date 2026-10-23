package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyConnectionQuery(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"success", "query error", "row error", "early close", "canceled context"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly})
			expected := mock.ExpectQuery("SELECT").WithArgs(pgx.QueryExecModeExec, 42)
			if name == "query error" {
				expected.WillReturnError(errors.New("query failed"))
			} else {
				rows := pgxmock.NewRows([]string{"value"}).AddRow(42).AddRow(43)
				if name == "row error" {
					rows.RowError(0, errors.New("row failed"))
				}
				expected.WillReturnRows(rows).RowsWillBeClosed()
			}
			mock.ExpectRollback()
			conn := &readOnlyConnection{pool: mock}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rows, err := conn.Query(ctx, "SELECT $1", 42)
			if name == "query error" {
				require.EqualError(t, err, "query failed")
			} else {
				require.NoError(t, err)
				if name == "canceled context" {
					cancel()
				}
				if name == "success" || name == "row error" {
					for rows.Next() {
						_, err := rows.Values()
						if name == "row error" {
							require.EqualError(t, err, "row failed")
							break
						}
						require.NoError(t, err)
					}
					if name == "row error" {
						require.EqualError(t, rows.Err(), "row failed")
					} else {
						require.NoError(t, rows.Err())
					}
				}
				rows.Close()
				rows.Close()
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadOnlyConnectionExec(t *testing.T) {
	t.Parallel()
	for _, fails := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "write rejected"}[fails], func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly})
			expected := mock.ExpectQuery("DELETE FROM items").WithArgs(pgx.QueryExecModeExec)
			if fails {
				expected.WillReturnError(errors.New("read-only transaction"))
			} else {
				expected.WillReturnRows(pgxmock.NewRows([]string{"value"}).AddRow(1)).RowsWillBeClosed()
			}
			mock.ExpectRollback()
			client := &Client{connection: &readOnlyConnection{pool: mock}}
			err = client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "DELETE FROM items"})
			if fails {
				require.EqualError(t, err, "read-only transaction")
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadOnlyConnectionBeginFailure(t *testing.T) {
	t.Parallel()
	for _, exec := range []bool{false, true} {
		mock, err := pgxmock.NewPool()
		require.NoError(t, err)
		defer mock.Close()
		mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly}).WillReturnError(errors.New("unavailable"))
		conn := &readOnlyConnection{pool: mock}
		if exec {
			_, err = conn.Exec(t.Context(), "SELECT 1")
		} else {
			rows, queryErr := conn.Query(t.Context(), "SELECT 1")
			if rows != nil {
				rows.Close()
			}
			err = queryErr
		}
		require.EqualError(t, err, "failed to start read-only transaction: unavailable")
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

type unreachableConnection struct {
	t *testing.T
}

//nolint:ireturn
func (c unreachableConnection) Query(context.Context, string, ...any) (pgx.Rows, error) {
	c.t.Fatal("read-only validation must reject the query before it reaches the database")
	return nil, nil
}

func (c unreachableConnection) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	c.t.Fatal("read-only validation must reject the query before it reaches the database")
	return pgconn.CommandTag{}, nil
}

func TestClientReadOnlyValidation(t *testing.T) {
	t.Parallel()

	methods := []struct {
		name     string
		run      func(context.Context, *Client, *query.Query) error
		expected func(string) string
	}{
		{name: "Select", run: func(ctx context.Context, c *Client, q *query.Query) error { _, err := c.Select(ctx, q); return err }},
		{name: "SelectWithSchema", run: func(ctx context.Context, c *Client, q *query.Query) error {
			_, err := c.SelectWithSchema(ctx, q)
			return err
		}},
		{name: "RunQueryWithoutResult", run: func(ctx context.Context, c *Client, q *query.Query) error { return c.RunQueryWithoutResult(ctx, q) }},
		{name: "IsValid", run: func(ctx context.Context, c *Client, q *query.Query) error { _, err := c.IsValid(ctx, q); return err }, expected: func(sql string) string {
			return fmt.Sprintf("DO $TEST$ BEGIN RETURN;\n    %s\nEND; $TEST$;", sql)
		}},
		{name: "DryRunQuery", run: func(ctx context.Context, c *Client, q *query.Query) error {
			_, err := c.DryRunQuery(ctx, q)
			return err
		}, expected: func(sql string) string {
			return "EXPLAIN " + sql
		}},
	}
	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			t.Parallel()

			for _, sql := range []string{
				"DELETE FROM t",
				"SELECT dblink_exec('dbname=db', 'DELETE FROM t')",
				"SELECT public.dblink_exec('dbname=db', 'DELETE FROM t')",
				`SELECT "DBLINK_EXEC"('dbname=db', 'DELETE FROM t')`,
				"SELECT * FROM dblink('dbname=db', 'DELETE FROM t RETURNING 1') AS x(a int)",
				"SELECT a FROM t WHERE a IN (SELECT dblink_connect('c', 'dbname=db'))",
				"SELECT pg_background_launch('DELETE FROM t')",
			} {
				client := &Client{connection: unreachableConnection{t: t}, readOnlyDialect: "postgres"}
				require.ErrorContains(t, method.run(t.Context(), client, &query.Query{Query: sql}), "read-only", sql)
			}

			mock, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mock.Close()
			client := &Client{connection: &readOnlyConnection{pool: mock}, readOnlyDialect: "postgres"}
			for _, sql := range []string{
				"SELECT 1",
				"SELECT date_trunc('day', now()), count(*) FROM t GROUP BY 1",
				"SELECT NULLIF(BTRIM(utm_term), '') FROM t",
				"SELECT table_name FROM information_schema.tables WHERE has_table_privilege(format('%I.%I', table_schema, table_name), 'SELECT')",
				"SELECT unaccent(name), make_date(2024, 1, 1), regexp_match(name, 'a') FROM t",
				"SELECT jsonb_build_object('a', 1), age(now(), ts) FROM t",
				"SELECT my_udf(1), analytics.my_udf(2)",
				"SELECT pg_advisory_xact_lock(1)",
				"EXPLAIN SELECT a FROM t",
			} {
				expectedSQL := sql
				if method.expected != nil {
					expectedSQL = method.expected(sql)
				}
				mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly})
				mock.ExpectQuery(expectedSQL).WithArgs(pgx.QueryExecModeExec).WillReturnRows(pgxmock.NewRows([]string{"value"}).AddRow(1))
				mock.ExpectRollback()
				require.NoError(t, method.run(t.Context(), client, &query.Query{Query: sql}))
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadOnlyDialect(t *testing.T) {
	t.Parallel()
	require.Empty(t, readOnlyDialect(Config{}))
	require.Equal(t, "postgres", readOnlyDialect(Config{ReadOnly: true}))
	require.Empty(t, readOnlyDialect(RedShiftConfig{}))
	require.Equal(t, "redshift", readOnlyDialect(RedShiftConfig{ReadOnly: true}))
}

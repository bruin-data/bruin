package postgres

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyConnectionIntegration(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("BRUIN_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("BRUIN_TEST_POSTGRES_DSN is not set")
	}
	ctx := t.Context()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	defer pool.Close()
	table := fmt.Sprintf("bruin_readonly_%d", time.Now().UnixNano())
	_, err = pool.Exec(ctx, "CREATE TABLE "+table+" (id int)")
	require.NoError(t, err)
	defer func() {
		_, err := pool.Exec(ctx, "DROP TABLE "+table)
		require.NoError(t, err)
	}()
	_, err = pool.Exec(ctx, "INSERT INTO "+table+" VALUES (1)")
	require.NoError(t, err)
	client := &Client{connection: &readOnlyConnection{pool: pool}}
	for _, method := range []string{"select", "schema", "exec"} {
		run := func(sql string) error {
			q := &query.Query{Query: sql}
			switch method {
			case "select":
				_, err := client.Select(ctx, q)
				return err
			case "schema":
				_, err := client.SelectWithSchema(ctx, q)
				return err
			default:
				return client.RunQueryWithoutResult(ctx, q)
			}
		}
		for _, sql := range []string{
			"INSERT INTO " + table + " VALUES (2)",
			"DROP TABLE " + table,
			"SELECT 1; SELECT 2",
			"COMMIT; INSERT INTO " + table + " VALUES (3)",
			"SET TRANSACTION READ WRITE; INSERT INTO " + table + " VALUES (4)",
		} {
			require.Error(t, run(sql), "%s: %s", method, sql)
			require.NoError(t, run("SELECT * FROM "+table), method)
		}
	}
	var count int
	require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count))
	require.Equal(t, 1, count)
}

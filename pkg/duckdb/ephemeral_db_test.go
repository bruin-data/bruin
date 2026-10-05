//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestConnectionReuseDatabaseAndSessionIsolation(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(Config{Path: ":memory:"})
	require.NoError(t, err)

	// An in-memory attachment cannot survive reopening the engine. It must
	// remain visible across all three operation APIs, as lakehouse catalogs do.
	session, release, err := conn.openADBC(ctx, "")
	require.NoError(t, err)
	err = execADBCStatement(ctx, session, "ATTACH ':memory:' AS attached; CREATE TABLE attached.values_table AS SELECT 17 AS n")
	release()
	require.NoError(t, err)
	rows, err := conn.QueryContext(ctx, "SELECT n FROM attached.values_table")
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var n int
	require.NoError(t, rows.Scan(&n))
	require.Equal(t, 17, n)
	require.NoError(t, rows.Err())
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT n + 4 FROM attached.values_table").Scan(&n))
	require.Equal(t, 21, n)

	// Connections must not carry temporary tables or unfinished transactions
	// into the next asset, including when the prior statement failed.
	_, err = conn.ExecContext(ctx, "CREATE TEMP TABLE session_only AS SELECT 1")
	require.NoError(t, err)
	_, err = conn.QueryContext(ctx, "SELECT * FROM session_only")
	require.Error(t, err)
	_, err = conn.ExecContext(ctx, "BEGIN; INSERT INTO attached.values_table VALUES (99); SELECT * FROM missing_table")
	require.Error(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT CAST(SUM(n) AS BIGINT) FROM attached.values_table").Scan(&n))
	require.Equal(t, 17, n)

	cleanup()
	require.Nil(t, conn.lock.database)
}

func TestConnectionReuseAttachmentKeywordsInData(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(Config{Path: ":memory:"})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE TABLE numbers AS SELECT 43 AS n")
	require.NoError(t, err)
	for _, sql := range []string{
		"SELECT 'ATTACH'",
		"SELECT 'it''s DETACH'",
		`SELECT E'it\'s ATTACH'`,
		"SELECT $$; ATTACH ':memory:' AS source;$$",
		"SELECT $sql$DETACH source;$sql$",
		`SELECT 'ok' AS "ATTACH"`,
		"-- ATTACH ':memory:' AS source\nSELECT 'ok'",
		"/* outer /* ATTACH */ DETACH */ SELECT 'ok'",
	} {
		var value string
		require.NoError(t, conn.QueryRowContext(ctx, sql).Scan(&value), sql)
		var n int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM numbers").Scan(&n), sql)
		require.Equal(t, 43, n)
	}
	// A lexical error must not discard the engine either.
	_, err = conn.ExecContext(ctx, "SELECT 'ATTACH")
	require.Error(t, err)
	var n int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM numbers").Scan(&n))
	require.Equal(t, 43, n)
}

func TestHasAttachmentStatement(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		sql  string
		want bool
	}{
		{"ATTACH ':memory:' AS source", true},
		{"SELECT ';'; /* comment */ aTtAcH ':memory:' AS source", true},
		{"SELECT $$; DETACH source;$$; ATTACH ':memory:' AS source", true},
		{"-- comment\n DETACH source", true},
		{"SELECT 1; DETACH source", true},
		{"SELECT 'ATTACH'", false},
		{"SELECT attach FROM numbers", false},
		{"SELECT 1 -- ; ATTACH ':memory:' AS source", false},
	} {
		got, err := hasAttachmentStatement(tt.sql)
		require.NoError(t, err)
		require.Equal(t, tt.want, got, tt.sql)
	}
}

func TestConnectionReuseUserAttachments(t *testing.T) {
	t.Parallel()
	for _, api := range []string{"exec", "query", "row"} {
		t.Run(api, func(t *testing.T) {
			t.Parallel()
			ctx, cleanup := WithConnectionReuse(t.Context())
			cfg := Config{Path: filepath.Join(t.TempDir(), "attachments.db")}
			t.Cleanup(cleanup)
			conn, err := NewEphemeralConnection(cfg)
			require.NoError(t, err)
			for range 2 {
				var n int
				// Warm the shared engine before each user-managed attachment.
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT 7").Scan(&n))
				sql := "SELECT 1; -- an asset's private source\n aTtAcH ':memory:' AS source; CREATE TABLE source.numbers AS SELECT 19 AS n; CREATE OR REPLACE TABLE main.result AS SELECT n FROM source.numbers; SELECT n FROM main.result"
				switch api {
				case "exec":
					_, err = conn.ExecContext(ctx, sql)
					require.NoError(t, err)
				case "query":
					rows, err := conn.QueryContext(ctx, sql)
					require.NoError(t, err)
					require.True(t, rows.Next())
					require.NoError(t, rows.Scan(&n))
					require.Equal(t, 19, n)
					require.NoError(t, rows.Close())
				case "row":
					require.NoError(t, conn.QueryRowContext(ctx, sql).Scan(&n))
					require.Equal(t, 19, n)
				}
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM result").Scan(&n))
				require.Equal(t, 19, n)
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'source'").Scan(&n))
				require.Zero(t, n, "user attachments must not leak to the next operation")
			}
		})
	}
}

func TestConnectionReuseOverlappingScopes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"idle", "active"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Path: filepath.Join(t.TempDir(), "scopes.db")}
			firstCtx, firstCleanup := WithConnectionReuse(t.Context())
			t.Cleanup(firstCleanup)
			secondCtx, secondCleanup := WithConnectionReuse(t.Context())
			t.Cleanup(secondCleanup)
			conn, err := NewEphemeralConnection(cfg)
			require.NoError(t, err)
			_, err = conn.ExecContext(firstCtx, "CREATE TABLE numbers AS SELECT 37 AS n")
			require.NoError(t, err)
			database := conn.lock.database
			var n int
			require.NoError(t, conn.QueryRowContext(secondCtx, "SELECT n FROM numbers").Scan(&n))
			require.Equal(t, 37, n)
			if mode == "active" {
				_, release, err := conn.openADBC(secondCtx, "")
				require.NoError(t, err)
				firstCleanup()
				release()
			} else {
				firstCleanup()
			}
			require.Eventually(t, func() bool {
				return conn.lock.scopes.Load() == 1
			}, 5*time.Second, time.Millisecond)
			conn.lock.RLock()
			remaining := conn.lock.database
			conn.lock.RUnlock()
			require.Same(t, database, remaining, "one run must not close another run's cached engine")
			require.NoError(t, conn.QueryRowContext(secondCtx, "SELECT n FROM numbers").Scan(&n))
			require.Equal(t, 37, n)
			secondCleanup()
			secondCleanup()
			require.Nil(t, conn.lock.database)
			require.Zero(t, conn.lock.scopes.Load())
		})
	}
}

func TestConnectionReuseAllowsConcurrentClients(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	cfg := Config{Path: filepath.Join(t.TempDir(), "parallel.db")}
	t.Cleanup(cleanup) // Close the file before TempDir cleanup, including on Windows.
	first, err := NewClient(cfg)
	require.NoError(t, err)
	second, err := NewClient(cfg)
	require.NoError(t, err)
	conn := first.connection.(*EphemeralConnection)
	session, release, err := conn.openADBC(ctx, "")
	require.NoError(t, err)
	var once sync.Once
	defer once.Do(release)
	require.NoError(t, execADBCStatement(ctx, session, "CREATE TABLE numbers AS SELECT 13 AS n"))

	// Keep one session checked out while another client writes and reads.
	// This fails if Client still takes the old exclusive per-query lock, or
	// if the second client attempts to open an independent engine instance.
	done := make(chan error, 1)
	var rows [][]any
	go func() {
		err := second.RunQueryWithoutResult(ctx, &query.Query{Query: "INSERT INTO numbers VALUES (29)"})
		if err == nil {
			rows, err = second.Select(ctx, &query.Query{Query: "SELECT CAST(SUM(n) AS BIGINT) FROM numbers"})
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(42)}}, rows)
	case <-time.After(10 * time.Second):
		once.Do(release)
		<-done
		t.Fatal("second client waited for the first SQL session to close")
	}
}

func TestConnectionReuseCleanupWithActiveSession(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(Config{Path: ":memory:"})
	require.NoError(t, err)
	session, release, err := conn.openADBC(ctx, "")
	require.NoError(t, err)
	var once sync.Once
	defer once.Do(release)

	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		once.Do(release)
		<-done
		t.Fatal("run cleanup waited for an active DuckDB worker")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, conn.QueryRowContext(ctx, "SELECT 1").Err(), context.Canceled)
	// Cleanup must not close a database underneath the active worker.
	require.NoError(t, execADBCStatement(t.Context(), session, "SELECT 17"))
	once.Do(release)
	require.Eventually(t, func() bool {
		conn.lock.RLock()
		defer conn.lock.RUnlock()
		return conn.lock.database == nil
	}, 5*time.Second, time.Millisecond)
}

func TestConnectionReuseExternalProcessHandoff(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	cfg := Config{Path: filepath.Join(t.TempDir(), "handoff.db")}
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(cfg)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE TABLE numbers AS SELECT 7 AS n")
	require.NoError(t, err)

	runChild := func() ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDuckDBProcessHelper$")
		cmd.Env = append(os.Environ(), "BRUIN_TEST_DUCKDB_PATH="+cfg.Path)
		return cmd.CombinedOutput()
	}
	// Prove the parent really retains the OS file lock, then prove the same
	// handoff used by ingestr releases it for a different process.
	output, err := runChild()
	require.Error(t, err)
	// DuckDB reports a different file-lock message on Windows and Unix.
	// The same child must succeed once the parent releases the file below.
	require.Contains(t, string(output), "IO Error")
	unlock := LockDatabases(cfg.GetIngestrURI(), cfg.Path)
	output, err = runChild()
	unlock()
	require.NoError(t, err, "%s", output)

	var total int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT CAST(SUM(n) AS BIGINT) FROM numbers").Scan(&total))
	require.Equal(t, 48, total)
	cleanup()
	output, err = runChild()
	require.NoError(t, err, "run cleanup must release the file lock: %s", output)
}

func TestDuckDBProcessHelper(t *testing.T) {
	t.Parallel()
	path := os.Getenv("BRUIN_TEST_DUCKDB_PATH")
	if path == "" {
		return
	}
	conn, err := NewEphemeralConnection(Config{Path: path})
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO numbers VALUES (41)")
	require.NoError(t, err)
}

func TestConnectionReuseReadOnlyConfiguration(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	cfg := Config{Path: filepath.Join(t.TempDir(), "readonly.db")}
	t.Cleanup(cleanup)
	writer, err := NewEphemeralConnection(cfg)
	require.NoError(t, err)
	_, err = writer.ExecContext(ctx, "CREATE TABLE numbers AS SELECT 5 AS n")
	require.NoError(t, err)
	cfg.ReadOnly = true
	reader, err := NewEphemeralConnection(cfg)
	require.NoError(t, err)
	var n int
	require.NoError(t, reader.QueryRowContext(ctx, "SELECT n FROM numbers").Scan(&n))
	require.Equal(t, 5, n)
	_, err = reader.ExecContext(ctx, "INSERT INTO numbers VALUES (8)")
	require.ErrorContains(t, err, "read-only")
	_, err = writer.ExecContext(ctx, "INSERT INTO numbers VALUES (12)")
	require.NoError(t, err)
	require.NoError(t, writer.QueryRowContext(ctx, "SELECT CAST(SUM(n) AS BIGINT) FROM numbers").Scan(&n))
	require.Equal(t, 17, n)
}

func TestConnectionReuseCatalogPolicy(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name  string
		cfg   DuckDBConfig
		reuse bool
	}{
		{"native", Config{Path: "data.db"}, true},
		{"motherduck", MotherDuckConfig{Database: "test"}, true},
		{"postgres", Config{Lakehouse: validDuckLakePostgresConfig()}, true},
		{"iceberg", Config{Lakehouse: validIcebergLakehouseConfig()}, true},
		{"duckdb catalog", Config{Lakehouse: &config.LakehouseConfig{
			Format: config.LakehouseFormatDuckLake, Catalog: config.CatalogConfig{Type: config.CatalogTypeDuckDB},
		}}, false},
		{"sqlite catalog", Config{Lakehouse: &config.LakehouseConfig{
			Format: config.LakehouseFormatDuckLake, Catalog: config.CatalogConfig{Type: config.CatalogTypeSQLite},
		}}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn := EphemeralConnection{config: tt.cfg}
			require.Equal(t, tt.reuse, conn.canReuseDatabase())
		})
	}
}

func TestConnectionReuseLakehouseSessionCatalog(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(Config{Path: ":memory:"})
	require.NoError(t, err)
	session, release, err := conn.openADBC(ctx, "")
	require.NoError(t, err)
	err = execADBCStatement(ctx, session, "ATTACH ':memory:' AS ducklake_catalog; CREATE TABLE ducklake_catalog.numbers AS SELECT 31 AS n")
	release()
	require.NoError(t, err)

	// Stand in for an already attached lakehouse without network credentials.
	// USE from initialization does not propagate to newly opened sessions.
	cfg := Config{Path: ":memory:", Lakehouse: &config.LakehouseConfig{Format: config.LakehouseFormatDuckLake}}
	conn.config = cfg
	conn.lock.database.(*cachedDatabase).config = cfg
	for range 2 {
		var n int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM numbers").Scan(&n))
		require.Equal(t, 31, n)
	}
}

func TestConnectionReuseConcurrentSchemaCreation(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	cfg := Config{Path: filepath.Join(t.TempDir(), "schemas.db")}
	t.Cleanup(cleanup)
	const workers = 8
	ready := make(chan struct{})
	done := make(chan error, workers)
	for range workers {
		client, err := NewClient(cfg)
		require.NoError(t, err)
		go func() {
			<-ready
			done <- client.CreateSchemaIfNotExist(ctx, &pipeline.Asset{Name: "shared_schema.asset"})
		}()
	}
	close(ready)
	for range workers {
		require.NoError(t, <-done)
	}
}

func TestConnectionReuseTransactionConflict(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	t.Cleanup(cleanup)
	conn, err := NewEphemeralConnection(Config{Path: ":memory:"})
	require.NoError(t, err)
	session, release, err := conn.openADBC(ctx, "")
	require.NoError(t, err)
	defer release()
	require.NoError(t, execADBCStatement(ctx, session, "CREATE TABLE numbers AS SELECT 10 AS n; BEGIN; UPDATE numbers SET n = 20"))

	_, err = conn.ExecContext(ctx, "UPDATE numbers SET n = 30")
	var conflict *TransactionConflictError
	require.ErrorAs(t, err, &conflict)
	require.ErrorContains(t, err, "retryable")
	require.Error(t, errors.Unwrap(conflict))
	require.NoError(t, execADBCStatement(ctx, session, "COMMIT"))
	_, err = conn.ExecContext(ctx, "UPDATE numbers SET n = n + 7")
	require.NoError(t, err)
	var n int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT n FROM numbers").Scan(&n))
	require.Equal(t, 27, n)
}

func TestTransactionError(t *testing.T) {
	t.Parallel()
	err := errors.New("Transaction conflict: conflicting update")
	var conflict *TransactionConflictError
	require.ErrorAs(t, transactionError(err), &conflict)
	require.ErrorIs(t, conflict, err)
	fileLock := errors.New("IO Error: Could not set lock on file")
	require.Same(t, fileLock, transactionError(fileLock))
	require.NoError(t, transactionError(nil))
}

func BenchmarkConnectionReuse(b *testing.B) {
	for _, reuse := range []bool{false, true} {
		name := "ephemeral"
		if reuse {
			name = "run-scoped"
		}
		b.Run(name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "benchmark.db")
			ctx := b.Context()
			if reuse {
				var cleanup func()
				ctx, cleanup = WithConnectionReuse(ctx)
				b.Cleanup(cleanup)
			}
			conn, err := NewEphemeralConnection(Config{Path: path})
			require.NoError(b, err)
			var n int
			require.NoError(b, conn.QueryRowContext(ctx, "SELECT 17").Scan(&n))
			b.ResetTimer()
			for range b.N {
				require.NoError(b, conn.QueryRowContext(ctx, "SELECT 17").Scan(&n))
				require.Equal(b, 17, n)
			}
		})
	}
}

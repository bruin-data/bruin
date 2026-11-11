//go:build linux || darwin

package postgrescdc_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const testPassword = "bruin-cdc-integration"

// These tests launch the real CLI and its pinned ingestr binary. The explicit
// Make target enables them; ordinary Go test discovery does not start Docker.
//
//nolint:paralleltest,tparallel
func TestPostgresCDC(t *testing.T) {
	if os.Getenv("BRUIN_POSTGRES_CDC_TEST") != "1" {
		t.Skip("run make integration-test-postgres-cdc to enable Docker integration tests")
	}
	binary := bruinBinary(t)
	host := startPostgres(t)

	for _, mode := range []struct {
		name   string
		params map[string]any
	}{
		{"spool_destination", map[string]any{"cdc_snapshot_spool": true, "cdc_wal_staging": "destination"}},
		{"spool_disk", map[string]any{"cdc_snapshot_spool": true, "cdc_wal_staging": "disk"}},
		{"spool_memory", map[string]any{"cdc_snapshot_spool": true, "cdc_wal_staging": "memory"}},
		{"chunk_serial", map[string]any{"cdc_snapshot_chunked": true, "cdc_snapshot_chunk_size": 2}},
		{"chunk_parallel", map[string]any{"cdc_snapshot_chunked": true, "cdc_snapshot_chunk_size": 2, "cdc_snapshot_execution": "parallel", "cdc_wal_staging": "destination"}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f := newFixture(t, host, binary)
			for key, value := range mode.params {
				f.params[key] = value
			}
			if mode.name == "spool_disk" {
				f.params["cdc_wal_spool_dir"] = filepath.Join(f.root, "wal")
			}
			f.params["destination_parallelism"] = 2
			f.params["extract_parallelism"] = 2
			f.params["batch_size"] = 1
			f.params["cdc_binary"] = false
			f.params["cdc_discover_interval"] = "off"
			f.params["no_load_timestamp"] = true
			f.params["no_run_id"] = true
			f.run(t)
			f.assertRows(t, map[int]string{1: "one", 2: "two", 3: "three", 4: "four"})

			execSQL(t, f.source, `UPDATE public.items SET value = 'updated' WHERE id = 1;
				DELETE FROM public.items WHERE id = 2;
				INSERT INTO public.items VALUES (5, 'inserted')`)
			f.run(t)
			f.assertRows(t, map[int]string{1: "updated", 3: "three", 4: "four", 5: "inserted"})
			var metadataColumns int
			require.NoError(t, f.dest.QueryRowContext(t.Context(), `SELECT count(*) FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'items'
				AND column_name IN ('_ingestr_loaded_at', '_ingestr_run_id')`).Scan(&metadataColumns))
			require.Zero(t, metadataColumns)

			execSQL(t, f.source, `TRUNCATE public.items; INSERT INTO public.items VALUES (6, 'after truncate')`)
			f.run(t)
			f.assertRows(t, map[int]string{6: "after truncate"})
		})
	}

	t.Run("batch_metrics", func(t *testing.T) { testBatchMetrics(t, host, binary) })
	t.Run("interrupted_disk_spool", func(t *testing.T) { testInterruptedSnapshot(t, host, binary, false) })
	t.Run("interrupted_chunk", func(t *testing.T) { testInterruptedSnapshot(t, host, binary, true) })
	t.Run("exported_baseline", func(t *testing.T) { testExportedBaseline(t, host, binary) })
	t.Run("stream_shutdown", func(t *testing.T) { testStreamShutdown(t, host, binary) })
}

func testBatchMetrics(t *testing.T, host, binary string) {
	t.Helper()
	f := newFixture(t, host, binary)
	f.params["cdc_snapshot_spool"] = true
	f.params["cdc_wal_staging"] = "memory"
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	f.params["metrics_addr"] = addr

	// Hold the destination write until the metrics have been scraped. Tiny
	// fixtures otherwise finish too quickly to observe a finite run's endpoint.
	release := f.pauseDestination(t)
	run := f.start(t, false)
	f.waitForDestinationGate(t, run)
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		run.requireRunning(t)
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/metrics", nil)
		require.NoError(t, requestErr)
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(response.Body)
		return readErr == nil && response.StatusCode == http.StatusOK &&
			strings.Contains(string(body), "ingestr_replication_slot_retained_wal_bytes{") &&
			strings.Contains(string(body), "ingestr_replication_slot_collection_success{")
	}, time.Minute, 100*time.Millisecond, "batch CDC did not expose slot retention metrics")
	release()
	run.waitSuccess(t)
	f.assertRows(t, map[int]string{1: "one", 2: "two", 3: "three", 4: "four"})
}

func testInterruptedSnapshot(t *testing.T, host, binary string, chunked bool) {
	t.Helper()
	f := newFixture(t, host, binary)
	if chunked {
		f.params["cdc_snapshot_chunked"] = true
		f.params["cdc_snapshot_chunk_size"] = 2
		f.params["cdc_snapshot_execution"] = "parallel"
		f.params["cdc_wal_staging"] = "destination"
	} else {
		f.params["cdc_snapshot_spool"] = true
		f.params["cdc_wal_staging"] = "disk"
		f.params["cdc_wal_spool_dir"] = filepath.Join(f.root, "wal")
	}

	release := f.pauseDestination(t)
	run := f.start(t, false)
	f.waitForDestinationGate(t, run)
	var firstLSN string
	if chunked {
		require.NoError(t, f.dest.QueryRowContext(t.Context(), "SELECT _cdc_lsn FROM public.items WHERE id = 1").Scan(&firstLSN))
	}
	execSQL(t, f.source, `UPDATE public.items SET value = 'concurrent' WHERE id = 4;
		DELETE FROM public.items WHERE id = 2; INSERT INTO public.items VALUES (5, 'during snapshot')`)
	run.interrupt(t)
	f.waitForInactiveSlot(t)
	release()
	if !chunked {
		entries, readErr := os.ReadDir(filepath.Join(f.root, "wal"))
		require.NoError(t, readErr)
		require.NotEmpty(t, entries, "disk spool must survive process interruption")
	}
	f.run(t)
	f.assertRows(t, map[int]string{1: "one", 3: "three", 4: "concurrent", 5: "during snapshot"})
	if chunked {
		var resumedLSN string
		require.NoError(t, f.dest.QueryRowContext(t.Context(), "SELECT _cdc_lsn FROM public.items WHERE id = 1").Scan(&resumedLSN))
		require.Equal(t, firstLSN, resumedLSN, "completed snapshot chunks must not be recopied")
	}
}

func testExportedBaseline(t *testing.T, host, binary string) {
	t.Helper()
	f := newFixture(t, host, binary)
	execSQL(t, f.source, "CREATE PUBLICATION bruin_publication FOR TABLE public.items")
	exporter, err := pgconn.Connect(t.Context(), f.sourceURI+"&replication=database")
	require.NoError(t, err)
	t.Cleanup(func() { _ = exporter.Close(context.Background()) })
	results, err := exporter.Exec(t.Context(), "CREATE_REPLICATION_SLOT "+f.slot+" LOGICAL pgoutput EXPORT_SNAPSHOT").ReadAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Rows, 1)
	require.Len(t, results[0].Rows[0], 4)
	lsn := string(results[0].Rows[0][1])
	snapshot := string(results[0].Rows[0][2])
	require.NotEmpty(t, snapshot)

	execSQL(t, f.source, `UPDATE public.items SET value = 'after export' WHERE id = 1;
		DELETE FROM public.items WHERE id = 2; INSERT INTO public.items VALUES (5, 'after export')`)
	f.params = map[string]any{
		"source_connection": "source", "source_table": "public.items", "destination": "postgres",
		"postgres_snapshot": snapshot, "incremental_strategy": "replace",
	}
	f.run(t)
	f.assertRows(t, map[int]string{1: "one", 2: "two", 3: "three", 4: "four"})
	require.NoError(t, exporter.Close(t.Context()))

	expired := f.start(t, false)
	require.Error(t, expired.wait(t))
	require.Contains(t, expired.output(t), "failed to import postgres snapshot "+snapshot)
	f.assertRows(t, map[int]string{1: "one", 2: "two", 3: "three", 4: "four"})
	f.params = cdcParameters()
	f.params["cdc_publication"] = "bruin_publication"
	f.params["cdc_slot"] = f.slot
	f.params["cdc_snapshot_mode"] = "never"
	f.params["cdc_start_lsn"] = lsn
	f.run(t)
	f.assertRows(t, map[int]string{1: "after export", 3: "three", 4: "four", 5: "after export"})
	execSQL(t, f.source, "UPDATE public.items SET value = 'after adoption' WHERE id = 5")
	f.run(t)
	f.assertRows(t, map[int]string{1: "after export", 3: "three", 4: "four", 5: "after adoption"})
}

func testStreamShutdown(t *testing.T, host, binary string) {
	t.Helper()
	f := newFixture(t, host, binary)
	f.params["cdc_snapshot_spool"] = true
	f.params["cdc_wal_staging"] = "destination"
	f.run(t)
	f.params["stream"] = true
	f.params["page_size"] = 1
	f.params["cdc_stream_flush_interval"] = "1h"
	f.params["cdc_stream_flush_records"] = 1000000
	run := f.start(t, true)
	require.Eventually(t, func() bool {
		run.requireRunning(t)
		var active bool
		return f.source.QueryRowContext(t.Context(), "SELECT active FROM pg_replication_slots WHERE slot_name = $1", f.slot).Scan(&active) == nil && active
	}, time.Minute, 100*time.Millisecond)
	execSQL(t, f.source, "UPDATE public.items SET value = 'streamed' WHERE id = 1")
	// Page size forces the source to emit the update, while destination flush
	// thresholds leave it pending until the stream receives its stop signal.
	require.Eventually(t, func() bool {
		run.requireRunning(t)
		return strings.Contains(run.output(t), "[CDC] Batch 1: 1 rows (total: 1)")
	}, time.Minute, 100*time.Millisecond, "stream did not emit the pending change")
	f.assertRows(t, map[int]string{1: "one", 2: "two", 3: "three", 4: "four"})
	run.interrupt(t)
	f.waitForInactiveSlot(t)
	require.Contains(t, run.output(t), "[STREAM] Final flush: 1 buffered rows")
	require.Contains(t, run.output(t), "Streaming ingestion stopped.")
	f.assertRows(t, map[int]string{1: "streamed", 2: "two", 3: "three", 4: "four"})
	f.params["stream"] = false
	delete(f.params, "cdc_stream_flush_interval")
	delete(f.params, "cdc_stream_flush_records")
	execSQL(t, f.source, "INSERT INTO public.items VALUES (5, 'after shutdown')")
	f.run(t)
	f.assertRows(t, map[int]string{1: "streamed", 2: "two", 3: "three", 4: "four", 5: "after shutdown"})
}

type fixture struct {
	root      string
	binary    string
	sourceURI string
	slot      string
	source    *sql.DB
	dest      *sql.DB
	params    map[string]any
}

func newFixture(t *testing.T, host, binary string) *fixture {
	t.Helper()
	admin := openPostgres(t, postgresURI(host, "postgres"))
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	sourceName, destName := "source_"+suffix, "dest_"+suffix
	execSQL(t, admin, "CREATE DATABASE "+sourceName)
	execSQL(t, admin, "CREATE DATABASE "+destName)
	f := &fixture{
		root: t.TempDir(), binary: binary, sourceURI: postgresURI(host, sourceName), slot: "bruin_" + suffix,
		source: openPostgres(t, postgresURI(host, sourceName)), dest: openPostgres(t, postgresURI(host, destName)),
		params: cdcParameters(),
	}
	f.params["cdc_slot"] = f.slot
	execSQL(t, f.source, `CREATE TABLE public.items (id integer PRIMARY KEY, value text);
		ALTER TABLE public.items REPLICA IDENTITY FULL;
		INSERT INTO public.items VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')`)
	address, port, err := net.SplitHostPort(host)
	require.NoError(t, err)
	config := fmt.Sprintf(`default_environment: test
environments:
  test:
    connections:
      postgres:
        - name: source
          host: %s
          port: %s
          username: postgres
          password: %q
          database: %s
          ssl_mode: disable
        - name: destination
          host: %s
          port: %s
          username: postgres
          password: %q
          database: %s
          ssl_mode: disable
`, address, port, testPassword, sourceName, address, port, testPassword, destName)
	require.NoError(t, os.MkdirAll(filepath.Join(f.root, "assets"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, ".bruin.yml"), []byte(config), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(f.root, "pipeline.yml"), []byte("name: postgres-cdc-integration\n"), 0o600))
	output, err := exec.CommandContext(t.Context(), "git", "init", f.root).CombinedOutput()
	require.NoError(t, err, "%s", output)
	return f
}

func cdcParameters() map[string]any {
	return map[string]any{
		"source_connection": "source", "source_table": "public.items", "destination": "postgres",
		"cdc": true, "cdc_delete_behavior": "hard", "cdc_state_id": "bruin_integration",
	}
}

func (f *fixture) assertRows(t *testing.T, expected map[int]string) {
	t.Helper()
	rows, err := f.dest.QueryContext(t.Context(), "SELECT id, value FROM public.items")
	require.NoError(t, err)
	defer rows.Close()
	actual := map[int]string{}
	for rows.Next() {
		var id int
		var value string
		require.NoError(t, rows.Scan(&id, &value))
		require.NotContains(t, actual, id, "duplicate destination key")
		actual[id] = value
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, actual)
}

func (f *fixture) run(t *testing.T) {
	t.Helper()
	f.start(t, false).waitSuccess(t)
}

func (f *fixture) waitForInactiveSlot(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		var inactive bool
		return f.source.QueryRowContext(t.Context(), "SELECT NOT active FROM pg_replication_slots WHERE slot_name = $1", f.slot).Scan(&inactive) == nil && inactive
	}, 40*time.Second, 100*time.Millisecond, "the stopped CLI must release its replication connection")
}

func (f *fixture) pauseDestination(t *testing.T) func() {
	t.Helper()
	// CDC merge preserves the target table and its triggers. The first chunk
	// commits before this gate, while the second waits on our dedicated session.
	execSQL(t, f.dest, `CREATE TABLE public.items (id integer PRIMARY KEY, value text);
		CREATE FUNCTION public.pause_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF NEW.id >= 3 THEN PERFORM pg_advisory_xact_lock(86420); END IF;
			RETURN NEW;
		END $$;
		CREATE TRIGGER pause_snapshot BEFORE INSERT ON public.items
		FOR EACH ROW EXECUTE FUNCTION public.pause_snapshot()`)
	gate, err := f.dest.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = gate.Close() })
	_, err = gate.ExecContext(t.Context(), "SELECT pg_advisory_lock(86420)")
	require.NoError(t, err)
	return func() {
		_, err := gate.ExecContext(t.Context(), "SELECT pg_advisory_unlock(86420)")
		require.NoError(t, err)
	}
}

func (f *fixture) waitForDestinationGate(t *testing.T, run *process) {
	t.Helper()
	require.Eventually(t, func() bool {
		run.requireRunning(t)
		var waiting bool
		return f.dest.QueryRowContext(t.Context(), `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event = 'advisory')`).Scan(&waiting) == nil && waiting
	}, time.Minute, 100*time.Millisecond, "snapshot did not reach the destination gate")
}

type process struct {
	cmd     *exec.Cmd
	done    chan struct{}
	err     error
	logPath string
}

func (f *fixture) start(t *testing.T, stream bool) *process {
	t.Helper()
	asset, err := yaml.Marshal(map[string]any{
		"name": "public.items", "type": "ingestr", "connection": "destination", "parameters": f.params,
	})
	require.NoError(t, err)
	assetPath := filepath.Join(f.root, "assets", "items.asset.yml")
	require.NoError(t, os.WriteFile(assetPath, asset, 0o600))
	args := []string{"run", "--config-file", filepath.Join(f.root, ".bruin.yml"), "--env", "test", "--workers", "1", "--no-log-file", "--no-color"}
	if stream {
		args = append([]string{"--debug"}, args...)
		args = append(args, "--stream")
	}
	args = append(args, assetPath)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, f.binary, args...)
	command.Dir = f.root
	command.Env = append(os.Environ(), "DISABLE_TELEMETRY=true", "INGESTR_DISABLE_TELEMETRY=true")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error { return command.Process.Signal(os.Interrupt) }
	command.WaitDelay = 40 * time.Second
	log, err := os.CreateTemp(f.root, "run-*.log")
	require.NoError(t, err)
	command.Stdout, command.Stderr = log, log
	p := &process{cmd: command, done: make(chan struct{}), logPath: log.Name()}
	require.NoError(t, command.Start())
	go func() {
		p.err = command.Wait()
		_ = log.Close()
		close(p.done)
	}()
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			_ = command.Process.Signal(os.Interrupt)
			select {
			case <-p.done:
			case <-time.After(45 * time.Second):
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				<-p.done
			}
		}
		if t.Failed() {
			t.Log(p.output(t))
		}
	})
	return p
}

func (p *process) requireRunning(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("Bruin stopped before the expected observation: %v\n%s", p.err, p.output(t))
	default:
	}
}

func (p *process) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(2 * time.Minute):
		t.Fatalf("Bruin did not exit\n%s", p.output(t))
		return nil
	}
}

func (p *process) waitSuccess(t *testing.T) {
	t.Helper()
	require.NoError(t, p.wait(t), "Bruin run failed:\n%s", p.output(t))
}

func (p *process) interrupt(t *testing.T) {
	t.Helper()
	// Bruin kills finite ingestion immediately; streaming ingestion receives
	// SIGTERM and gets time to flush. Both must release their replication slot.
	require.NoError(t, p.cmd.Process.Signal(os.Interrupt))
	if err := p.wait(t); err != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 1, exit.ExitCode(), "unexpected cancellation failure:\n%s", p.output(t))
	}
}

func (p *process) output(t *testing.T) string {
	t.Helper()
	contents, err := os.ReadFile(p.logPath)
	require.NoError(t, err)
	return string(contents)
}

func startPostgres(t *testing.T) string {
	t.Helper()
	container := fmt.Sprintf("bruin-postgres-cdc-%d", time.Now().UnixNano())
	image := os.Getenv("BRUIN_POSTGRES_CDC_TEST_IMAGE")
	if image == "" {
		image = "postgres:17-alpine"
	}
	output, err := exec.CommandContext(t.Context(), "docker", "run", "-d", "--rm", "--name", container,
		"-e", "POSTGRES_PASSWORD="+testPassword, "-p", "127.0.0.1::5432", image,
		"postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32").CombinedOutput()
	require.NoError(t, err, "Docker with PostgreSQL is required: %s", output)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if t.Failed() {
			logs, _ := exec.CommandContext(ctx, "docker", "logs", container).CombinedOutput()
			t.Logf("PostgreSQL logs:\n%s", logs)
		}
		cleanup, cleanupErr := exec.CommandContext(ctx, "docker", "rm", "-f", container).CombinedOutput()
		if cleanupErr != nil {
			t.Logf("container cleanup: %v: %s", cleanupErr, cleanup)
		}
	})
	output, err = exec.CommandContext(t.Context(), "docker", "port", container, "5432/tcp").CombinedOutput()
	require.NoError(t, err, "%s", output)
	host := strings.TrimSpace(string(output))
	db, err := sql.Open("pgx", postgresURI(host, "postgres"))
	require.NoError(t, err)
	defer db.Close()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		return db.PingContext(ctx) == nil
	}, time.Minute, 200*time.Millisecond, "PostgreSQL did not become ready")
	return host
}

func postgresURI(host, database string) string {
	return (&url.URL{
		Scheme: "postgres", User: url.UserPassword("postgres", testPassword), Host: host,
		Path: "/" + database, RawQuery: "sslmode=disable",
	}).String()
}

func openPostgres(t *testing.T, uri string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func execSQL(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), statement)
	require.NoError(t, err, "SQL failed: %s", statement)
}

func bruinBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("BRUIN_TEST_BINARY")
	if binary == "" {
		binary = "../../bin/bruin"
	}
	path, err := filepath.Abs(binary)
	require.NoError(t, err)
	require.FileExists(t, path)
	return path
}

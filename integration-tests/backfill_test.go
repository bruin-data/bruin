package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/backfill"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type backfillResult struct {
	Backfill   backfill.Manifest `json:"backfill"`
	Summary    backfill.Summary  `json:"summary"`
	Options    backfill.Options  `json:"options"`
	Partitions []backfill.Record `json:"partitions"`
	Page       struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	} `json:"page"`
	HasMore bool `json:"has_more"`
}

type backfillFixture struct{ dir, binary, config, state, pipeline string }

func newBackfillFixture(t *testing.T) backfillFixture {
	t.Helper()
	cwd, err := os.Getwd()
	require.NoError(t, err)
	f := backfillFixture{dir: t.TempDir(), binary: bruinBinary(cwd)}
	f.config = filepath.Join(f.dir, ".bruin.yml")
	f.state = filepath.Join(f.dir, "logs", "backfills")
	f.pipeline = filepath.Join(f.dir, "pipeline")
	require.NoError(t, os.MkdirAll(filepath.Join(f.pipeline, "assets"), 0o755))
	require.NoError(t, exec.CommandContext(t.Context(), "git", "init", f.dir).Run())
	f.write(t, f.config, "default_environment: test\nenvironments:\n  test:\n    connections:\n      duckdb:\n        - name: local\n          path: data.duckdb\n")
	f.write(t, filepath.Join(f.pipeline, "pipeline.yml"), "name: backfill_test\nschedule: daily\ndefault_connections:\n  duckdb: local\nvariables:\n  marker:\n    type: string\n    default: default\n")
	return f
}

func (f backfillFixture) write(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func (f backfillFixture) asset(t *testing.T, name, content string) {
	t.Helper()
	f.write(t, filepath.Join(f.pipeline, "assets", name+".sql"), content)
}

func (f backfillFixture) command(ctx context.Context, args ...string) *exec.Cmd {
	c := exec.CommandContext(ctx, f.binary, args...)
	c.Dir = f.dir
	for _, v := range os.Environ() {
		name, _, _ := strings.Cut(v, "=")
		if !strings.HasPrefix(name, "BRUIN_") && name != "TELEMETRY_KEY" {
			c.Env = append(c.Env, v)
		}
	}
	c.Env = append(c.Env, "TELEMETRY_OPTOUT=1", "NO_COLOR=1")
	return c
}

func (f backfillFixture) run(t *testing.T, wantError bool, args ...string) backfillResult {
	t.Helper()
	args = append([]string{"backfill", "--state-dir", f.state, "--output", "json"}, args...)
	c := f.command(t.Context(), args...)
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	err := c.Run()
	if wantError {
		require.Error(t, err, "%s\n%s", stdout.String(), stderr.String())
	} else {
		require.NoError(t, err, "%s\n%s", stdout.String(), stderr.String())
	}
	var result backfillResult
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &result), "stdout: %s\nstderr: %s", stdout.String(), stderr.String())
	return result
}

func (f backfillFixture) query(t *testing.T, query string) string {
	t.Helper()
	c := f.command(t.Context(), "query", "--config-file", f.config, "--connection", "local", "--query", query, "--output", "csv")
	var stderr bytes.Buffer
	c.Stderr = &stderr
	out, err := c.Output()
	require.NoError(t, err, "%s\n%s", out, stderr.String())
	return strings.TrimSpace(string(out))
}

const captureBackfillSQL = `/* @bruin
name: captures
type: duckdb.sql
tags: [capture]
materialization:
  type: table
  strategy: append
@bruin */
SELECT '{{ start_timestamp }}'::TIMESTAMPTZ AS start_at,
       '{{ end_timestamp }}'::TIMESTAMPTZ AS end_at,
       '{{ run_id }}' AS run_id,
       '{{ var.marker }}' AS marker
`

// These tests are included in both make integration-test and the existing
// TestWorkflowTasks prefix used by make integration-test-light.
func TestWorkflowTasksBackfillDuckDBBoundaries(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	f.asset(t, "capture", captureBackfillSQL)
	f.asset(t, "excluded", "/* @bruin\nname: excluded\ntype: duckdb.sql\ntags: [excluded]\n@bruin */\nSELECT error('excluded asset must not run')")
	args := []string{f.pipeline, "--start-date", "2024-03-09", "--end-date", "2024-03-11", "--timezone", "America/New_York", "--partition", "daily", "--max-parallel", "4", "--workers", "2", "--selector", "tag:capture", "--var", `marker="override"`, "--environment", "test"}
	plan := f.run(t, false, append(slices.Clone(args), "--dry-run")...)
	require.Equal(t, 3, plan.Summary.Queued)
	require.Equal(t, 1, plan.Options.MaxParallel)
	_, err := os.Stat(f.state)
	require.True(t, os.IsNotExist(err), "dry-run must not create the store")
	result := f.run(t, false, args...)
	require.Equal(t, 3, result.Summary.Succeeded)
	for n, record := range result.Partitions {
		require.Equal(t, plan.Partitions[n].ID, record.ID)
		require.Len(t, record.Attempts, 1)
	}
	require.Equal(t, "hours\n24\n23\n24", f.query(t, "SELECT ((epoch_us(end_at) - epoch_us(start_at) + 1) / 3600000000)::INTEGER AS hours FROM captures ORDER BY start_at"))
	require.Equal(t, "n,distinct_runs,markers\n3,3,1", f.query(t, "SELECT count(*) AS n, count(DISTINCT run_id) AS distinct_runs, count(DISTINCT marker) AS markers FROM captures WHERE marker = 'override'"))
	for _, record := range result.Partitions {
		require.FileExists(t, filepath.Join(f.dir, "logs", "runs", "backfill_test", record.Attempts[0].RunID+".json"))
	}
	resumed := f.run(t, false, "--continue", result.Backfill.ID)
	require.Equal(t, 3, resumed.Summary.Skipped)
	require.Equal(t, "n\n3", f.query(t, "SELECT count(*) AS n FROM captures"))
}

func TestWorkflowTasksBackfillDuckDBTimeInterval(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE events (event_at TIMESTAMP)")
	f.asset(t, "events", `/* @bruin
name: events
type: duckdb.sql
materialization:
  type: table
  strategy: time_interval
  incremental_key: event_at
  time_granularity: timestamp
@bruin */
SELECT event_at FROM (VALUES
 (TIMESTAMP '2024-01-01 00:00:00'),
 (TIMESTAMP '2024-01-01 00:59:59.999999'),
 (TIMESTAMP '2024-01-01 01:00:00'),
 (TIMESTAMP '2024-01-01 01:59:59.999999'),
 (TIMESTAMP '2024-01-01 02:00:00'),
 (TIMESTAMP '2024-01-01 02:59:59.999999'),
 (TIMESTAMP '2024-01-01 03:00:00')
) source(event_at) WHERE event_at BETWEEN '{{ start_timestamp }}' AND '{{ end_timestamp }}'
`)
	result := f.run(t, false, f.pipeline, "--start-date", "2024-01-01T00:00:00Z", "--end-date", "2024-01-01T03:00:00Z", "--partition", "hourly", "--reverse")
	require.Equal(t, 3, result.Summary.Succeeded)
	require.Equal(t, "n,unique_rows\n6,6", f.query(t, "SELECT count(*) AS n, count(DISTINCT event_at) AS unique_rows FROM events"))
	result = f.run(t, false, "--continue", result.Backfill.ID, "--rerun", "all")
	require.Equal(t, 3, result.Summary.Succeeded)
	require.Equal(t, "n,unique_rows\n6,6", f.query(t, "SELECT count(*) AS n, count(DISTINCT event_at) AS unique_rows FROM events"))
	for _, r := range result.Partitions {
		require.Len(t, r.Attempts, 2)
	}
}

func TestWorkflowTasksBackfillDuckDBResumePartial(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	f.asset(t, "capture", captureBackfillSQL+"WHERE CASE WHEN '{{ start_date }}' = '2024-01-02' THEN error('injected failure') ELSE true END")
	first := f.run(t, true, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-04", "--on-failure", "stop")
	require.Equal(t, 1, first.Summary.Succeeded)
	require.Equal(t, 1, first.Summary.Failed)
	require.Equal(t, 2, first.Summary.Queued)
	f.asset(t, "capture", captureBackfillSQL)
	failed := f.run(t, false, "--continue", first.Backfill.ID, "--rerun", "failed")
	require.Equal(t, 2, failed.Summary.Succeeded)
	require.Equal(t, 2, failed.Summary.Queued)
	require.Equal(t, 3, failed.Summary.Skipped)
	all := f.run(t, false, "--continue", first.Backfill.ID)
	require.Equal(t, 4, all.Summary.Succeeded)
	require.Equal(t, 2, all.Summary.Skipped)
	require.Equal(t, "n,unique_intervals\n4,4", f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
	require.Equal(t, first.Partitions[0].Attempts, all.Partitions[0].Attempts)
	require.Len(t, all.Partitions[1].Attempts, 2)
}

func TestWorkflowTasksBackfillDuckDBRetries(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.asset(t, "attempt", `/* @bruin
name: attempts
type: duckdb.sql
@bruin */
CREATE TABLE IF NOT EXISTS attempts (day DATE);
INSERT INTO attempts VALUES ('{{ start_date }}');`)
	f.asset(t, "check", `/* @bruin
name: retry_check
type: duckdb.sql
depends: [attempts]
@bruin */
SELECT CASE WHEN count(*) < 2 THEN error('transient failure') ELSE 1 END
FROM attempts WHERE day = '{{ start_date }}';`)
	result := f.run(t, false, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-02", "--retries", "1")
	require.Equal(t, 2, result.Summary.Succeeded)
	for _, r := range result.Partitions {
		require.Len(t, r.Attempts, 2)
		require.Equal(t, backfill.Failed, r.Attempts[0].Status)
		require.Equal(t, backfill.Succeeded, r.Attempts[1].Status)
	}
	require.Equal(t, "day,n\n2024-01-01,2\n2024-01-02,2", f.query(t, "SELECT day::VARCHAR AS day, count(*) AS n FROM attempts GROUP BY day ORDER BY day"))
}

func TestWorkflowTasksBackfillDuckDBModifiersAndMonths(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	sql := strings.Replace(captureBackfillSQL, "tags: [capture]", "interval_modifiers:\n  start: -2h\n  end: -2h", 1)
	f.asset(t, "capture", sql)
	result := f.run(t, false, filepath.Join(f.pipeline, "assets", "capture.sql"), "--start-date", "2023-12-31", "--end-date", "2024-02-01", "--partition", "monthly", "--apply-interval-modifiers", "--var", `marker="shifted"`)
	require.Equal(t, 3, result.Summary.Succeeded)
	require.Equal(t, "first_at,last_at\n2023-12-30 22:00:00,2024-02-01 21:59:59.999999", f.query(t, "SELECT (min(start_at) AT TIME ZONE 'UTC')::VARCHAR AS first_at, (max(end_at) AT TIME ZONE 'UTC')::VARCHAR AS last_at FROM captures"))
}

func TestWorkflowTasksBackfillDuckDBTimeout(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.asset(t, "capture", captureBackfillSQL)
	result := f.run(t, true, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-02", "--timeout", "0")
	require.Equal(t, 0, result.Summary.Succeeded)
	require.Equal(t, 1, result.Summary.Failed)
	require.Equal(t, 1, result.Summary.Queued)
}

func TestWorkflowTasksBackfillDuckDBCancellation(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("os.Interrupt is unsupported on Windows")
	}
	f := newBackfillFixture(t)
	f.asset(t, "slow", "/* @bruin\nname: slow\ntype: duckdb.sql\n@bruin */\nSELECT sum(sin(i)) FROM range(1000000000000) t(i)")
	c := f.command(t.Context(), "backfill", f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-03", "--output", "json")
	var out, stderr bytes.Buffer
	c.Stdout = &out
	c.Stderr = &stderr
	require.NoError(t, c.Start())
	done := make(chan error, 1)
	go func() { done <- c.Wait() }()
	var manifest backfill.Manifest
	require.Eventually(t, func() bool {
		paths, _ := filepath.Glob(filepath.Join(f.state, "*", "manifest.json"))
		if len(paths) != 1 {
			return false
		}
		data, err := os.ReadFile(paths[0])
		if err != nil || json.Unmarshal(data, &manifest) != nil {
			return false
		}
		records, _ := filepath.Glob(filepath.Join(f.state, manifest.ID, "partitions", "*.json"))
		if len(records) == 0 {
			return false
		}
		data, err = os.ReadFile(records[0])
		if err != nil {
			return false
		}
		var r backfill.Record
		if json.Unmarshal(data, &r) != nil || r.Status != backfill.Running || len(r.Attempts) == 0 {
			return false
		}
		log, err := os.ReadFile(filepath.Join(f.state, manifest.ID, "children", r.Attempts[0].RunID+".log"))
		return err == nil && strings.Contains(string(log), "Running:")
	}, 30*time.Second, 25*time.Millisecond)
	require.NoError(t, c.Process.Signal(os.Interrupt))
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(40 * time.Second):
		_ = c.Process.Kill()
		t.Fatal("backfill failed to cancel")
	}
	var result backfillResult
	require.NoError(t, json.Unmarshal(out.Bytes(), &result), "%s\n%s", out.String(), stderr.String())
	require.Equal(t, 1, result.Summary.Cancelled)
	require.Equal(t, 2, result.Summary.Queued)
	require.Zero(t, result.Summary.Succeeded)
	f.asset(t, "slow", "/* @bruin\nname: slow\ntype: duckdb.sql\n@bruin */\nSELECT 1")
	resumed := f.run(t, false, "--continue", manifest.ID)
	require.Equal(t, 3, resumed.Summary.Succeeded)
	require.Len(t, resumed.Partitions[0].Attempts, 2)
}

func TestWorkflowTasksBackfillDuckDBParallelReadOnly(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE seed AS SELECT 1 AS n")
	f.write(t, f.config, "default_environment: test\nenvironments:\n  test:\n    connections:\n      duckdb:\n        - name: local\n          path: data.duckdb\n          read_only: true\n          max_concurrent_assets: 3\n")
	f.asset(t, "read", "/* @bruin\nname: read_only\ntype: duckdb.sql\n@bruin */\nSELECT sum(sin(i)) FROM range(1000000) t(i)")
	result := f.run(t, false, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-04", "--max-parallel", "2", "--workers", "1")
	require.Equal(t, 4, result.Summary.Succeeded)
	require.Equal(t, 2, result.Options.MaxParallel)
	type event struct {
		at    time.Time
		delta int
	}
	events := make([]event, 0, 2*len(result.Partitions))
	for _, r := range result.Partitions {
		a := r.Attempts[0]
		events = append(events, event{a.StartedAt, 1}, event{*a.FinishedAt, -1})
	}
	slices.SortFunc(events, func(a, b event) int { return a.at.Compare(b.at) })
	active, peak := 0, 0
	for _, e := range events {
		active += e.delta
		peak = max(peak, active)
	}
	require.Equal(t, 2, peak, fmt.Sprint(events))
	require.Zero(t, active)
}

func TestWorkflowTasksBackfillDuckDBMultiYear(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		partition, end string
		count          int
	}{{"daily", "2020-12-31", 731}, {"monthly", "2021-12-31", 36}} {
		t.Run(tc.partition, func(t *testing.T) {
			t.Parallel()
			f := newBackfillFixture(t)
			f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
			f.asset(t, "capture", captureBackfillSQL)
			result := f.run(t, false, f.pipeline, "--start-date", "2019-01-01", "--end-date", tc.end, "--partition", tc.partition, "--workers", "1", "--no-validation")
			require.Equal(t, tc.count, result.Summary.Succeeded)
			require.Equal(t, fmt.Sprintf("n,unique_intervals\n%d,%d", tc.count, tc.count), f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
			require.Equal(t, "gaps\n0", f.query(t, "SELECT count(*) AS gaps FROM (SELECT start_at, lag(end_at) OVER (ORDER BY start_at) AS previous_end FROM captures) WHERE previous_end + INTERVAL 1 MICROSECOND != start_at"))
			resumed := f.run(t, false, "--continue", result.Backfill.ID)
			require.Equal(t, tc.count, resumed.Summary.Skipped)
		})
	}
}

func TestWorkflowTasksBackfillSavedEnvironmentOverrides(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	f.asset(t, "capture", captureBackfillSQL)
	run := func(args, env []string) backfillResult {
		t.Helper()
		c := f.command(t.Context(), append([]string{"backfill", "--output", "json"}, args...)...)
		c.Env = append(c.Env, env...)
		var out, stderr bytes.Buffer
		c.Stdout = &out
		c.Stderr = &stderr
		require.NoError(t, c.Run(), "%s\n%s", out.String(), stderr.String())
		var result backfillResult
		require.NoError(t, json.Unmarshal(out.Bytes(), &result))
		return result
	}
	first := run([]string{f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-01"}, []string{`BRUIN_VARS=marker="saved"`, "BRUIN_CONFIG_FILE=" + f.config, "BRUIN_FULL_REFRESH=1", "BRUIN_RUN_ID=external-run"})
	require.Equal(t, 1, first.Summary.Succeeded)
	second := run([]string{"--continue", first.Backfill.ID, "--rerun", "all"}, []string{`BRUIN_VARS=marker="changed"`, "BRUIN_CONFIG_FILE=/missing/config", "BRUIN_QUERY_ANNOTATIONS=invalid-json", "BRUIN_FULL_REFRESH=1", "BRUIN_RUN_ID=external-run"})
	require.Equal(t, 1, second.Summary.Succeeded)
	require.Equal(t, "n,unique_runs\n2,2", f.query(t, "SELECT count(*) AS n, count(DISTINCT run_id) AS unique_runs FROM captures WHERE marker='saved'"))
	for _, a := range second.Partitions[0].Attempts {
		require.NotEqual(t, "external-run", a.RunID)
	}
}

// Check the on-disk records and the child run logs independently of the parent
// command's JSON response, including the inclusive interval passed to bruin run.
func (f backfillFixture) assertRunRecords(t *testing.T, result backfillResult, workers int) {
	t.Helper()
	root := filepath.Join(f.state, result.Backfill.ID)
	data, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	var manifest backfill.Manifest
	require.NoError(t, json.Unmarshal(data, &manifest))
	require.Equal(t, result.Backfill, manifest)
	for _, partition := range result.Partitions {
		if len(partition.Attempts) == 0 {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, "partitions", partition.ID+".json"))
		require.NoError(t, err)
		var saved backfill.Record
		require.NoError(t, json.Unmarshal(data, &saved))
		require.Equal(t, partition, saved)
		for _, attempt := range partition.Attempts {
			require.NotNil(t, attempt.FinishedAt)
			require.False(t, attempt.FinishedAt.Before(attempt.StartedAt))
			require.FileExists(t, filepath.Join(root, "children", attempt.RunID+".log"))
			data, err := os.ReadFile(filepath.Join(f.dir, "logs", "runs", "backfill_test", attempt.RunID+".json"))
			require.NoError(t, err)
			var child scheduler.PipelineState
			require.NoError(t, json.Unmarshal(data, &child))
			require.Equal(t, attempt.RunID, child.RunID)
			require.Equal(t, result.Backfill.ID, child.BackfillID)
			require.Equal(t, result.Summary.Total, child.BackfillTotal)
			require.Equal(t, workers, child.Parameters.Workers)
			require.Equal(t, "test", child.Parameters.Environment)
			start, err := time.Parse(time.RFC3339Nano, child.Parameters.StartDate)
			require.NoError(t, err)
			end, err := time.Parse(time.RFC3339Nano, child.Parameters.EndDate)
			require.NoError(t, err)
			require.True(t, partition.Start.Equal(start), "child start differs from partition start")
			require.True(t, partition.End.Add(-time.Microsecond).Equal(end), "child end must be inclusive")
		}
	}
}

func TestWorkflowTasksBackfillDuckDBFailurePolicies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		policy            string
		succeeded, queued int
	}{
		{"continue", 3, 0}, {"stop", 1, 2}, {"fail-fast", 1, 2},
	} {
		t.Run(tc.policy, func(t *testing.T) {
			t.Parallel()
			f := newBackfillFixture(t)
			f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
			f.asset(t, "capture", captureBackfillSQL+"WHERE CASE WHEN '{{ start_date }}' = '2024-01-02' THEN error('injected failure') ELSE true END")
			result := f.run(t, true, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-04", "--on-failure", tc.policy, "--workers", "2")
			require.Equal(t, 4, result.Summary.Total)
			require.Equal(t, tc.succeeded, result.Summary.Succeeded)
			require.Equal(t, 1, result.Summary.Failed)
			require.Equal(t, tc.queued, result.Summary.Queued)
			require.Zero(t, result.Summary.Running)
			require.Equal(t, backfill.Failed, result.Partitions[1].Status)
			require.Equal(t, fmt.Sprintf("n,unique_intervals\n%d,%d", tc.succeeded, tc.succeeded), f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
			require.Equal(t, "n\n0", f.query(t, "SELECT count(*) AS n FROM captures WHERE start_at = TIMESTAMPTZ '2024-01-02 00:00:00+00'"))
			f.assertRunRecords(t, result, 2)
		})
	}
}

func TestWorkflowTasksBackfillDuckDBResumeMissing(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	f.asset(t, "capture", captureBackfillSQL+"WHERE CASE WHEN '{{ start_date }}' = '2024-01-02' THEN error('injected failure') ELSE true END")
	first := f.run(t, true, f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-04", "--on-failure", "stop")
	require.Equal(t, 2, first.Summary.Queued)
	// The failed partition remains failed, so the aggregate command still exits
	// nonzero even though both selected missing partitions complete successfully.
	missing := f.run(t, true, "--continue", first.Backfill.ID, "--rerun", "missing")
	require.Equal(t, 3, missing.Summary.Succeeded)
	require.Equal(t, 1, missing.Summary.Failed)
	require.Equal(t, 2, missing.Summary.Skipped)
	require.Zero(t, missing.Summary.Queued)
	require.Equal(t, first.Partitions[0], missing.Partitions[0])
	require.Equal(t, first.Partitions[1], missing.Partitions[1])
	require.Equal(t, "n,unique_intervals\n3,3", f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
	f.asset(t, "capture", captureBackfillSQL)
	completed := f.run(t, false, "--continue", first.Backfill.ID)
	require.Equal(t, 4, completed.Summary.Succeeded)
	require.Equal(t, 3, completed.Summary.Skipped)
	require.Equal(t, missing.Partitions[0], completed.Partitions[0])
	require.Equal(t, missing.Partitions[2:], completed.Partitions[2:])
	require.Len(t, completed.Partitions[1].Attempts, 2)
	require.Equal(t, "n,unique_intervals\n4,4", f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
	f.assertRunRecords(t, completed, 16)
}

func TestWorkflowTasksBackfillDuckDBFallDST(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	f.asset(t, "capture", captureBackfillSQL)
	args := []string{f.pipeline, "--start-date", "2024-11-03", "--end-date", "2024-11-03", "--timezone", "America/New_York", "--partition", "hourly", "--workers", "1"}
	planned := f.run(t, false, append(slices.Clone(args), "--dry-run")...)
	executed := f.run(t, false, append(slices.Clone(args), "--reverse")...)
	require.Equal(t, 25, executed.Summary.Succeeded)
	var repeatedHour []backfill.Record
	for n, partition := range executed.Partitions {
		require.Equal(t, planned.Partitions[len(planned.Partitions)-1-n].ID, partition.ID)
		require.Equal(t, time.Hour, partition.End.Sub(partition.Start))
		if partition.Start.Hour() == 1 {
			repeatedHour = append(repeatedHour, partition)
		}
	}
	require.Len(t, repeatedHour, 2)
	require.NotEqual(t, repeatedHour[0].ID, repeatedHour[1].ID)
	_, offset0 := repeatedHour[0].Start.Zone()
	_, offset1 := repeatedHour[1].Start.Zone()
	require.NotEqual(t, offset0, offset1)
	require.Equal(t, "n,unique_intervals\n25,25", f.query(t, "SELECT count(*) AS n, count(DISTINCT start_at) AS unique_intervals FROM captures"))
	require.Equal(t, "n\n2", f.query(t, "SELECT count(*) AS n FROM captures WHERE start_at IN (TIMESTAMPTZ '2024-11-03 01:00:00-04:00', TIMESTAMPTZ '2024-11-03 01:00:00-05:00')"))
	require.Equal(t, "gaps\n0", f.query(t, "SELECT count(*) AS gaps FROM (SELECT start_at, lag(end_at) OVER (ORDER BY start_at) AS previous_end FROM captures) WHERE previous_end + INTERVAL 1 MICROSECOND != start_at"))
	f.assertRunRecords(t, executed, 1)
}

func TestWorkflowTasksBackfillDuckDBPagination(t *testing.T) {
	t.Parallel()
	f := newBackfillFixture(t)
	f.asset(t, "capture", captureBackfillSQL)
	args := []string{f.pipeline, "--start-date", "2024-01-01", "--end-date", "2024-01-04"}
	page := f.run(t, false, append(slices.Clone(args), "--dry-run", "--offset", "1", "--limit", "1")...)
	require.Equal(t, 4, page.Summary.Queued)
	require.Len(t, page.Partitions, 1)
	require.Equal(t, 1, page.Page.Offset)
	require.Equal(t, 1, page.Page.Limit)
	require.True(t, page.HasMore)
	require.NoDirExists(t, f.state)
	require.NoFileExists(t, filepath.Join(f.dir, "data.duckdb"))
	plan := f.run(t, false, append(slices.Clone(args), "--dry-run", "--limit", "0")...)
	require.Len(t, plan.Partitions, 4)
	require.False(t, plan.HasMore)
	require.Equal(t, plan.Partitions[1], page.Partitions[0])
	f.query(t, "CREATE TABLE captures (start_at TIMESTAMPTZ, end_at TIMESTAMPTZ, run_id VARCHAR, marker VARCHAR)")
	executed := f.run(t, false, append(slices.Clone(args), "--offset", "1", "--limit", "1")...)
	require.Equal(t, 4, executed.Summary.Succeeded)
	require.Len(t, executed.Partitions, 1)
	require.Equal(t, page.Partitions[0].ID, executed.Partitions[0].ID)
	require.True(t, executed.HasMore)
	require.Equal(t, "n\n4", f.query(t, "SELECT count(*) AS n FROM captures"))
	empty := f.run(t, false, "--continue", executed.Backfill.ID, "--dry-run", "--offset", "4", "--limit", "1")
	require.Empty(t, empty.Partitions)
	require.False(t, empty.HasMore)
	require.Equal(t, 4, empty.Summary.Succeeded)
	all := f.run(t, false, "--continue", executed.Backfill.ID, "--limit", "0")
	require.Len(t, all.Partitions, 4)
	require.Equal(t, 4, all.Summary.Skipped)
	require.Equal(t, "n\n4", f.query(t, "SELECT count(*) AS n FROM captures"))
}

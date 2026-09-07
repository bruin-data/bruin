package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/backfill"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestBackfillChildArgs(t *testing.T) {
	t.Parallel()
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	i := backfill.Interval{Start: time.Date(2024, 3, 10, 0, 0, 0, 0, loc), End: time.Date(2024, 3, 11, 0, 0, 0, 0, loc)}
	m := backfill.Manifest{ID: "bf", Plan: backfill.Plan{Target: "/repo/assets/a.sql", RunFlags: map[string][]string{"var": {"a=1", "payload={\"x\": [1,2]}"}, "selector": {"+tag:finance"}, "apply-interval-modifiers": {"true"}, "secrets-backend": {"vault"}}}}
	args := backfillChildArgs(m, i, 3, 20, true)
	require.Equal(t, []string{"--debug", "--secrets-backend", "vault", "run", "--workers", "3", "--start-date", "2024-03-10T00:00:00.000000-05:00", "--end-date", "2024-03-10T23:59:59.999999-04:00", "--backfill-id", "bf", "--backfill-total", "20", "--apply-interval-modifiers=true", "--selector=+tag:finance", "--var=a=1", "--var=payload={\"x\": [1,2]}", "/repo/assets/a.sql"}, args)
}

func TestBackfillParallelism(t *testing.T) {
	t.Parallel()
	limit := 5
	for _, tc := range []struct {
		name                    string
		parallel, workers, want int
		connections             *config.Connections
	}{
		{"no limits", 4, 16, 4, &config.Connections{}},
		{"nil", 4, 16, 4, nil},
		{"workers below limit", 4, 2, 2, &config.Connections{DuckDB: []config.DuckDBConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "db", MaxConcurrentAssets: &limit}, ReadOnly: true}}}},
		{"workers exceed limit", 4, 16, 1, &config.Connections{DuckDB: []config.DuckDBConnection{{ConnectionMetadata: config.ConnectionMetadata{Name: "db", MaxConcurrentAssets: &limit}, ReadOnly: true}}}},
		{"local DuckDB writer", 4, 2, 1, &config.Connections{DuckDB: []config.DuckDBConnection{{Path: "test.db"}}}},
		{"read only DuckDB", 4, 2, 4, &config.Connections{DuckDB: []config.DuckDBConnection{{Path: "test.db", ReadOnly: true}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := backfillParallelism(tc.parallel, tc.workers, tc.connections)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestBackfillOutputStreamsJSON(t *testing.T) {
	t.Parallel()
	start, end, err := backfill.ParseRange("2024-11-03", "2024-11-03", "America/New_York")
	require.NoError(t, err)
	m := backfill.Manifest{Version: backfill.Version, ID: "test", Plan: backfill.Plan{Target: "pipeline", Start: start, End: end, Timezone: "America/New_York", Partition: "hourly"}}
	summary, err := backfill.Summarize(t.Context(), m.Plan, nil, "")
	require.NoError(t, err)
	var output bytes.Buffer
	require.NoError(t, writeBackfillOutput(&output, "json", m, nil, backfill.Options{Reverse: true}, summary, backfillPage{Limit: 1000}))
	var result struct {
		Partitions []backfill.Record `json:"partitions"`
		Summary    backfill.Summary  `json:"summary"`
	}
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	require.Len(t, result.Partitions, 25)
	require.Equal(t, 25, result.Summary.Queued)
	require.True(t, result.Partitions[0].End.Equal(end))
}

// Each case asserts the specific rejection, so a guard that stops firing cannot be
// masked by an unrelated error raised later in runBackfill.
//
//nolint:paralleltest // urfave/cli mutates shared run flag definitions during setup.
func TestBackfillInvalidCLI(t *testing.T) {
	for name, tc := range map[string]struct {
		args    []string
		wantErr string
	}{
		"max-parallel":     {[]string{"--max-parallel", "0"}, "max-parallel and workers must be positive"},
		"workers":          {[]string{"--workers", "0"}, "max-parallel and workers must be positive"},
		"retries":          {[]string{"--retries", "-1"}, "retries must not be negative"},
		"on-failure":       {[]string{"--on-failure", "bad"}, "on-failure must be continue, stop, or fail-fast"},
		"output":           {[]string{"--output", "bad"}, "output must be text or json"},
		"missing end-date": {[]string{"--state-dir", "unused", "--start-date", "2024-01-01"}, "start-date and end-date are required"},
		"escaping ID":      {[]string{"--state-dir", "unused", "--continue", "../../escape"}, "invalid backfill ID"},
		// --rerun only selects partitions that already have a recorded status, so
		// accepting it on a new backfill would silently execute nothing and exit 0.
		"rerun without continue": {[]string{"--rerun", "failed"}, "requires --continue"},
	} {
		t.Run(name, func(t *testing.T) {
			debug := false
			c := Backfill(&debug)
			c.Writer = io.Discard
			c.ErrWriter = io.Discard
			app := &cli.Command{Name: "bruin", Commands: []*cli.Command{c}, Writer: io.Discard, ErrWriter: io.Discard}
			require.ErrorContains(t, app.Run(t.Context(), append([]string{"bruin", "backfill"}, tc.args...)), tc.wantErr)
		})
	}
}

//nolint:paralleltest // urfave/cli mutates shared run flag definitions during setup.
func TestBackfillFlagDefinitions(t *testing.T) {
	debug := false
	names := map[string]bool{}
	for _, flag := range Backfill(&debug).Flags {
		names[flag.Names()[0]] = true
	}
	for _, name := range backfillRunFlags {
		require.True(t, names[name], name)
	}
	for _, name := range []string{"stream", "full-refresh", "modified", "interactive", "backfill-id"} {
		require.False(t, names[name], name)
	}
}

func TestBackfillOutputPagination(t *testing.T) {
	t.Parallel()
	start, end, err := backfill.ParseRange("2000-01-01", "2025-12-31", "UTC")
	require.NoError(t, err)
	m := backfill.Manifest{Version: backfill.Version, ID: "test", Plan: backfill.Plan{Target: "pipeline", Start: start, End: end, Timezone: "UTC", Partition: "1us"}}
	summary, err := backfill.Summarize(t.Context(), m.Plan, nil, "")
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, writeBackfillOutput(&out, "json", m, nil, backfill.Options{}, summary, backfillPage{Offset: 2, Limit: 3}))
	var result struct {
		Partitions []backfill.Record `json:"partitions"`
		HasMore    bool              `json:"has_more"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &result))
	require.True(t, result.HasMore)
	require.Len(t, result.Partitions, 3)
	require.Equal(t, start.Add(2*time.Microsecond), result.Partitions[0].Start)
	require.Equal(t, start.Add(5*time.Microsecond), result.Partitions[2].End)
}

// The saved inputs define the partition identities, so a resume that silently
// accepted a different selector or range would write records for a different plan.
//
//nolint:paralleltest // urfave/cli mutates shared run flag definitions during setup.
func TestBackfillContinueRejectsChangedInputs(t *testing.T) {
	root := t.TempDir()
	const id = "saved-backfill"
	store, err := backfill.Open(root, id)
	require.NoError(t, err)
	start, end, err := backfill.ParseRange("2024-01-01", "2024-01-02", "UTC")
	require.NoError(t, err)
	require.NoError(t, store.Create(backfill.Manifest{
		Version: backfill.Version, ID: id, CreatedAt: time.Now().UTC(),
		Plan: backfill.Plan{Target: "/repo/pipeline", Environment: "dev", Timezone: "UTC", Partition: "daily", Start: start, End: end},
	}))
	for _, args := range [][]string{
		{"--selector", "tag:finance"},
		{"--var", `region="eu"`},
		{"--partition", "monthly"},
		{"--start-date", "2024-02-01"},
		{"--timezone", "America/New_York"},
		{"--environment", "other"},
	} {
		t.Run(args[0], func(t *testing.T) {
			debug := false
			c := Backfill(&debug)
			c.Writer, c.ErrWriter = io.Discard, io.Discard
			app := &cli.Command{Name: "bruin", Commands: []*cli.Command{c}, Writer: io.Discard, ErrWriter: io.Discard}
			err := app.Run(t.Context(), append([]string{"bruin", "backfill", "--state-dir", root, "--continue", id}, args...))
			require.ErrorContains(t, err, "cannot be changed with --continue")
		})
	}
}

// A backfill writes to every partition unattended, so the production guard is the
// only confirmation step; switchEnvironment's interactive prompt cannot apply here.
//
//nolint:paralleltest // urfave/cli mutates shared run flag definitions during setup.
func TestBackfillRequiresForceInProduction(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, ".bruin.yml")
	require.NoError(t, os.WriteFile(configPath, []byte("default_environment: production\nenvironments:\n  production:\n    connections:\n      duckdb:\n        - name: local\n          path: data.duckdb\n"), 0o600))
	args := []string{
		"bruin", "backfill", dir, "--state-dir", filepath.Join(dir, "state"), "--config-file", configPath,
		"--environment", "production", "--start-date", "2024-01-01", "--end-date", "2024-01-01", "--dry-run",
	}
	run := func(args ...string) error {
		debug := false
		c := Backfill(&debug)
		c.Writer, c.ErrWriter = io.Discard, io.Discard
		app := &cli.Command{Name: "bruin", Commands: []*cli.Command{c}, Writer: io.Discard, ErrWriter: io.Discard}
		return app.Run(t.Context(), args)
	}
	// --dry-run returns before the guard, so a preview of production is allowed.
	require.NoError(t, run(args...))
	require.ErrorContains(t, run(args[:len(args)-1]...), "production environment requires --force")
	require.NoDirExists(t, filepath.Join(dir, "state"))
}

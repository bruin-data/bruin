package cmd

import (
	"bytes"
	"encoding/json"
	"io"
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

//nolint:paralleltest // urfave/cli mutates shared run flag definitions during setup.
func TestBackfillInvalidCLI(t *testing.T) {
	for _, args := range [][]string{
		{"--max-parallel", "0"},
		{"--workers", "0"},
		{"--retries", "-1"},
		{"--on-failure", "bad"},
		{"--output", "bad"},
		{"--state-dir", "unused", "--start-date", "2024-01-01"},
		{"--state-dir", "unused", "--continue", "../../escape"},
	} {
		t.Run(args[0], func(t *testing.T) {
			debug := false
			c := Backfill(&debug)
			c.Writer = io.Discard
			c.ErrWriter = io.Discard
			app := &cli.Command{Name: "bruin", Commands: []*cli.Command{c}, Writer: io.Discard, ErrWriter: io.Discard}
			require.Error(t, app.Run(t.Context(), append([]string{"bruin", "backfill"}, args...)))
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

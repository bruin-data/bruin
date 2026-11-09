package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type rawRenderStubConnection struct {
	summary *ansisql.DBDatabase
}

func (c *rawRenderStubConnection) GetDatabaseSummary(context.Context) (*ansisql.DBDatabase, error) {
	return c.summary, nil
}

func prefixedRawRenderConnection() *rawRenderStubConnection {
	return &rawRenderStubConnection{summary: &ansisql.DBDatabase{
		Name: "main",
		Schemas: []*ansisql.DBSchema{
			{Name: "raw", Tables: []*ansisql.DBTable{{Name: "events"}, {Name: "users"}}},
			{Name: "jane_raw", Tables: []*ansisql.DBTable{{Name: "events"}}},
		},
	}}
}

func rawRenderOptions(prefix string) rawQueryRenderOptions {
	return rawQueryRenderOptions{
		ConnectionName: "warehouse",
		Dialect:        "postgres",
		SchemaPrefix:   prefix,
		StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndDate:        time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
	}
}

func TestRenderRawQuerySQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sql    string
		prefix string
		conn   any
		opts   func(rawQueryRenderOptions) rawQueryRenderOptions
		want   string
	}{
		{
			name: "no prefix leaves the query unchanged",
			sql:  "SELECT * FROM raw.events",
			conn: nil,
			opts: func(o rawQueryRenderOptions) rawQueryRenderOptions { return o },
			want: "SELECT * FROM raw.events",
		},
		{
			name:   "schema prefix rewrites a table that exists in the prefixed schema",
			sql:    "SELECT * FROM raw.events",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "SELECT * FROM jane_raw.events",
		},
		{
			name:   "schema prefix leaves a table without a prefixed copy",
			sql:    "SELECT * FROM raw.users",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "SELECT * FROM raw.users",
		},
		{
			name:   "schema_prefix renders the selected environment prefix",
			sql:    "SELECT '{{ schema_prefix }}' AS prefix",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "SELECT 'jane_' AS prefix",
		},
		{
			name:   "quoted mixed-case identifiers are preserved",
			sql:    `SELECT * FROM "raw"."Events"`,
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   `SELECT * FROM "raw"."Events"`,
		},
		{
			name:   "differently cased identifiers are preserved",
			sql:    "SELECT * FROM raw.EVENTS",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "SELECT * FROM raw.EVENTS",
		},
		{
			name:   "ddl for a table without a prefixed copy passes through",
			sql:    "CREATE TABLE raw.new_table (id INT)",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "CREATE TABLE raw.new_table (id INT)",
		},
		{
			name:   "already prefixed tables are not prefixed twice",
			sql:    "SELECT * FROM jane_raw.events",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			want:   "SELECT * FROM jane_raw.events",
		},
		{
			name:   "large integer variables keep their digits",
			sql:    "SELECT {{ big_id }} AS id",
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			opts: func(o rawQueryRenderOptions) rawQueryRenderOptions {
				o.Vars = map[string]any{"big_id": "18446744073709551615"}
				return o
			},
			want: "SELECT 18446744073709551615 AS id",
		},
		{
			name:   "full refresh and variables are available to jinja",
			sql:    `SELECT '{{ schema_prefix }}' AS prefix, {{ count }} AS count, {% if full_refresh %}true{% else %}false{% endif %} AS full_refresh`,
			prefix: "jane_",
			conn:   prefixedRawRenderConnection(),
			opts: func(o rawQueryRenderOptions) rawQueryRenderOptions {
				o.FullRefresh = true
				o.Vars = map[string]any{"count": 3}
				return o
			},
			want: "SELECT 'jane_' AS prefix, 3 AS count, TRUE AS full_refresh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := rawRenderOptions(tt.prefix)
			if tt.opts != nil {
				opts = tt.opts(opts)
			}

			got, err := renderRawQuerySQL(context.Background(), tt.sql, tt.conn, opts)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWriteRawQueryRenderOutput(t *testing.T) {
	t.Parallel()

	t.Run("json response contains the query field", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		require.NoError(t, writeRawQueryRenderOutput(buf, "json", "SELECT * FROM jane_raw.events"))

		var payload map[string]string
		require.NoError(t, json.Unmarshal(buf.Bytes(), &payload))
		assert.Equal(t, "SELECT * FROM jane_raw.events", payload["query"])
	})

	t.Run("plain output writes the query", func(t *testing.T) {
		t.Parallel()

		buf := &bytes.Buffer{}
		require.NoError(t, writeRawQueryRenderOutput(buf, "", "SELECT 1"))
		assert.Equal(t, "SELECT 1\n", buf.String())
	})
}

func TestParseTypedQueryVars(t *testing.T) {
	t.Parallel()

	got, err := parseTypedQueryVars([]string{
		`name="alice"`,
		"count=3",
		"enabled=true",
		`filters={"start_date":"2024-01-01"}`,
		"start_date=2024-01-01",
		"nested.value=7",
		"big_id=18446744073709551615",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"name":       "alice",
		"count":      int64(3),
		"enabled":    true,
		"filters":    map[string]any{"start_date": "2024-01-01"},
		"start_date": "2024-01-01",
		"big_id":     "18446744073709551615",
		"nested":     map[string]any{"value": int64(7)},
	}, got)

	_, err = parseTypedQueryVars([]string{"missing-separator"})
	require.Error(t, err)

	_, err = parseTypedQueryVars([]string{"=value"})
	require.Error(t, err)
}

func TestRenderCommandStdinCloudConnectionSkipsLocalConnection(t *testing.T) { //nolint:paralleltest // uses t.Chdir
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".bruin.yml"), []byte(`
default_environment: dev
environments:
  dev:
    schema_prefix: jane_
    connections:
      google_cloud_platform:
        - name: warehouse
          use_cloud: true
`), 0o600))
	t.Chdir(dir)

	command := Render()
	command.Reader = bytes.NewBufferString("SELECT '{{ schema_prefix }}' AS prefix")
	output := &bytes.Buffer{}
	command.Writer = output
	command.ErrWriter = &bytes.Buffer{}
	command.ExitErrHandler = func(context.Context, *cli.Command, error) {}

	err := command.Run(context.Background(), []string{
		"render", "--stdin", "--connection", "warehouse", "--raw-query", "--output", "json", "--environment", "dev",
	})
	require.NoError(t, err)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(output.Bytes(), &payload))
	assert.Equal(t, "SELECT '' AS prefix", payload["query"])
}

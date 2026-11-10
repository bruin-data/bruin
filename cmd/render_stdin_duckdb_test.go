//go:build !bruin_no_duckdb

package cmd

import (
	"fmt"
	"path/filepath"
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:paralleltest // urfave/cli mutates the shared date flag definitions during setup.
func TestRenderStdinCommandOnDuckDB(t *testing.T) {
	dbPath := filepath.ToSlash(filepath.Join(t.TempDir(), "analytics.duckdb"))
	client, err := duck.NewClient(duck.Config{Path: dbPath})
	require.NoError(t, err)
	require.NoError(t, client.RunQueryWithoutResult(t.Context(), &query.Query{Query: `
CREATE SCHEMA raw;
CREATE SCHEMA dev_raw;
CREATE SCHEMA mart;
CREATE SCHEMA dev_mart;
CREATE TABLE raw.events (id INT);
CREATE TABLE raw.orders (id INT);
CREATE TABLE dev_raw.events (id INT);
CREATE TABLE mart.daily (id INT);
`}))

	configFile := writeRenderStdinConfig(t, fmt.Sprintf(`
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: local
          path: '%[1]s'
  dev:
    schema_prefix: dev_
    connections:
      duckdb:
        - name: local
          path: '%[1]s'
`, dbPath))

	tests := []struct {
		name  string
		env   string
		this  string
		stdin string
		want  string
	}{
		{
			name:  "no prefix leaves the query as rendered",
			stdin: "SELECT '{{ schema_prefix }}' AS p, * FROM raw.events",
			want:  "SELECT '' AS p, * FROM raw.events",
		},
		{
			name:  "schema_prefix renders the environment prefix",
			env:   "dev",
			stdin: "SELECT '{{ schema_prefix }}' AS p",
			want:  "SELECT 'dev_' AS p",
		},
		{
			name:  "only tables with a prefixed copy are rewritten",
			env:   "dev",
			stdin: "SELECT * FROM raw.events e JOIN raw.orders o ON e.id = o.id",
			want:  "SELECT * FROM dev_raw.events AS e JOIN raw.orders AS o ON e.id = o.id",
		},
		{
			name:  "a {{ schema_prefix }} reference is not prefixed again",
			env:   "dev",
			stdin: "SELECT * FROM {{ schema_prefix }}raw.events",
			want:  "SELECT * FROM dev_raw.events",
		},
		{
			name:  "quoted name keeps the table quoted",
			env:   "dev",
			stdin: `SELECT * FROM "raw"."events"`,
			want:  `SELECT * FROM dev_raw."events"`,
		},
		{
			name:  "differently cased name is left alone",
			env:   "dev",
			stdin: "SELECT * FROM RAW.EVENTS",
			want:  "SELECT * FROM RAW.EVENTS",
		},
		{
			name:  "DDL without tables to rewrite is unchanged",
			env:   "dev",
			stdin: "CREATE TABLE raw.orders_copy AS SELECT * FROM raw.orders",
			want:  "CREATE TABLE raw.orders_copy AS SELECT * FROM raw.orders",
		},
		{
			name:  "BRUIN_THIS points self references at the prefixed asset table",
			env:   "dev",
			this:  "dev_mart.daily",
			stdin: "SELECT * FROM raw.events WHERE id > (SELECT MAX(id) FROM mart.daily)",
			want:  "SELECT * FROM dev_raw.events WHERE id > (SELECT MAX(id) FROM dev_mart.daily)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The Python SDK passes the asset name through the environment.
			t.Setenv("BRUIN_THIS", tt.this)

			args := []string{"--stdin", "--connection", "local", "--config-file", configFile, "--raw-query", "--output", "json"}
			if tt.env != "" {
				args = append(args, "--environment", tt.env)
			}
			got, err := runRenderCommand(t, tt.stdin, args...)
			require.NoError(t, err)
			assert.JSONEq(t, fmt.Sprintf(`{"query":%q}`, tt.want), got)
		})
	}
}

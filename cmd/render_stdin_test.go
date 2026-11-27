package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

type summaryConnection struct {
	db *ansisql.DBDatabase
}

func (c summaryConnection) GetDatabaseSummary(context.Context) (*ansisql.DBDatabase, error) {
	return c.db, nil
}

func dbSchema(name string, tables ...string) *ansisql.DBSchema {
	schema := &ansisql.DBSchema{Name: name}
	for _, table := range tables {
		schema.Tables = append(schema.Tables, &ansisql.DBTable{Name: table})
	}
	return schema
}

func TestPrefixStdinQuery(t *testing.T) {
	t.Parallel()

	conn := summaryConnection{db: &ansisql.DBDatabase{Name: "analytics", Schemas: []*ansisql.DBSchema{
		dbSchema("raw", "events", "orders"),
		dbSchema("dev_raw", "events"),
		dbSchema("mart", "daily", "weekly"),
		dbSchema("dev_mart", "weekly"),
	}}}

	tests := []struct {
		name string
		this string
		sql  string
		want string
	}{
		{
			name: "table with a prefixed copy is rewritten",
			sql:  "SELECT * FROM raw.events",
			want: "SELECT * FROM dev_raw.events",
		},
		{
			name: "table without a prefixed copy is left alone",
			sql:  "SELECT * FROM raw.orders",
			want: "SELECT * FROM raw.orders",
		},
		{
			name: "each table is checked on its own",
			sql:  "SELECT * FROM raw.events e JOIN raw.orders o ON e.id = o.id",
			want: "SELECT * FROM dev_raw.events AS e JOIN raw.orders AS o ON e.id = o.id",
		},
		{
			name: "already prefixed table is not prefixed again",
			sql:  "SELECT * FROM dev_raw.events",
			want: "SELECT * FROM dev_raw.events",
		},
		{
			name: "quoted name of a prefixed table keeps the table quoted",
			sql:  `SELECT * FROM "raw"."events"`,
			want: `SELECT * FROM dev_raw."events"`,
		},
		{
			name: "quoted name in another case is left alone",
			sql:  `SELECT * FROM "Raw"."Events"`,
			want: `SELECT * FROM "Raw"."Events"`,
		},
		{
			name: "differently cased name is left alone",
			sql:  "SELECT * FROM RAW.EVENTS",
			want: "SELECT * FROM RAW.EVENTS",
		},
		{
			name: "statement without tables is unchanged",
			sql:  "CREATE SCHEMA IF NOT EXISTS raw",
			want: "CREATE SCHEMA IF NOT EXISTS raw",
		},
		{
			name: "DDL target without a prefixed copy is left alone",
			sql:  "CREATE TABLE raw.orders_copy AS SELECT * FROM raw.orders",
			want: "CREATE TABLE raw.orders_copy AS SELECT * FROM raw.orders",
		},
		{
			name: "DML target with a prefixed copy is rewritten, like bruin query",
			sql:  "INSERT INTO raw.events SELECT * FROM raw.orders",
			want: "INSERT INTO dev_raw.events SELECT * FROM raw.orders",
		},
		{
			name: "self reference points at the prefixed asset table even before it exists",
			this: "dev_mart.daily",
			sql:  "SELECT * FROM raw.events WHERE id > (SELECT MAX(id) FROM mart.daily)",
			want: "SELECT * FROM dev_raw.events WHERE id > (SELECT MAX(id) FROM dev_mart.daily)",
		},
		{
			name: "self reference with an existing prefixed table is prefixed once",
			this: "dev_mart.weekly",
			sql:  "SELECT * FROM mart.weekly",
			want: "SELECT * FROM dev_mart.weekly",
		},
		{
			name: "differently cased self reference is left alone, like bruin run",
			this: "dev_mart.daily",
			sql:  "SELECT * FROM MART.daily",
			want: "SELECT * FROM MART.daily",
		},
		{
			name: "other tables of the asset's schema are not self references",
			this: "dev_mart.daily",
			sql:  "SELECT * FROM mart.weekly JOIN mart.daily USING (id)",
			want: "SELECT * FROM dev_mart.weekly JOIN dev_mart.daily USING (id)",
		},
	}

	for _, dialect := range []string{"duckdb", "postgres"} {
		for _, tt := range tests {
			t.Run(dialect+"/"+tt.name, func(t *testing.T) {
				t.Parallel()

				got, err := prefixStdinQuery(t.Context(), tt.sql, dialect, "dev_", tt.this, "conn", conn)
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			})
		}
	}
}

func TestPrefixStdinQueryMatchesBruinQuery(t *testing.T) {
	t.Parallel()

	conn := summaryConnection{db: &ansisql.DBDatabase{Name: "analytics", Schemas: []*ansisql.DBSchema{
		dbSchema("raw", "events", "orders"),
		dbSchema("dev_raw", "events"),
	}}}
	parser, err := sqlparser.NewSQLParser(false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parser.Close() })
	require.NoError(t, parser.Start())

	for _, sql := range []string{
		"select id from raw.events where id > 1",
		"SELECT 1; SELECT * FROM raw.events",
		"WITH e AS (SELECT * FROM raw.events) SELECT * FROM e JOIN raw.orders USING (id)",
		"DROP TABLE raw.events",
	} {
		want, err := applySchemaPrefix(t.Context(), sql, "duckdb", "dev_", false, parser, "conn", conn)
		require.NoError(t, err)
		got, err := prefixStdinQuery(t.Context(), sql, "duckdb", "dev_", "", "conn", conn)
		require.NoError(t, err)
		assert.Equal(t, want, got, sql)
	}
}

func TestPrefixStdinQueryRequiresDevEnvironmentSupport(t *testing.T) {
	t.Parallel()

	_, err := prefixStdinQuery(t.Context(), "SELECT * FROM raw.events", "duckdb", "dev_", "", "conn", struct{}{})
	require.ErrorContains(t, err, "the connection 'conn' does not support developer environments")
}

func TestParseRenderStdinVars(t *testing.T) {
	t.Parallel()

	got, err := parseRenderStdinVars([]string{
		`name="alice"`,
		`count=10`,
		`ratio=0.5`,
		`enabled=true`,
		`missing=null`,
		`id=18446744073709551615`,
		`ids=[1, 18446744073709551616]`,
		`filters={"start_date":"2024-01-01","limit":5}`,
		`day=2024-01-01`,
		`empty=`,
		`nested.key="value"`,
		`trailing="a" "b"`,
	})
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"name":     "alice",
		"count":    int64(10),
		"ratio":    0.5,
		"enabled":  true,
		"missing":  nil,
		"id":       "18446744073709551615",
		"ids":      []any{int64(1), "18446744073709551616"},
		"filters":  map[string]any{"start_date": "2024-01-01", "limit": int64(5)},
		"day":      "2024-01-01",
		"empty":    "",
		"nested":   map[string]any{"key": "value"},
		"trailing": `"a" "b"`,
	}, got)

	_, err = parseRenderStdinVars([]string{"no-equals-sign"})
	require.ErrorContains(t, err, "must be in key=value format")
}

// renderStdinTestConfig has a Postgres connection that is never dialed: pgx
// connects lazily, and these tests only render queries that need no database.
const renderStdinTestConfig = `
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: pg
          host: 127.0.0.1
          port: 1
          username: user
          database: db
  cloud_dev:
    schema_prefix: dev_
    connections:
      postgres:
        - name: pg
          use_cloud: true
`

func writeRenderStdinConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".bruin.yml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// runRenderCommand runs `bruin render` with stdin and returns what it wrote.
func runRenderCommand(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	out := &bytes.Buffer{}
	root := &cli.Command{
		Name:           "bruin",
		Reader:         strings.NewReader(stdin),
		Writer:         out,
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
		Commands:       []*cli.Command{Render()},
	}
	err := root.Run(t.Context(), append([]string{"bruin", "render"}, args...))
	return out.String(), err
}

// renderStdinError runs `bruin render --stdin` and returns the error it fails
// with, which the command itself only prints.
func renderStdinError(t *testing.T, stdin string, args ...string) error {
	t.Helper()
	command := Render()
	var renderErr error
	command.Action = func(ctx context.Context, c *cli.Command) error {
		_, renderErr = renderStdin(ctx, c, afero.NewOsFs())
		return nil
	}
	root := &cli.Command{
		Name:     "bruin",
		Reader:   strings.NewReader(stdin),
		Writer:   &bytes.Buffer{},
		Commands: []*cli.Command{command},
	}
	require.NoError(t, root.Run(t.Context(), append([]string{"bruin", "render", "--stdin"}, args...)))
	return renderErr
}

//nolint:paralleltest // urfave/cli mutates the shared date flag definitions during setup.
func TestRenderStdinCommandOutput(t *testing.T) {
	configFile := writeRenderStdinConfig(t, renderStdinTestConfig)

	tests := []struct {
		name  string
		stdin string
		args  []string
		want  string
	}{
		{
			name:  "json output is the rendered query",
			stdin: "SELECT * FROM raw.events WHERE dt >= '{{ start_date }}'",
			args:  []string{"--start-date", "2024-01-02", "--output", "json", "--raw-query"},
			want:  `{"query":"SELECT * FROM raw.events WHERE dt >= '2024-01-02'"}`,
		},
		{
			name:  "plain output is the rendered query on its own line",
			stdin: "  SELECT '{{ end_date }}'\n",
			args:  []string{"--end-date", "2024-01-03"},
			want:  "SELECT '2024-01-03'\n",
		},
		{
			name:  "typed variables are available at the top level and under var",
			stdin: "SELECT {{ count }} AS c, '{{ name }}' AS n, '{{ var.name }}' AS v, {{ id }} AS id",
			args: []string{
				"--output", "json",
				"--var", "count=10", "--var", `name="alice"`, "--var", "id=18446744073709551615",
			},
			want: `{"query":"SELECT 10 AS c, 'alice' AS n, 'alice' AS v, 18446744073709551615 AS id"}`,
		},
		{
			name:  "full refresh is exposed to the template",
			stdin: "SELECT {{ full_refresh }}",
			args:  []string{"--full-refresh", "--output", "json"},
			want:  `{"query":"SELECT True"}`,
		},
		{
			name:  "schema prefix is empty without an environment prefix",
			stdin: "SELECT '{{ schema_prefix }}' AS prefix FROM raw.events",
			args:  []string{"--output", "json"},
			want:  `{"query":"SELECT '' AS prefix FROM raw.events"}`,
		},
		{
			name:  "cloud connections render templates but leave tables as written",
			stdin: "SELECT '{{ schema_prefix }}' AS prefix FROM raw.events",
			args:  []string{"--environment", "cloud_dev", "--output", "json"},
			want:  `{"query":"SELECT 'dev_' AS prefix FROM raw.events"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--stdin", "--connection", "pg", "--config-file", configFile}, tt.args...)
			got, err := runRenderCommand(t, tt.stdin, args...)
			require.NoError(t, err)
			if strings.HasPrefix(tt.want, "{") {
				// One JSON object holding only the query, which the Python SDK reads.
				assert.JSONEq(t, tt.want, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

//nolint:paralleltest // urfave/cli mutates the shared date flag definitions during setup.
func TestRenderStdinCommandErrors(t *testing.T) {
	configFile := writeRenderStdinConfig(t, renderStdinTestConfig)

	tests := []struct {
		name    string
		stdin   string
		args    []string
		wantErr string
	}{
		{
			name:    "connection is required",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile},
			wantErr: "--connection is required with --stdin",
		},
		{
			name:    "unknown connection",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile, "--connection", "nope"},
			wantErr: "connection 'nope' not found",
		},
		{
			name:    "unknown environment",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile, "--connection", "pg", "--environment", "nope"},
			wantErr: "failed to use the environment 'nope'",
		},
		{
			name:    "missing config file",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", filepath.Join(t.TempDir(), "missing.yml"), "--connection", "pg"},
			wantErr: "failed to load or create config",
		},
		{
			name:    "empty stdin",
			stdin:   " \n ",
			args:    []string{"--config-file", configFile, "--connection", "pg"},
			wantErr: "no SQL given on standard input",
		},
		{
			name:    "asset path cannot be combined with stdin",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile, "--connection", "pg", "assets/my_asset.sql"},
			wantErr: "cannot be combined with an asset path",
		},
		{
			name:    "asset-only flags are rejected",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile, "--connection", "pg", "--variant", "eu"},
			wantErr: "only apply to asset files",
		},
		{
			name:    "invalid start date",
			stdin:   "SELECT 1",
			args:    []string{"--config-file", configFile, "--connection", "pg", "--start-date", "yesterday"},
			wantErr: `invalid --start-date "yesterday"`,
		},
		{
			name:    "template errors are reported",
			stdin:   "SELECT {{ broken",
			args:    []string{"--config-file", configFile, "--connection", "pg"},
			wantErr: "could not render",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := renderStdinError(t, tt.stdin, tt.args...)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

//nolint:paralleltest // urfave/cli mutates the shared date flag definitions during setup.
func TestRenderStdinCommandFailureOutput(t *testing.T) {
	configFile := writeRenderStdinConfig(t, renderStdinTestConfig)
	args := []string{"--stdin", "--connection", "nope", "--config-file", configFile}

	assertExitCode := func(t *testing.T, err error) {
		t.Helper()
		var exitErr cli.ExitCoder
		require.ErrorAs(t, err, &exitErr)
		assert.Equal(t, 1, exitErr.ExitCode())
	}

	t.Run("json output is an error object on stdout and exit code 1", func(t *testing.T) {
		got, err := runRenderCommand(t, "SELECT 1", append(args, "--output", "json")...)
		assertExitCode(t, err)

		var payload map[string]string
		require.NoError(t, json.Unmarshal([]byte(got), &payload))
		assert.Len(t, payload, 1)
		assert.Contains(t, payload["error"], "connection 'nope' not found")
	})

	t.Run("plain output keeps stdout empty and exits with code 1", func(t *testing.T) {
		got, err := runRenderCommand(t, "SELECT 1", args...)
		assertExitCode(t, err)
		assert.Empty(t, got)
	})
}

//nolint:paralleltest // urfave/cli mutates the shared date flag definitions during setup.
func TestRenderCommandRejectsConnectionWithoutStdin(t *testing.T) {
	_, err := runRenderCommand(t, "", "--connection", "pg", "assets/my_asset.sql")
	require.Error(t, err)
}

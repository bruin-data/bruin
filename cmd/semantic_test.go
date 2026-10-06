package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/semanticcheck"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestSemanticCommand_Help(t *testing.T) {
	t.Parallel()
	isDebug := false
	cmd := Semantic(&isDebug)
	require.NotNil(t, cmd)
	assert.Equal(t, "semantic", cmd.Name)
	require.Len(t, cmd.Commands, 2)

	names := make([]string, 0, len(cmd.Commands))
	for _, sub := range cmd.Commands {
		names = append(names, sub.Name)
		flagNames := make([]string, 0, len(sub.Flags))
		for _, flag := range sub.Flags {
			flagNames = append(flagNames, flag.Names()[0])
		}
		assert.ElementsMatch(t, []string{"environment", "config-file", "connection", "model", "output"}, flagNames, sub.Name)
	}
	assert.ElementsMatch(t, []string{"validate", "check"}, names)
}

func TestSelectSemanticModels(t *testing.T) {
	t.Parallel()
	models := map[string]*semantic.Model{
		"orders":    {Name: "orders"},
		"customers": {Name: "customers"},
	}
	invalid := map[string]error{"broken.yml": errors.New("bad yaml")}

	all, err := selectSemanticModels(models, invalid, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"broken.yml", "customers", "orders"}, all)

	some, err := selectSemanticModels(models, invalid, []string{"orders", " orders ", "broken.yml"})
	require.NoError(t, err)
	assert.Equal(t, []string{"broken.yml", "orders"}, some)

	_, err = selectSemanticModels(models, invalid, []string{"missing"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `semantic model "missing" not found; available models: customers, orders`)
	assert.Contains(t, err.Error(), "broken.yml: bad yaml")
}

func TestSemanticCheckDialect(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "spark", semanticCheckDialect("sail"))
	assert.Equal(t, "bigquery", semanticCheckDialect("google_cloud_platform"))
	assert.Empty(t, semanticCheckDialect("unknown"))
}

func TestResolveSemanticConfigPath(t *testing.T) {
	t.Parallel()
	path, err := resolveSemanticConfigPath("/tmp/custom.yml", "")
	require.NoError(t, err)
	assert.Equal(t, "/tmp/custom.yml", path)

	path, err = resolveSemanticConfigPath("", ".")
	require.NoError(t, err)
	assert.Equal(t, ".bruin.yml", filepath.Base(path))
}

func TestSemanticSchemaPrefixer(t *testing.T) {
	t.Parallel()
	checks := []semantic.CompiledCheck{{SQL: "SELECT count(*) FROM analytics.orders", ValidationSQL: "SELECT 1 FROM analytics.orders"}}

	var noPrefix *semanticSchemaPrefixer
	require.NoError(t, noPrefix.rewriteChecks(checks, "duckdb"))
	assert.Equal(t, "SELECT count(*) FROM analytics.orders", checks[0].SQL)

	prefixer := &semanticSchemaPrefixer{prefix: "dev_"}
	defer prefixer.Close()
	require.NoError(t, prefixer.rewriteChecks(checks, "duckdb"))
	assert.Contains(t, checks[0].SQL, "dev_analytics.orders")
	assert.Contains(t, checks[0].ValidationSQL, "dev_analytics.orders")

	// Sail checks are generated as Spark SQL; the rewrite must keep RLIKE,
	// since Sail has no REGEXP_LIKE.
	sail := []semantic.CompiledCheck{{SQL: "SELECT count(*) FROM analytics.orders WHERE NOT (country RLIKE '^[A-Z]{2}$')"}}
	require.NoError(t, prefixer.rewriteChecks(sail, semanticCheckDialect("sail")))
	assert.Contains(t, sail[0].SQL, "dev_analytics.orders")
	assert.Contains(t, sail[0].SQL, "RLIKE")
	assert.NotContains(t, sail[0].SQL, "REGEXP_LIKE")
}

func TestSemanticResultLabelAndPluralize(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "dimension country: not_null", semanticResultLabel(semanticcheck.Result{Scope: semantic.CheckScopeDimension, Target: "country", Name: "not_null"}))
	assert.Equal(t, "metric revenue: positive", semanticResultLabel(semanticcheck.Result{Scope: semantic.CheckScopeMetric, Target: "revenue", Name: "positive"}))
	assert.Equal(t, "check no_negative", semanticResultLabel(semanticcheck.Result{Scope: semantic.CheckScopeModel, Name: "no_negative"}))
	assert.Equal(t, "1 check", pluralize(1, "check", "checks"))
	assert.Equal(t, "0 checks", pluralize(0, "check", "checks"))
}

const semanticTestOrdersModel = `schema: v1
name: orders
source:
  connection: duckdb-semantic
  table: |
    (
      SELECT 1 AS order_id, 101 AS customer_id, 'US' AS country, 'completed' AS status, 100 AS amount
      UNION ALL SELECT 2, 102, 'US', 'completed', 50
      UNION ALL SELECT 3, 101, 'DE', 'pending', 70
    ) AS orders
primary_key: order_id
joins:
  - name: customers
    relationship: many_to_one
    foreign_key: customer_id
dimensions:
  - name: order_id
    type: number
    checks:
      - name: not_null
      - name: unique
  - name: country
    type: string
    checks:
      - name: accepted_values
        value: [US, DE]
      - name: pattern
        value: "^[A-Z]{2}$"
  - name: status
    type: string
metrics:
  - name: revenue
    expression: sum(amount)
    checks:
      - name: positive
      - name: equals
        value: 220
  - name: order_count
    expression: count(distinct order_id)
    checks:
      - name: max
        value: 2
segments:
  - name: completed
    filter: "status = 'completed'"
checks:
  - name: completed_revenue
    query:
      metrics: [revenue]
      segments: [completed]
    value: 140
  - name: customer_countries
    query:
      dimensions: [customers.country]
    count: 1
  - name: no_negative_amounts
    query:
      dimensions: [order_id]
      filters:
        - expression: amount < 0
    count: 0
  - name: revenue_by_country
    query:
      dimensions: [country]
      metrics: [revenue]
      sort: [revenue:desc]
    value:
      - country: US
        revenue: 150
      - country: DE
        revenue: 70
  - name: revenue_by_customer_country
    query:
      dimensions: [customers.country]
      metrics: [revenue, order_count]
    value:
      - customers.country: US
        revenue: 220
        order_count: 3
`

const semanticTestCustomersModel = `schema: v1
name: customers
source:
  connection: duckdb-semantic
  table: |
    (
      SELECT 101 AS customer_id, 'US' AS country
      UNION ALL SELECT 102, 'US'
    ) AS customers
primary_key: customer_id
dimensions:
  - name: country
    type: string
    checks:
      - name: not_null
`

func writeSemanticTestRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dbPath := filepath.Join(root, "semantic.db")
	configContent := "default_environment: default\nenvironments:\n  default:\n    connections:\n      duckdb:\n        - name: duckdb-semantic\n          path: " + dbPath + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, ".bruin.yml"), []byte(configContent), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "semantic", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "orders.yml"), []byte(semanticTestOrdersModel), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "nested", "customers.yml"), []byte(semanticTestCustomersModel), 0o600))
	return root
}

func runSemanticCommandCapturingStdout(t *testing.T, args ...string) (string, int) {
	t.Helper()
	isDebug := false
	exitCode := 0
	app := &cli.Command{
		Name:     "bruin",
		Commands: []*cli.Command{Semantic(&isDebug)},
		ExitErrHandler: func(_ context.Context, _ *cli.Command, err error) {
			var exitErr cli.ExitCoder
			if errors.As(err, &exitErr) {
				exitCode = exitErr.ExitCode()
			} else if err != nil {
				exitCode = 1
			}
		},
	}

	original := os.Stdout
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writer

	runErr := app.Run(t.Context(), append([]string{"bruin", "semantic"}, args...))

	os.Stdout = original
	require.NoError(t, writer.Close())
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	require.NoError(t, err)

	if runErr != nil && exitCode == 0 {
		exitCode = 1
	}
	return buf.String(), exitCode
}

func skipSemanticDuckDBTest(t *testing.T) {
	t.Helper()
	if runtime.GOOS == osWindows {
		t.Skip("skipping on Windows due to DuckDB file locking")
	}
	if testing.Short() {
		t.Skip("skipping integration test")
	}
}

func TestSemanticValidateExpressionCoverage(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
	skipSemanticDuckDBTest(t)
	const model = `name: orders
source:
  connection: duckdb-semantic
  table: "(VALUES (DATE '2026-01-01', 10), (DATE '2026-01-02', 20)) AS t(order_date, amount)"
dimensions:
  - name: order_date
    type: time
metrics:
  - name: revenue
    expression: sum(amount)
  - name: order_count
    expression: count(*)
  - name: raw_aov
    expression: sum(amount) / {order_count}
  - name: running_revenue
    expression: "{revenue}"
    window:
      type: running_total
      order_by: order_date
`
	for _, tc := range []struct { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		name, yaml string
		code       int
	}{
		{name: "independent aggregate and window metrics", yaml: model},
		{name: "aggregate segment with dimension", yaml: model + "segments:\n  - name: completed\n    filter: '{order_date} IS NOT NULL AND {revenue} > 0'\n"},
		{name: "invalid segment", yaml: model + "segments:\n  - name: broken\n    filter: missing_column > 0\n", code: 1},
		{name: "invalid granularity", yaml: strings.Replace(model, "    type: time", "    type: time\n    granularities:\n      month: date_trunc('month', missing_column)", 1), code: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeSemanticTestRepo(t)
			require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "orders.yml"), []byte(tc.yaml), 0o600))
			out, code := runSemanticCommandCapturingStdout(t, "validate", "--config-file", filepath.Join(root, ".bruin.yml"), "--model", "orders", "--output", "json")
			require.Equal(t, tc.code, code, out)
			var report semanticValidationReport
			require.NoError(t, json.Unmarshal([]byte(out), &report))
			assert.Equal(t, tc.code == 0, report.Valid, out)
			if tc.code != 0 {
				assert.Contains(t, out, "missing_column")
			}
		})
	}
}

func TestSemanticCheckExpressionAndBoolean(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
	skipSemanticDuckDBTest(t)
	root := writeSemanticTestRepo(t)
	const model = `name: orders
source:
  connection: duckdb-semantic
  table: "(SELECT NULL AS a, 1 AS b) AS t"
dimensions:
  - name: any_null
    expression: a IS NULL OR b IS NULL
    checks:
      - name: not_null
metrics:
  - name: valid
    expression: bool_and(b = 1)
    checks:
      - name: equals
        value: true
  - name: latest_date
    expression: max(DATE '2025-01-15')
    checks:
      - name: equals
        value: '2025-01-15'
      - name: min
        value: '2025-01-15'
      - name: max
        value: '2025-01-15'
checks:
  - name: row_count
    query:
      dimensions: [any_null]
    count: 1
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "orders.yml"), []byte(model), 0o600))
	out, code := runSemanticCommandCapturingStdout(t, "check", "--config-file", filepath.Join(root, ".bruin.yml"), "--model", "orders", "--output", "json")
	require.Equal(t, 0, code, out)
	var report semanticCheckReport
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	assert.Equal(t, 6, report.Summary.Passed, out)
}

func TestSemanticValidateAndCheck_DuckDB(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
	skipSemanticDuckDBTest(t)
	root := writeSemanticTestRepo(t)
	configFile := filepath.Join(root, ".bruin.yml")

	t.Run("validate passes against the warehouse", func(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		out, code := runSemanticCommandCapturingStdout(t, "validate", "--config-file", configFile, "--output", "json")
		require.Equal(t, 0, code, out)

		var report semanticValidationReport
		require.NoError(t, json.Unmarshal([]byte(out), &report))
		assert.True(t, report.Valid)
		assert.Equal(t, filepath.Join(root, "semantic"), report.Path)
		require.Len(t, report.Models, 2)

		assert.Equal(t, "customers", report.Models[0].Name)
		assert.Equal(t, "duckdb-semantic", report.Models[0].Connection)
		assert.Equal(t, 1, report.Models[0].Checks)

		orders := report.Models[1]
		assert.Equal(t, "orders", orders.Name)
		assert.True(t, orders.Valid)
		assert.Equal(t, 12, orders.Checks)
		assert.Empty(t, orders.Errors)
		require.Len(t, orders.Results, 12)
		for _, result := range orders.Results {
			assert.Equal(t, semanticcheck.StatusPassed, result.Status, result.ID)
		}
	})

	t.Run("check runs every check and reports pass and fail", func(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		out, code := runSemanticCommandCapturingStdout(t, "check", "--config-file", configFile, "--output", "json")
		require.Equal(t, 1, code, out)

		var report semanticCheckReport
		require.NoError(t, json.Unmarshal([]byte(out), &report))
		assert.Empty(t, report.Errors)
		assert.Equal(t, semanticcheck.Summary{Passed: 11, Failed: 2}, report.Summary)

		byID := make(map[string]semanticcheck.Result, len(report.Results))
		for _, result := range report.Results {
			byID[result.ID] = result
		}
		assert.Equal(t, semanticcheck.StatusPassed, byID["customers.dimension.country.not_null"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.dimension.order_id.unique"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.dimension.country.pattern"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.metric.revenue.equals"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.check.customer_countries"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.check.no_negative_amounts"].Status)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.check.revenue_by_country"].Status, byID["orders.check.revenue_by_country"].Message)
		assert.Equal(t, semanticcheck.StatusPassed, byID["orders.check.revenue_by_customer_country"].Status, byID["orders.check.revenue_by_customer_country"].Message)

		assert.Equal(t, semanticcheck.StatusFailed, byID["orders.metric.order_count.max"].Status)
		assert.Equal(t, "metric 'order_count' is 3, above maximum 2", byID["orders.metric.order_count.max"].Message)
		assert.Equal(t, semanticcheck.StatusFailed, byID["orders.check.completed_revenue"].Status)
		assert.Equal(t, "check returned 150, expected 140", byID["orders.check.completed_revenue"].Message)
	})

	t.Run("model filter and connection override", func(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		out, code := runSemanticCommandCapturingStdout(t, "check", "--config-file", configFile, "--output", "json", "--model", "customers", "--connection", "duckdb-semantic")
		require.Equal(t, 0, code, out)

		var report semanticCheckReport
		require.NoError(t, json.Unmarshal([]byte(out), &report))
		require.Len(t, report.Results, 1)
		assert.Equal(t, "customers.dimension.country.not_null", report.Results[0].ID)
		assert.Equal(t, semanticcheck.Summary{Passed: 1}, report.Summary)
	})

	t.Run("unknown connection override fails validation", func(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		out, code := runSemanticCommandCapturingStdout(t, "validate", "--config-file", configFile, "--output", "json", "--connection", "nope")
		require.Equal(t, 1, code, out)

		var report semanticValidationReport
		require.NoError(t, json.Unmarshal([]byte(out), &report))
		assert.False(t, report.Valid)
		for _, model := range report.Models {
			assert.False(t, model.Valid)
			require.NotEmpty(t, model.Errors)
			assert.Contains(t, model.Errors[0], "nope")
		}
	})

	t.Run("unknown model name is rejected", func(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
		out, code := runSemanticCommandCapturingStdout(t, "check", "--config-file", configFile, "--output", "json", "--model", "missing")
		require.Equal(t, 1, code, out)
		assert.Contains(t, out, `semantic model \"missing\" not found`)
	})
}

func TestSemanticValidate_WithoutConnectionAndWithInvalidModel(t *testing.T) { //nolint:paralleltest // runSemanticCommandCapturingStdout swaps the global os.Stdout
	skipSemanticDuckDBTest(t)
	root := writeSemanticTestRepo(t)
	configFile := filepath.Join(root, ".bruin.yml")

	noConnection := `name: events
source:
  table: raw.events
dimensions:
  - name: id
    checks:
      - name: not_null
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "events.yml"), []byte(noConnection), 0o600))
	broken := `name: broken
source:
  table: raw.broken
metrics:
  - name: x
    expression: count(*)
    checks:
      - name: unique
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "broken.yml"), []byte(broken), 0o600))
	badQuery := `name: bad_query
source:
  connection: duckdb-semantic
  table: (SELECT 1 AS id) AS bad_query
checks:
  - name: refers_to_missing_metric
    query:
      metrics: [missing]
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "bad_query.yml"), []byte(badQuery), 0o600))

	out, code := runSemanticCommandCapturingStdout(t, "validate", "--config-file", configFile, "--output", "json")
	require.Equal(t, 1, code, out)

	var report semanticValidationReport
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	assert.False(t, report.Valid)

	byName := make(map[string]semanticModelValidation, len(report.Models))
	for _, model := range report.Models {
		byName[model.Name] = model
	}

	events := byName["events"]
	assert.True(t, events.Valid)
	assert.Empty(t, events.Connection)
	require.Len(t, events.Warnings, 1)
	assert.Contains(t, events.Warnings[0], "no connection configured")

	brokenModel := byName["broken"]
	assert.False(t, brokenModel.Valid)
	require.NotEmpty(t, brokenModel.Errors)
	assert.Contains(t, brokenModel.Errors[0], `metric "x": unknown check "unique"`)

	badQueryModel := byName["bad_query"]
	assert.False(t, badQueryModel.Valid)
	require.NotEmpty(t, badQueryModel.Errors)
	assert.Contains(t, badQueryModel.Errors[0], "metric not found: missing")

	assert.True(t, byName["orders"].Valid)
	assert.True(t, byName["customers"].Valid)

	checkOut, checkCode := runSemanticCommandCapturingStdout(t, "check", "--config-file", configFile, "--output", "json", "--model", "events")
	require.Equal(t, 1, checkCode, checkOut)
	var checkReport semanticCheckReport
	require.NoError(t, json.Unmarshal([]byte(checkOut), &checkReport))
	require.Len(t, checkReport.Errors, 1)
	assert.Contains(t, checkReport.Errors[0], "model 'events' has no connection")

	noChecks := `name: plain
source:
  connection: duckdb-semantic
  table: (SELECT 1 AS id) AS plain
dimensions:
  - name: id
`
	require.NoError(t, os.WriteFile(filepath.Join(root, "semantic", "plain.yml"), []byte(noChecks), 0o600))
	emptyOut, emptyCode := runSemanticCommandCapturingStdout(t, "check", "--config-file", configFile, "--output", "json", "--model", "plain")
	require.Equal(t, 1, emptyCode, emptyOut)
	assert.Contains(t, emptyOut, "define no quality checks")
}

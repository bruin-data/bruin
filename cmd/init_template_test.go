//nolint:paralleltest
package cmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/templates"
	"github.com/stretchr/testify/require"
)

func TestInitSelfHealDemoCopiesPipelineTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "self-heal-demo"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "self-heal-demo")
	require.FileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "README.md"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "source_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "duplicate_silver_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "quality_silver_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "freshness_silver_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "schema_drift_silver_orders.sql"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, ".agents"))

	configContent, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: self-heal-demo")
	require.Contains(t, string(configContent), "path: self-heal-demo.duckdb")
}

func TestSelfHealDemoTemplateContainsDataProblemScenarios(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	duplicate := readTemplate("self-heal-demo/assets/duplicate_silver_orders.sql")
	require.Contains(t, duplicate, "name: duplicate.silver_orders")
	require.Contains(t, duplicate, "duplicate-investigate")
	require.Contains(t, duplicate, "SELECT * FROM typed_orders WHERE order_id = 1002")

	quality := readTemplate("self-heal-demo/assets/quality_silver_orders.sql")
	require.Contains(t, quality, "name: quality.silver_orders")
	require.Contains(t, quality, "quality-check-investigate")
	require.Contains(t, quality, "CASE WHEN order_id = 1003 THEN -amount ELSE amount END AS amount")

	freshness := readTemplate("self-heal-demo/assets/freshness_silver_orders.sql")
	require.Contains(t, freshness, "name: freshness.silver_orders")
	require.Contains(t, freshness, "freshness-check")
	require.Contains(t, freshness, "transaction_date < DATE '2025-01-03'")

	schemaDriftBronze := readTemplate("self-heal-demo/assets/schema_drift_bronze_orders.sql")
	schemaDriftSilver := readTemplate("self-heal-demo/assets/schema_drift_silver_orders.sql")
	require.Contains(t, schemaDriftBronze, "name: schema_drift.bronze_orders")
	require.Contains(t, schemaDriftBronze, "schema-drift-check")
	require.Contains(t, schemaDriftBronze, "amount AS gross_amount")
	require.Contains(t, schemaDriftSilver, "name: schema_drift.silver_orders")
	require.Contains(t, schemaDriftSilver, "schema-drift-check")
	require.Contains(t, schemaDriftSilver, "orders.amount AS amount")
}

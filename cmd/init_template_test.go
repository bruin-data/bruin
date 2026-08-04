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
	require.FileExists(t, filepath.Join(pipelineRoot, "README.md"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "orders.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "orders.csv"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "order_status_history.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "order_status_history.csv"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "order_adjustments.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "order_adjustments.csv"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "fulfillment_events.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "fulfillment_events.csv"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "product_catalog.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-seed", "assets", "product_catalog.csv"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "staging_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "status_snapshot.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "order_margin.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "daily_activity.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "product_prices.sql"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "queries"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "orders"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "finance"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "fulfillment"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "catalog"))
	require.NoFileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "duplicate_silver_orders.sql"))
	require.NoFileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "quality_silver_orders.sql"))
	require.NoFileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "freshness_silver_orders.sql"))
	require.NoFileExists(t, filepath.Join(pipelineRoot, "demo-pipeline", "assets", "schema_drift_silver_orders.sql"))
	require.NoDirExists(t, filepath.Join(pipelineRoot, ".agents"))

	configContent, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: self-heal-demo")
	require.Contains(t, string(configContent), "path: self-heal-demo.duckdb")
}

func TestInitShopifyClickHouseCopiesPipelineTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "shopify-clickhouse"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "shopify-clickhouse")
	require.FileExists(t, filepath.Join(pipelineRoot, "README.md"))
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "t1", "t1_orders.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "t2", "t2_orders.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "t3", "t3_daily_kpis.sql"))

	pipeline, err := os.ReadFile(filepath.Join(pipelineRoot, "pipeline.yml"))
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: shopify-clickhouse")
	require.Contains(t, string(pipeline), "shopify: \"shopify-default\"")

	ordersAsset, err := os.ReadFile(filepath.Join(pipelineRoot, "assets", "t1", "t1_orders.asset.yml"))
	require.NoError(t, err)
	require.Contains(t, string(ordersAsset), "name: shopify.t1_orders")
	require.Contains(t, string(ordersAsset), "source_connection: shopify-default")

	orderLinesAsset, err := os.ReadFile(filepath.Join(pipelineRoot, "assets", "t2", "t2_order_line_items.sql"))
	require.NoError(t, err)
	require.Contains(t, string(orderLinesAsset), "strategy: delete+insert")
	require.Contains(t, string(orderLinesAsset), "incremental_key: order_id")
}

func TestInitMigrationFivetranCopiesMigrationWorkspace(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = os.WriteFile(filepath.Join(targetRoot, ".bruin.yml"), []byte("connections:\n  duckdb: []\n  duckdb: []\n"), 0o600)
	require.NoError(t, err)

	err = Init().Run(t.Context(), []string{"init", "migration-fivetran", "my-fivetran-migration"})
	require.NoError(t, err)

	migrationRoot := filepath.Join(targetRoot, "my-fivetran-migration")
	require.FileExists(t, filepath.Join(migrationRoot, ".gitignore"))
	require.FileExists(t, filepath.Join(migrationRoot, "fivetran-bruin-prompt.md"))
	require.FileExists(t, filepath.Join(migrationRoot, "plan.md"))
	require.FileExists(t, filepath.Join(migrationRoot, "bruin", "pipeline.yml"))
	require.FileExists(t, filepath.Join(migrationRoot, "bruin", "assets", "placeholder"))
	require.FileExists(t, filepath.Join(migrationRoot, ".agents", "skills", "bruin-fivetran-migrator", "SKILL.md"))
	require.FileExists(t, filepath.Join(migrationRoot, ".agents", "skills", "bruin-fivetran-migrator", "import_fivetran.py"))

	pipeline, err := os.ReadFile(filepath.Join(migrationRoot, "bruin", "pipeline.yml"))
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: bruin")

	prompt, err := os.ReadFile(filepath.Join(migrationRoot, "fivetran-bruin-prompt.md"))
	require.NoError(t, err)
	require.Contains(t, string(prompt), "review-gated workflow")

	skill, err := os.ReadFile(filepath.Join(migrationRoot, ".agents", "skills", "bruin-fivetran-migrator", "SKILL.md"))
	require.NoError(t, err)
	require.Contains(t, string(skill), "Capture exactly one connection selected by the user.")

	gitCheckIgnore := exec.CommandContext(t.Context(), "git", "check-ignore", ".bruin.yml", ".artifacts/fivetran/capture")
	gitCheckIgnore.Dir = migrationRoot
	out, err = gitCheckIgnore.CombinedOutput()
	require.NoError(t, err, string(out))
}

func TestSelfHealDemoTemplateContainsDataProblemScenarios(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	seedPipeline := readTemplate("self-heal-demo/demo-seed/pipeline.yml")
	require.Contains(t, seedPipeline, "name: demo-seed")

	demoPipeline := readTemplate("self-heal-demo/demo-pipeline/pipeline.yml")
	require.Contains(t, demoPipeline, "name: demo-pipeline")

	seed := readTemplate("self-heal-demo/demo-seed/assets/order_status_history.asset.yml")
	require.Contains(t, seed, "name: raw.order_status_history")
	require.Contains(t, seed, "type: duckdb.seed")

	duplicate := readTemplate("self-heal-demo/demo-pipeline/assets/status_snapshot.sql")
	require.Contains(t, duplicate, "name: orders.status_snapshot")
	require.Contains(t, duplicate, "duplicate-investigate")
	require.Contains(t, duplicate, "raw.order_status_history")
	require.NotContains(t, duplicate, "name: duplicate.")
	require.NotContains(t, duplicate, "UNION ALL")

	quality := readTemplate("self-heal-demo/demo-pipeline/assets/order_margin.sql")
	require.Contains(t, quality, "name: finance.order_margin")
	require.Contains(t, quality, "quality-check-investigate")
	require.Contains(t, quality, "raw.order_adjustments")
	require.Contains(t, quality, "net_amount")
	require.NotContains(t, quality, "CASE WHEN order_id = 1003 THEN -amount ELSE amount END AS amount")

	freshness := readTemplate("self-heal-demo/demo-pipeline/assets/daily_activity.sql")
	require.Contains(t, freshness, "name: fulfillment.daily_activity")
	require.Contains(t, freshness, "freshness-check")
	require.Contains(t, freshness, "raw.fulfillment_events")
	require.NotContains(t, freshness, "transaction_date < DATE '2025-01-03'")

	schemaDrift := readTemplate("self-heal-demo/demo-pipeline/assets/product_prices.sql")
	require.Contains(t, schemaDrift, "name: catalog.product_prices")
	require.Contains(t, schemaDrift, "schema-drift-check")
	require.Contains(t, schemaDrift, "raw.product_catalog")
	require.NotContains(t, schemaDrift, "schema_drift.")

	templatesRoot, err := templates.Templates.ReadDir("self-heal-demo/demo-pipeline/assets")
	require.NoError(t, err)
	for _, entry := range templatesRoot {
		require.NotContains(t, entry.Name(), "duplicate")
		require.NotContains(t, entry.Name(), "quality")
		require.NotContains(t, entry.Name(), "freshness")
		require.NotContains(t, entry.Name(), "schema_drift")
	}
}

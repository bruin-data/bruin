//nolint:paralleltest
package cmd

import (
	iofs "io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestInitStripeBigQueryCopiesStarterTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "stripe-bigquery"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "stripe-bigquery")
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, ".gitignore"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "stripe_raw", "customer.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "stripe_reports", "monthly_subscription_kpis.sql"))
	require.NoFileExists(t, filepath.Join(pipelineRoot, "assets", "stripe_raw", "refund.asset.yml"))

	pipeline, err := os.ReadFile(filepath.Join(pipelineRoot, "pipeline.yml"))
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: stripe-bigquery")

	configContent, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: gcp-default")
	require.Contains(t, string(configContent), "name: stripe-default")
}

func TestStripeBigQueryStarterTemplateHasFocusedAssetSet(t *testing.T) {
	t.Parallel()

	expectedAssets := []string{
		"stripe_raw/customer.asset.yml",
		"stripe_raw/product.asset.yml",
		"stripe_raw/price.asset.yml",
		"stripe_raw/subscription.asset.yml",
		"stripe_raw/subscription_item.asset.yml",
		"stripe_raw/invoice.asset.yml",
		"stripe_stage/customers.sql",
		"stripe_stage/products.sql",
		"stripe_stage/prices.sql",
		"stripe_stage/subscriptions.sql",
		"stripe_stage/subscription_items.sql",
		"stripe_stage/invoices.sql",
		"stripe_stage/invoice_line_items.sql",
		"stripe_stage/subscription_item_daily_snapshot.sql",
		"stripe_stage/customer_currency_daily_mrr_snapshot.sql",
		"stripe_reports/monthly_mrr_by_customer.sql",
		"stripe_reports/monthly_mrr_movements.sql",
		"stripe_reports/monthly_subscription_kpis.sql",
		"stripe_reports/monthly_invoice_billings.sql",
	}

	var actualAssets []string
	err := iofs.WalkDir(templates.Templates, "stripe-bigquery/assets", func(path string, entry iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		actualAssets = append(actualAssets, strings.TrimPrefix(path, "stripe-bigquery/assets/"))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, actualAssets, 19)
	require.ElementsMatch(t, expectedAssets, actualAssets)

	pipeline, err := templates.Templates.ReadFile("stripe-bigquery/pipeline.yml")
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: stripe-bigquery")
	require.Contains(t, string(pipeline), "source_connection: stripe-default")
	require.Contains(t, string(pipeline), "destination: bigquery")

	readme, err := templates.Templates.ReadFile("stripe-bigquery/README.md")
	require.NoError(t, err)
	require.Contains(t, string(readme), "Stripe Billing Analytics to BigQuery")
}

func TestInitGA4SearchConsoleBigQueryCopiesStarterTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "ga4-search-console-bigquery"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "ga4-search-console-bigquery")
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "README.md"))
	require.FileExists(t, filepath.Join(pipelineRoot, ".gitignore"))
	require.FileExists(t, filepath.Join(pipelineRoot, "macros", "url.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "macros", "search.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "web_stage", "ga4_sessions.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "web_reports", "organic_landing_page_performance.sql"))

	pipeline, err := os.ReadFile(filepath.Join(pipelineRoot, "pipeline.yml"))
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: ga4-search-console-bigquery")
	require.Contains(t, string(pipeline), "google_cloud_platform: gcp-default")

	configContent, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: gcp-default")
}

func TestGA4SearchConsoleBigQueryTemplateHasFocusedAssetSet(t *testing.T) {
	t.Parallel()

	expectedAssets := []string{
		"web_stage/gsc_site_query_daily.sql",
		"web_stage/gsc_url_query_daily.sql",
		"web_stage/gsc_position_click_curve.sql",
		"web_stage/gsc_export_log.sql",
		"web_stage/ga4_sessions.sql",
		"web_stage/ga4_page_daily.sql",
		"web_reports/search_brand_split_weekly.sql",
		"web_reports/search_query_opportunities.sql",
		"web_reports/search_query_cannibalization.sql",
		"web_reports/search_page_trend.sql",
		"web_reports/search_new_and_lost_queries.sql",
		"web_reports/search_competitor_visibility.sql",
		"web_reports/organic_landing_page_performance.sql",
		"web_reports/organic_query_value.sql",
		"web_reports/organic_intent_pipeline.sql",
	}

	var actualAssets []string
	err := iofs.WalkDir(templates.Templates, "ga4-search-console-bigquery/assets", func(path string, entry iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		actualAssets = append(actualAssets, strings.TrimPrefix(path, "ga4-search-console-bigquery/assets/"))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, actualAssets, 15)
	require.ElementsMatch(t, expectedAssets, actualAssets)

	pipeline, err := templates.Templates.ReadFile("ga4-search-console-bigquery/pipeline.yml")
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: ga4-search-console-bigquery")
	// The template reads exports that already exist in BigQuery, so the dataset
	// names and the brand pattern must stay overridable without editing SQL.
	require.Contains(t, string(pipeline), "ga4_dataset:")
	require.Contains(t, string(pipeline), "search_console_dataset:")
	require.Contains(t, string(pipeline), "brand_query_pattern:")

	readme, err := templates.Templates.ReadFile("ga4-search-console-bigquery/README.md")
	require.NoError(t, err)
	require.Contains(t, string(readme), "Google Analytics and Search Console Reporting on BigQuery")
}

func TestGA4SearchConsoleBigQueryTemplateReadsExistingExports(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	// Both GA4 models must pick their export tables through the shared macro. A
	// property exporting only continuously has no events_YYYYMMDD tables at all, so
	// a hard-coded daily filter returns an empty model rather than an error --
	// verified against a real streaming-only export, which held 0 rows in daily
	// tables and 9089 in intraday ones for the same window.
	for _, asset := range []string{"ga4_sessions", "ga4_page_daily"} {
		content := readTemplate("ga4-search-console-bigquery/assets/web_stage/" + asset + ".sql")
		require.Contains(t, content, "`{{ var.ga4_dataset }}.events_*`", asset)
		require.Contains(t, content, "ga4_events_table_filter(", asset)
	}

	macros := readTemplate("ga4-search-console-bigquery/macros/search.sql")
	require.Contains(t, macros, `REGEXP_CONTAINS(_TABLE_SUFFIX, r'^[0-9]{8}$')`)
	require.Contains(t, macros, `REGEXP_CONTAINS(_TABLE_SUFFIX, r'^intraday_[0-9]{8}$')`)

	// Streaming-only exports leave every session-scoped traffic-source field NULL,
	// which made every session look non-organic and emptied every organic report.
	// The fallback chain and the basis column are what keep those reports usable.
	sessions := readTemplate("ga4-search-console-bigquery/assets/web_stage/ga4_sessions.sql")
	require.Contains(t, sessions, "collected_traffic_source.manual_medium")
	require.Contains(t, sessions, "traffic_source.medium")
	require.Contains(t, sessions, "traffic_source_basis")

	siteImpression := readTemplate("ga4-search-console-bigquery/assets/web_stage/gsc_site_query_daily.sql")
	require.Contains(t, siteImpression, "`{{ var.search_console_dataset }}.searchdata_site_impression`")
	// Position lives in sum_top_position on the property-level table and in
	// sum_position on the URL-level one; swapping them silently breaks position.
	require.Contains(t, siteImpression, "SUM(sum_top_position)")

	urlImpression := readTemplate("ga4-search-console-bigquery/assets/web_stage/gsc_url_query_daily.sql")
	require.Contains(t, urlImpression, "`{{ var.search_console_dataset }}.searchdata_url_impression`")
	require.Contains(t, urlImpression, "SUM(sum_position)")

	// The GA4 and Search Console sides only join when both derive page_path from
	// the shared macro rather than normalizing inline.
	require.Contains(t, urlImpression, "{{ page_path('url') }}")
	require.Contains(t, sessions, "{{ page_path('landing_page_location') }}")

	// Search Console only reports Google, so the join must use the narrower flag.
	landingPages := readTemplate("ga4-search-console-bigquery/assets/web_reports/organic_landing_page_performance.sql")
	require.Contains(t, landingPages, "is_google_organic_session")
	require.NotContains(t, landingPages, "sessions.is_organic_search_session")
}

func TestGA4SearchConsoleBigQueryTemplateKeepsHostsSeparate(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	// A property that spans hosts serves the same path on more than one of them.
	// Every page-grain report must keep the host in its grain, otherwise unrelated
	// pages are summed and whichever host earns the revenue has its value diluted.
	for _, asset := range []string{
		"organic_landing_page_performance",
		"organic_query_value",
		"organic_intent_pipeline",
		"search_page_trend",
		"search_query_opportunities",
		"search_query_cannibalization",
		"search_new_and_lost_queries",
		"search_competitor_visibility",
	} {
		content := readTemplate("ga4-search-console-bigquery/assets/web_reports/" + asset + ".sql")
		require.Contains(t, content, "page_hostname", asset)
	}

	// Both sides of the GA4-to-Search-Console join must match on host as well as
	// path, or the join silently reintroduces the merge.
	landingPages := readTemplate("ga4-search-console-bigquery/assets/web_reports/organic_landing_page_performance.sql")
	require.Contains(t, landingPages, "ON landing.page_hostname = search.page_hostname")
	require.Contains(t, landingPages, "ON content.page_hostname = combined.page_hostname")

	queryValue := readTemplate("ga4-search-console-bigquery/assets/web_reports/organic_query_value.sql")
	require.Contains(t, queryValue, "USING (site_url, page_hostname, page_path)")
	require.Contains(t, queryValue, "ON outcomes.page_hostname = query_page.page_hostname")
}

func TestGA4SearchConsoleBigQueryTemplateEscapesBrandPattern(t *testing.T) {
	t.Parallel()

	macros, err := templates.Templates.ReadFile("ga4-search-console-bigquery/macros/search.sql")
	require.NoError(t, err)

	// Configured patterns are interpolated into string literals, and plenty carry
	// an apostrophe: brands such as Levi's and O'Reilly, competitors, path rules.
	// Without the triple quotes and the ['] rewrite, a bare apostrophe closes the
	// literal and every asset that classifies a query fails to parse. All patterns
	// route through re_literal so the escaping cannot be forgotten in one place.
	require.Contains(t, string(macros), `r'''{{ pattern | replace("'", "[']") }}'''`)
	require.Contains(t, string(macros), "{{ re_literal(brand_pattern) }}")
	require.Contains(t, string(macros), "{{ re_literal(commercial_pattern) }}")
	require.Contains(t, string(macros), "{{ re_literal(pattern | lower) }}")

	urlMacros, err := templates.Templates.ReadFile("ga4-search-console-bigquery/macros/url.sql")
	require.NoError(t, err)
	require.Contains(t, string(urlMacros), "{{ re_literal(support_pattern) }}")
	require.Contains(t, string(urlMacros), "{{ re_literal(content_pattern) }}")
}

func TestGA4SearchConsoleBigQueryTemplateSupportsB2BSaaS(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	pipeline := readTemplate("ga4-search-console-bigquery/pipeline.yml")
	// B2B SaaS revenue is recognized in a CRM weeks after the visit, so the GA4
	// export carries no purchase amount. Without priced key events every value
	// metric in the reports reads zero.
	require.Contains(t, pipeline, "key_event_values:")
	require.Contains(t, pipeline, "demo_event_names:")
	require.Contains(t, pipeline, "signup_event_names:")
	require.Contains(t, pipeline, "competitor_names:")
	require.Contains(t, pipeline, "support_path_pattern:")
	require.Contains(t, pipeline, "content_path_pattern:")
	// Weighting 'purchase' here as well as reading ecommerce revenue would double
	// count it, so the default map must leave it out.
	require.NotContains(t, pipeline, "      purchase:")

	sessions := readTemplate("ga4-search-console-bigquery/assets/web_stage/ga4_sessions.sql")
	require.Contains(t, sessions, "demo_event_count")
	require.Contains(t, sessions, "signup_event_count")
	require.Contains(t, sessions, "key_event_value_usd")
	// The single column the reports rank by, so it works whether the site sells
	// directly or hands off to sales.
	require.Contains(t, sessions, "key_event_value_usd + COALESCE(purchase_revenue_in_usd, 0) AS session_outcome_value_usd")

	// Documentation regularly out-ranks the marketing site and its clicks are
	// existing customers, so every page-level model must carry the role.
	for _, asset := range []string{
		"web_stage/gsc_url_query_daily",
		"web_stage/ga4_page_daily",
	} {
		require.Contains(t, readTemplate("ga4-search-console-bigquery/assets/"+asset+".sql"), "page_role")
	}
	require.Contains(t, sessions, "landing_page_role")

	// Commercial intent has to reach both Search Console models for the intent
	// reports to slice anything.
	for _, asset := range []string{
		"web_stage/gsc_url_query_daily",
		"web_stage/gsc_site_query_daily",
	} {
		content := readTemplate("ga4-search-console-bigquery/assets/" + asset + ".sql")
		require.Contains(t, content, "query_intent_type", asset)
		require.Contains(t, content, "competitor_name", asset)
	}

	landingPages := readTemplate("ga4-search-console-bigquery/assets/web_reports/organic_landing_page_performance.sql")
	require.Contains(t, landingPages, "outcome_value_per_search_click_usd")
	require.Contains(t, landingPages, "demo_events_per_hundred_clicks")

	queryValue := readTemplate("ga4-search-console-bigquery/assets/web_reports/organic_query_value.sql")
	require.Contains(t, queryValue, "modelled_outcome_value_per_click_usd")
	require.Contains(t, queryValue, "modelled_demo_events")
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

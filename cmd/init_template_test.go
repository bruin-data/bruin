//nolint:paralleltest
package cmd

import (
	iofs "io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

func TestInitGoogleWebAnalyticsCopiesStarterTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "google-web-analytics"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "google-web-analytics")
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "README.md"))
	require.FileExists(t, filepath.Join(pipelineRoot, ".gitignore"))
	require.FileExists(t, filepath.Join(pipelineRoot, "macros", "url.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "macros", "search.sql"))
	// Folder under assets/ is exactly the dataset, file name exactly the table.
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "web_analytics_raw", "ga4_events_intraday.asset.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "web_analytics_staging", "ga4_sessions.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "web_analytics_reports", "ga4_gsc_landing_page_performance.sql"))

	pipeline, err := os.ReadFile(filepath.Join(pipelineRoot, "pipeline.yml"))
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: google-web-analytics")
	require.Contains(t, string(pipeline), "google_cloud_platform: gcp-default")

	configContent, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(configContent), "name: gcp-default")
}

func TestGoogleWebAnalyticsTemplateHasFocusedAssetSet(t *testing.T) {
	t.Parallel()

	expectedAssets := []string{
		"web_analytics_raw/ga4_events_intraday.asset.yml",
		"web_analytics_raw/gsc_searchdata_url_impression.asset.yml",
		"web_analytics_raw/gsc_searchdata_site_impression.asset.yml",
		"web_analytics_raw/gsc_export_log.asset.yml",
		"web_analytics_staging/gsc_site_query_daily.sql",
		"web_analytics_staging/gsc_url_query_daily.sql",
		"web_analytics_staging/gsc_position_click_curve.sql",
		"web_analytics_staging/gsc_export_log.sql",
		"web_analytics_staging/ga4_sessions.sql",
		"web_analytics_staging/ga4_page_daily.sql",
		"web_analytics_reports/gsc_brand_split_weekly.sql",
		"web_analytics_reports/gsc_query_opportunities.sql",
		"web_analytics_reports/gsc_query_cannibalization.sql",
		"web_analytics_reports/gsc_page_trend.sql",
		"web_analytics_reports/gsc_new_and_lost_queries.sql",
		"web_analytics_reports/gsc_competitor_visibility.sql",
		"web_analytics_reports/ga4_gsc_landing_page_performance.sql",
		"web_analytics_reports/ga4_gsc_query_value.sql",
		"web_analytics_reports/ga4_gsc_intent_pipeline.sql",
	}

	var actualAssets []string
	err := iofs.WalkDir(templates.Templates, "google-web-analytics/assets", func(path string, entry iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		actualAssets = append(actualAssets, strings.TrimPrefix(path, "google-web-analytics/assets/"))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, actualAssets, 19)
	require.ElementsMatch(t, expectedAssets, actualAssets)

	pipeline, err := templates.Templates.ReadFile("google-web-analytics/pipeline.yml")
	require.NoError(t, err)
	require.Contains(t, string(pipeline), "name: google-web-analytics")
	// The template reads exports that already exist in BigQuery, so the dataset
	// names and the brand pattern must stay overridable without editing SQL.
	require.Contains(t, string(pipeline), "ga4_dataset:")
	require.Contains(t, string(pipeline), "search_console_dataset:")
	require.Contains(t, string(pipeline), "brand_query_pattern:")

	readme, err := templates.Templates.ReadFile("google-web-analytics/README.md")
	require.NoError(t, err)
	require.Contains(t, string(readme), "Google Analytics and Search Console Reporting on BigQuery")
}

func TestGoogleWebAnalyticsTemplateReadsExistingExports(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	// The template is scoped to the streaming export. Selecting through the
	// events_intraday_* wildcard rather than events_* makes _TABLE_SUFFIX the bare
	// date and makes it impossible to pick up a daily table by accident. Verified
	// against a real property whose 981 export tables are all intraday.
	for _, asset := range []string{"ga4_sessions", "ga4_page_daily"} {
		content := readTemplate("google-web-analytics/assets/web_analytics_staging/" + asset + ".sql")
		require.Contains(t, content, "`{{ var.ga4_dataset }}.events_intraday_*`", asset)
		require.Contains(t, content, "ga4_intraday_window(", asset)
		// Out of scope: daily tables and the user-data export.
		require.NotContains(t, content, "`{{ var.ga4_dataset }}.events_*`", asset)
		require.NotContains(t, content, "pseudonymous_users", asset)
		require.NotContains(t, content, "ga4_table_mode", asset)
	}

	// Streaming-only exports leave every session-scoped traffic-source field NULL,
	// which made every session look non-organic and emptied every organic report.
	// The fallback chain and the basis column are what keep those reports usable.
	sessions := readTemplate("google-web-analytics/assets/web_analytics_staging/ga4_sessions.sql")
	require.Contains(t, sessions, "collected_traffic_source.manual_medium")
	require.Contains(t, sessions, "traffic_source.medium")
	require.Contains(t, sessions, "traffic_source_basis")

	siteImpression := readTemplate("google-web-analytics/assets/web_analytics_staging/gsc_site_query_daily.sql")
	require.Contains(t, siteImpression, "`{{ var.search_console_dataset }}.searchdata_site_impression`")
	// Position lives in sum_top_position on the property-level table and in
	// sum_position on the URL-level one; swapping them silently breaks position.
	require.Contains(t, siteImpression, "SUM(sum_top_position)")

	urlImpression := readTemplate("google-web-analytics/assets/web_analytics_staging/gsc_url_query_daily.sql")
	require.Contains(t, urlImpression, "`{{ var.search_console_dataset }}.searchdata_url_impression`")
	require.Contains(t, urlImpression, "SUM(sum_position)")

	// The GA4 and Search Console sides only join when both derive page_path from
	// the shared macro rather than normalizing inline.
	require.Contains(t, urlImpression, "{{ page_path('url') }}")
	require.Contains(t, sessions, "{{ page_path('landing_page_location') }}")

	// Search Console only reports Google, so the join must use the narrower flag.
	landingPages := readTemplate("google-web-analytics/assets/web_analytics_reports/ga4_gsc_landing_page_performance.sql")
	require.Contains(t, landingPages, "is_google_organic_session")
	require.NotContains(t, landingPages, "sessions.is_organic_search_session")
}

func TestGoogleWebAnalyticsTemplateKeepsHostsSeparate(t *testing.T) {
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
		"ga4_gsc_landing_page_performance",
		"ga4_gsc_query_value",
		"ga4_gsc_intent_pipeline",
		"gsc_page_trend",
		"gsc_query_opportunities",
		"gsc_query_cannibalization",
		"gsc_new_and_lost_queries",
		"gsc_competitor_visibility",
	} {
		content := readTemplate("google-web-analytics/assets/web_analytics_reports/" + asset + ".sql")
		require.Contains(t, content, "page_hostname", asset)
	}

	// Both sides of the GA4-to-Search-Console join must match on host as well as
	// path, or the join silently reintroduces the merge.
	landingPages := readTemplate("google-web-analytics/assets/web_analytics_reports/ga4_gsc_landing_page_performance.sql")
	require.Contains(t, landingPages, "ON landing.page_hostname = search.page_hostname")
	require.Contains(t, landingPages, "ON content.page_hostname = combined.page_hostname")

	queryValue := readTemplate("google-web-analytics/assets/web_analytics_reports/ga4_gsc_query_value.sql")
	require.Contains(t, queryValue, "USING (site_url, page_hostname, page_path)")
	require.Contains(t, queryValue, "ON outcomes.page_hostname = query_page.page_hostname")
}

func TestGoogleWebAnalyticsTemplateEscapesBrandPattern(t *testing.T) {
	t.Parallel()

	macros, err := templates.Templates.ReadFile("google-web-analytics/macros/search.sql")
	require.NoError(t, err)

	// Configured patterns are interpolated into string literals, and plenty carry
	// an apostrophe: brands such as Levi's and O'Reilly, competitors, path rules.
	// The triple-quoted raw literal carries those through untouched. The pattern
	// must reach RE2 byte for byte: rewriting an apostrophe to a character class
	// would turn a pattern like [^']+ into [^[']]+ and silently change what it
	// matches. All patterns route through re_literal so this holds everywhere.
	require.Contains(t, string(macros), `r'''{{ pattern }}'''`)
	require.NotContains(t, string(macros), `replace("'"`)
	require.Contains(t, string(macros), "{{ re_literal(brand_pattern) }}")
	require.Contains(t, string(macros), "{{ re_literal(commercial_pattern) }}")
	require.Contains(t, string(macros), "{{ re_literal(pattern | lower) }}")

	urlMacros, err := templates.Templates.ReadFile("google-web-analytics/macros/url.sql")
	require.NoError(t, err)
	require.Contains(t, string(urlMacros), "{{ re_literal(support_pattern) }}")
	require.Contains(t, string(urlMacros), "{{ re_literal(content_pattern) }}")
}

func TestGoogleWebAnalyticsTemplateModelsOutcomeValue(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	pipeline := readTemplate("google-web-analytics/pipeline.yml")
	// Where revenue is recognized outside GA4 the export carries no purchase
	// amount, so without priced key events every value metric reads zero.
	require.Contains(t, pipeline, "key_event_values:")
	require.Contains(t, pipeline, "demo_event_names:")
	require.Contains(t, pipeline, "signup_event_names:")
	require.Contains(t, pipeline, "competitor_names:")
	require.Contains(t, pipeline, "support_path_pattern:")
	require.Contains(t, pipeline, "content_path_pattern:")
	// Weighting 'purchase' here as well as reading ecommerce revenue would double
	// count it, so the default map must leave it out.
	require.NotContains(t, pipeline, "      purchase:")

	sessions := readTemplate("google-web-analytics/assets/web_analytics_staging/ga4_sessions.sql")
	require.Contains(t, sessions, "demo_event_count")
	require.Contains(t, sessions, "signup_event_count")
	require.Contains(t, sessions, "key_event_value_usd")
	// The single column the reports rank by, so it works whether the site sells
	// directly or hands off to sales.
	require.Contains(t, sessions, "key_event_value_usd + COALESCE(purchase_revenue_in_usd, 0) AS session_outcome_value_usd")

	// Documentation regularly out-ranks the marketing site and its clicks are
	// existing customers, so every page-level model must carry the role.
	for _, asset := range []string{
		"web_analytics_staging/gsc_url_query_daily",
		"web_analytics_staging/ga4_page_daily",
	} {
		require.Contains(t, readTemplate("google-web-analytics/assets/"+asset+".sql"), "page_role")
	}
	require.Contains(t, sessions, "landing_page_role")

	// Commercial intent has to reach both Search Console models for the intent
	// reports to slice anything.
	for _, asset := range []string{
		"web_analytics_staging/gsc_url_query_daily",
		"web_analytics_staging/gsc_site_query_daily",
	} {
		content := readTemplate("google-web-analytics/assets/" + asset + ".sql")
		require.Contains(t, content, "query_intent_type", asset)
		require.Contains(t, content, "competitor_name", asset)
	}

	landingPages := readTemplate("google-web-analytics/assets/web_analytics_reports/ga4_gsc_landing_page_performance.sql")
	require.Contains(t, landingPages, "outcome_value_per_search_click_usd")
	require.Contains(t, landingPages, "demo_events_per_hundred_clicks")

	queryValue := readTemplate("google-web-analytics/assets/web_analytics_reports/ga4_gsc_query_value.sql")
	require.Contains(t, queryValue, "modelled_outcome_value_per_click_usd")
	require.Contains(t, queryValue, "modelled_demo_events")
}

func TestGoogleWebAnalyticsTemplateDeclaresSourcesForLineage(t *testing.T) {
	t.Parallel()

	readTemplate := func(path string) string {
		t.Helper()
		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		return string(content)
	}

	// The Google exports are read but never written, so they are declared as no-op
	// bq.source assets purely to give the staging models a visible upstream.
	// Asset names are validated before Jinja renders, so these cannot carry the
	// dataset variable or the wildcard -- hence stable logical names.
	sources := map[string]string{
		"ga4_events_intraday":            "web_analytics_raw.ga4_events_intraday",
		"gsc_searchdata_url_impression":  "web_analytics_raw.gsc_searchdata_url_impression",
		"gsc_searchdata_site_impression": "web_analytics_raw.gsc_searchdata_site_impression",
		"gsc_export_log":                 "web_analytics_raw.gsc_export_log",
	}
	for file, name := range sources {
		content := readTemplate("google-web-analytics/assets/web_analytics_raw/" + file + ".asset.yml")
		require.Contains(t, content, "name: "+name, file)
		require.Contains(t, content, "type: bq.source", file)
		require.NotContains(t, content, "{{ var.", file)
		require.NotContains(t, content, "*\n", file)
	}

	// Every staging model that reads an export must point at its source asset,
	// otherwise the lineage graph starts mid-pipeline.
	for asset, dep := range map[string]string{
		"ga4_sessions":         "web_analytics_raw.ga4_events_intraday",
		"ga4_page_daily":       "web_analytics_raw.ga4_events_intraday",
		"gsc_url_query_daily":  "web_analytics_raw.gsc_searchdata_url_impression",
		"gsc_site_query_daily": "web_analytics_raw.gsc_searchdata_site_impression",
		"gsc_export_log":       "web_analytics_raw.gsc_export_log",
	} {
		content := readTemplate("google-web-analytics/assets/web_analytics_staging/" + asset + ".sql")
		require.Contains(t, content, "  - "+dep, asset)
	}
}

func TestGoogleWebAnalyticsTemplateNamesMatchTheirLocation(t *testing.T) {
	t.Parallel()

	// One naming rule holds across the template: the folder under assets/ is
	// exactly the dataset and the file name is exactly the table. That is what
	// makes an asset findable from a table name in a warehouse, so it is worth
	// pinning rather than trusting to review.
	err := iofs.WalkDir(templates.Templates, "google-web-analytics/assets", func(path string, entry iofs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		rel := strings.TrimPrefix(path, "google-web-analytics/assets/")
		parts := strings.SplitN(rel, "/", 2)
		require.Len(t, parts, 2, rel)
		dataset := parts[0]
		table := strings.TrimSuffix(strings.TrimSuffix(parts[1], ".sql"), ".asset.yml")

		content, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		require.Contains(t, string(content), "name: "+dataset+"."+table, rel)

		// Every table is prefixed with the platform it comes from, so another
		// platform can be added later without renaming anything.
		require.True(t,
			strings.HasPrefix(table, "ga4_") || strings.HasPrefix(table, "gsc_"),
			"%s is missing a platform prefix", rel)
		return nil
	})
	require.NoError(t, err)

	for _, dataset := range []string{"web_analytics_raw", "web_analytics_staging", "web_analytics_reports"} {
		entries, err := iofs.ReadDir(templates.Templates, "google-web-analytics/assets/"+dataset)
		require.NoError(t, err, dataset)
		require.NotEmpty(t, entries, dataset)
	}
}

func TestGoogleWebAnalyticsTemplateUsesGoogleSQLTypes(t *testing.T) {
	t.Parallel()

	content, err := templates.Templates.ReadFile("google-web-analytics/assets/web_analytics_raw/ga4_events_intraday.asset.yml")
	require.NoError(t, err)

	// RECORD is the legacy and REST representation of a nested field. GoogleSQL,
	// and INFORMATION_SCHEMA.COLUMNS with it, reports STRUCT and ARRAY<STRUCT>.
	// Verified against a real GA4 export.
	require.NotContains(t, string(content), "type: RECORD")
	require.Contains(t, string(content), "type: STRUCT")
	require.Contains(t, string(content), "type: ARRAY<STRUCT>")
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

func TestGoogleWebAnalyticsTemplateShipsDashboards(t *testing.T) {
	t.Parallel()

	// The dashboards are embedded through the top-level `*` directive, so a
	// change to the embed rules could drop them from `bruin init` silently.
	entries, err := templates.Templates.ReadDir("google-web-analytics/dashboards")
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	require.ElementsMatch(t, []string{
		"01-overview.yml",
		"02-ga4-insights.yml",
		"03-gsc-insights.yml",
	}, names)

	for _, name := range names {
		content, err := templates.Templates.ReadFile("google-web-analytics/dashboards/" + name)
		require.NoError(t, err)
		body := string(content)

		// Dashboards must run on the same connection as the pipeline and carry
		// no hard-coded project from whichever warehouse they were tested on.
		require.Contains(t, body, "connection: gcp-default", name)
		require.NotContains(t, body, "bruin-landing-page", name)

		// Every table they read has to be one this pipeline builds, never the
		// raw Google export, so the dashboards stay behind the staging contract.
		refs := regexp.MustCompile("FROM `([^`]+)`").FindAllStringSubmatch(body, -1)
		require.NotEmpty(t, refs, name)
		for _, ref := range refs {
			require.Regexp(t, `^web_analytics_(staging|reports)\.`, ref[1], name)
		}
	}
}

func TestInitMurderMysteryCopiesTemplate(t *testing.T) {
	targetRoot := t.TempDir()
	t.Chdir(targetRoot)

	gitInit := exec.CommandContext(t.Context(), "git", "init")
	gitInit.Dir = targetRoot
	out, err := gitInit.CombinedOutput()
	require.NoError(t, err, string(out))

	err = Init().Run(t.Context(), []string{"init", "murder-mystery", "yorkville-case"})
	require.NoError(t, err)

	pipelineRoot := filepath.Join(targetRoot, "yorkville-case")
	require.FileExists(t, filepath.Join(targetRoot, ".bruin.yml"))
	require.FileExists(t, filepath.Join(pipelineRoot, "pipeline.yml"))

	for _, doc := range []string{"README.md", "CASE_FILE.md", "DATA_DICTIONARY.md", "AGENTS.md", "PLAY_WITH_AN_AGENT.md"} {
		require.FileExists(t, filepath.Join(pipelineRoot, doc), doc)
	}

	// .gitignore needs its own embed directive; the top-level `*` skips dotfiles.
	gitignore, err := os.ReadFile(filepath.Join(pipelineRoot, ".gitignore"))
	require.NoError(t, err)
	require.Contains(t, string(gitignore), "yorkville.db")

	// Macros load from the pipeline root by path; a missing one breaks every asset.
	for _, macro := range []string{"rng.sql", "pools.sql", "calendar.sql", "places.sql", "format.sql"} {
		require.FileExists(t, filepath.Join(pipelineRoot, "macros", macro), macro)
	}

	// Without the teardown, the played database keeps the answer in _gen.
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "seed", "99_cleanup.sql"))
	require.FileExists(t, filepath.Join(pipelineRoot, "assets", "notebook", "rally_window_plate_reads.sql"))

	seeds, err := os.ReadDir(filepath.Join(pipelineRoot, "assets", "seed"))
	require.NoError(t, err)
	require.Greater(t, len(seeds), 40, "the seed pipeline should be the whole town")

	// Nine other templates ship duckdb-default, and init merges into one config per
	// repository, so sharing the name would send the town into their database.
	config, err := os.ReadFile(filepath.Join(targetRoot, ".bruin.yml"))
	require.NoError(t, err)
	require.Contains(t, string(config), "name: duckdb-yorkville")
	require.Contains(t, string(config), "path: yorkville.db")
	require.NotContains(t, string(config), "duckdb-default")
}

func TestMurderMysteryTemplateKeepsTheCaseOutOfItsOwnSource(t *testing.T) {
	t.Parallel()

	// The case is only protected by the generator never writing down who it picked.
	// Culprit names cannot be checked for directly — they are unknowable without
	// running the generator, and each is one entry in a pool of 64 or 168. What must
	// never appear is a citizen id or a full name.
	forbidden := regexp.MustCompile(`(?i)\b(culprit|guilty|hitman|spoiler|murderer|answer[ _-]?key|solution)\b`)
	citizenID := regexp.MustCompile(`\bC\d{5}\b`)

	err := iofs.WalkDir(templates.Templates, "murder-mystery", func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		require.NotRegexp(t, forbidden, path, "file name gives the game away: %s", path)

		body, err := templates.Templates.ReadFile(path)
		require.NoError(t, err)
		text := string(body)

		// The vocabulary rule is about the generator naming things it should be
		// ranking. The player-facing documents talk about the case in plain
		// English on purpose, so they are exempt from it — but not from the id
		// check below, which applies everywhere.
		generated := strings.Contains(path, "/assets/") || strings.Contains(path, "/macros/")
		if match := forbidden.FindString(text); generated && match != "" {
			t.Errorf("%s contains %q, which names the case instead of generating it", path, match)
		}
		if match := citizenID.FindString(text); match != "" {
			t.Errorf("%s contains the citizen id %q; roles must be assigned by ranking, never by id", path, match)
		}

		// Role assignment is only unreadable while it stays a ranking. A generator
		// that filtered for the incriminating attributes together would hand a
		// reader the whole deduction path.
		if strings.HasSuffix(path, "02_actor_assignments.sql") {
			require.Contains(t, text, "ORDER BY", path)
			require.Contains(t, text, "LIMIT 1", path)
		}

		return nil
	})
	require.NoError(t, err)
}

func TestMurderMysteryTemplateDropsItsScaffolding(t *testing.T) {
	t.Parallel()

	// If the teardown stops being its own tagged asset, either the played database
	// keeps the answer or the QA pipeline loses its only way to read it.
	body, err := templates.Templates.ReadFile("murder-mystery/assets/seed/99_cleanup.sql")
	require.NoError(t, err)
	text := string(body)

	require.Contains(t, text, "DROP SCHEMA IF EXISTS _gen CASCADE")
	require.Contains(t, text, "tags:")
	require.Contains(t, text, "- scaffolding")

	// A pipeline-level materialization default would be inherited here and wrap
	// the DROP in a CREATE TABLE ... AS, which fails the run.
	pipeline, err := templates.Templates.ReadFile("murder-mystery/pipeline.yml")
	require.NoError(t, err)
	require.NotContains(t, string(pipeline), "materialization:")
}

package cmd

import (
	"fmt"
	"strings"
)

// --- Pipeline config generation ---

func generatePipelineYML(c *EcommerceChoices) string {
	var connKey, connName string
	switch c.Warehouse {
	case warehouseClickHouse:
		connKey = warehouseClickHouse
		connName = "clickhouse-default"
	case warehouseBigQuery:
		connKey = "google_cloud_platform"
		connName = "bigquery-default"
	case warehouseSnowflake:
		connKey = warehouseSnowflake
		connName = "snowflake-default"
	}

	return fmt.Sprintf(`name: ecommerce
schedule: daily
start_date: "2024-01-01"

default_connections:
  %s: %s
`, connKey, connName)
}

// --- Raw Ingestr Assets ---

const shopifyOrdersAsset = `name: raw.shopify_orders
type: ingestr
parameters:
  source_connection: shopify
  source_table: orders
  loader_file_format: jsonl
  incremental_strategy: merge
  incremental_key: updated_at
  primary_key: id
`

const shopifyCustomersAsset = `name: raw.shopify_customers
type: ingestr
parameters:
  source_connection: shopify
  source_table: customers
  loader_file_format: jsonl
  incremental_strategy: merge
  incremental_key: updated_at
  primary_key: id
`

const shopifyProductsAsset = `name: raw.shopify_products
type: ingestr
parameters:
  source_connection: shopify
  source_table: products
  loader_file_format: jsonl
  incremental_strategy: merge
  incremental_key: updated_at
  primary_key: id
`

const shopifyInventoryAsset = `name: raw.shopify_inventory
type: ingestr
parameters:
  source_connection: shopify
  source_table: inventory_levels
  loader_file_format: jsonl
  incremental_strategy: replace
`

// Stripe assets

const stripeChargesAsset = `name: raw.stripe_charges
type: ingestr
parameters:
  source_connection: stripe
  source_table: charges
  incremental_strategy: merge
  incremental_key: created
  primary_key: id
`

const stripeRefundsAsset = `name: raw.stripe_refunds
type: ingestr
parameters:
  source_connection: stripe
  source_table: refunds
  incremental_strategy: merge
  incremental_key: created
  primary_key: id
`

const stripeCustomersAsset = `name: raw.stripe_customers
type: ingestr
parameters:
  source_connection: stripe
  source_table: customers
  incremental_strategy: merge
  incremental_key: created
  primary_key: id
`

const stripePayoutsAsset = `name: raw.stripe_payouts
type: ingestr
parameters:
  source_connection: stripe
  source_table: payouts
  incremental_strategy: merge
  incremental_key: created
  primary_key: id
`

// Klaviyo assets

const klaviyoCampaignsAsset = `name: raw.klaviyo_campaigns
type: ingestr
parameters:
  source_connection: klaviyo
  source_table: campaigns
  incremental_strategy: replace
`

const klaviyoFlowsAsset = `name: raw.klaviyo_flows
type: ingestr
parameters:
  source_connection: klaviyo
  source_table: flows
  incremental_strategy: replace
`

const klaviyoMetricsAsset = `name: raw.klaviyo_metrics
type: ingestr
parameters:
  source_connection: klaviyo
  source_table: metrics
  incremental_strategy: replace
`

// HubSpot assets

const hubspotContactsAsset = `name: raw.hubspot_contacts
type: ingestr
parameters:
  source_connection: hubspot
  source_table: contacts
  incremental_strategy: merge
  incremental_key: updatedAt
  primary_key: id
`

const hubspotDealsAsset = `name: raw.hubspot_deals
type: ingestr
parameters:
  source_connection: hubspot
  source_table: deals
  incremental_strategy: merge
  incremental_key: updatedAt
  primary_key: id
`

const hubspotCampaignsAsset = `name: raw.hubspot_campaigns
type: ingestr
parameters:
  source_connection: hubspot
  source_table: campaigns
  incremental_strategy: replace
`

// Facebook Ads assets

const facebookCampaignsAsset = `name: raw.facebook_campaigns
type: ingestr
parameters:
  source_connection: facebook_ads
  source_table: campaigns
  incremental_strategy: replace
`

const facebookAdInsightsAsset = `name: raw.facebook_ad_insights
type: ingestr
parameters:
  source_connection: facebook_ads
  source_table: insights
  incremental_strategy: merge
  incremental_key: date_start
  primary_key: "date_start,campaign_id"
`

// Google Ads assets

const googleCampaignsAsset = `name: raw.google_campaigns
type: ingestr
parameters:
  source_connection: google_ads
  source_table: campaigns
  incremental_strategy: replace
`

const googleAdInsightsAsset = `name: raw.google_ad_insights
type: ingestr
parameters:
  source_connection: google_ads
  source_table: campaign_performance
  incremental_strategy: merge
  incremental_key: date
  primary_key: "date,campaign_id"
`

// TikTok Ads assets

const tiktokCampaignsAsset = `name: raw.tiktok_campaigns
type: ingestr
parameters:
  source_connection: tiktok_ads
  source_table: campaigns
  incremental_strategy: replace
`

const tiktokAdInsightsAsset = `name: raw.tiktok_ad_insights
type: ingestr
parameters:
  source_connection: tiktok_ads
  source_table: ads
  incremental_strategy: merge
  incremental_key: stat_datetime
  primary_key: "stat_datetime,campaign_id"
`

// GA4 assets

const ga4EventsAsset = `name: raw.ga4_events
type: ingestr
parameters:
  source_connection: google_analytics
  source_table: events
  incremental_strategy: merge
  incremental_key: date
  primary_key: "date,event_name"
`

const ga4SessionsAsset = `name: raw.ga4_sessions
type: ingestr
parameters:
  source_connection: google_analytics
  source_table: sessions
  incremental_strategy: merge
  incremental_key: date
  primary_key: date
`

// Mixpanel assets

const mixpanelEventsAsset = `name: raw.mixpanel_events
type: ingestr
parameters:
  source_connection: mixpanel
  source_table: events
  incremental_strategy: merge
  incremental_key: time
  primary_key: "distinct_id,time"
`

const mixpanelFunnelsAsset = `name: raw.mixpanel_funnels
type: ingestr
parameters:
  source_connection: mixpanel
  source_table: funnels
  incremental_strategy: replace
`

// --- Staging SQL Generation ---

func generateStgOrders(c *EcommerceChoices) string {
	header := `/* @bruin
name: staging.stg_orders
type: sql
materialization:
  type: table
depends:
  - raw.shopify_orders`

	if c.Payments == paymentsStripe {
		header += "\n  - raw.stripe_charges"
	}

	header += `
columns:
  - name: order_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: order_date
    type: timestamp
    checks:
      - name: not_null
custom_checks:
  - name: has_rows
    query: "SELECT count(*) > 0 FROM staging.stg_orders"
    value: 1
@bruin */

`

	var castFn, dateFn, joinClause, extraCols, extraWhere string

	switch c.Warehouse {
	case warehouseClickHouse:
		castFn = "CAST"
		dateFn = "toDate"
	case warehouseBigQuery:
		castFn = "SAFE_CAST"
		dateFn = "DATE"
		extraWhere = "\nWHERE o.test IS NOT TRUE AND o.financial_status IS NOT NULL\nQUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.updated_at DESC) = 1"
	case warehouseSnowflake:
		castFn = "CAST"
		dateFn = "" // uses ::DATE
	}

	if c.Payments == paymentsStripe {
		extraCols = `,
    c.amount / 100.0 AS stripe_charge_amount,
    c.status AS stripe_status,
    c.paid AS stripe_paid`

		if c.Warehouse == warehouseSnowflake {
			joinClause = `
LEFT JOIN raw.stripe_charges c
    ON o.email = c.receipt_email
    AND o.created_at::DATE = c.created::DATE`
		} else {
			joinClause = fmt.Sprintf(`
LEFT JOIN raw.stripe_charges c
    ON o.email = c.receipt_email
    AND %s(o.created_at) = %s(c.created)`, dateFn, dateFn)
		}
	}

	body := fmt.Sprintf(`SELECT
    o.id AS order_id,
    o.order_number,
    o.email AS customer_email,
    o.created_at AS order_date,
    o.financial_status AS payment_status,
    o.fulfillment_status,
    %s(o.total_price AS DECIMAL(12,2)) AS order_total,
    %s(o.subtotal_price AS DECIMAL(12,2)) AS subtotal,
    %s(o.total_tax AS DECIMAL(12,2)) AS tax_amount,
    %s(o.total_discounts AS DECIMAL(12,2)) AS discount_amount,
    o.currency,
    o.cancel_reason,
    o.cancelled_at%s
FROM raw.shopify_orders o%s%s
`, castFn, castFn, castFn, castFn, extraCols, joinClause, extraWhere)

	return header + body
}

func generateStgCustomers(c *EcommerceChoices) string {
	header := `/* @bruin
name: staging.stg_customers
type: sql
materialization:
  type: table
depends:
  - raw.shopify_customers`

	if c.Payments == paymentsStripe {
		header += "\n  - raw.stripe_customers"
	}

	header += `
columns:
  - name: customer_email
    type: varchar
    checks:
      - name: not_null
      - name: unique
@bruin */

`

	if c.Payments == paymentsStripe {
		return header + `SELECT
    COALESCE(sc.email, st.email) AS customer_email,
    sc.id AS shopify_customer_id,
    st.id AS stripe_customer_id,
    sc.first_name,
    sc.last_name,
    sc.created_at AS shopify_created_at,
    st.created AS stripe_created_at,
    LEAST(sc.created_at, st.created) AS first_seen_at,
    sc.orders_count,
    CAST(sc.total_spent AS DECIMAL(12,2)) AS shopify_total_spent,
    sc.tags AS customer_tags,
    sc.state AS customer_state
FROM raw.shopify_customers sc
FULL OUTER JOIN raw.stripe_customers st
    ON lower(sc.email) = lower(st.email)
WHERE COALESCE(sc.email, st.email) IS NOT NULL
`
	}

	return header + `SELECT
    email AS customer_email,
    id AS shopify_customer_id,
    first_name,
    last_name,
    created_at AS shopify_created_at,
    created_at AS first_seen_at,
    orders_count,
    CAST(total_spent AS DECIMAL(12,2)) AS shopify_total_spent,
    tags AS customer_tags,
    state AS customer_state
FROM raw.shopify_customers
WHERE email IS NOT NULL
`
}

const stgProductsSQL = `/* @bruin
name: staging.stg_products
type: sql
materialization:
  type: table
depends:
  - raw.shopify_products
columns:
  - name: product_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    id AS product_id,
    title AS product_name,
    product_type AS category,
    vendor,
    status AS product_status,
    CAST(price AS DECIMAL(12,2)) AS price,
    tags,
    created_at,
    updated_at
FROM raw.shopify_products
WHERE status = 'active'
`

func generateStgMarketingSpend(c *EcommerceChoices) string {
	depends := make([]string, 0)
	var parts []string

	// Add ad platform dependencies and queries
	for _, ad := range c.Ads {
		switch ad {
		case adsFacebook:
			depends = append(depends, "raw.facebook_ad_insights")
			var fbDate string
			switch c.Warehouse {
			case warehouseClickHouse:
				fbDate = "toDate(date_start)"
			case warehouseSnowflake:
				fbDate = "date_start::DATE"
			default:
				fbDate = "DATE(date_start)"
			}
			parts = append(parts, fmt.Sprintf(`-- Facebook Ads spend
SELECT
    %s AS spend_date,
    'paid_ads' AS channel,
    campaign_name,
    CAST(spend AS DECIMAL(12,2)) AS spend,
    CAST(impressions AS INTEGER) AS impressions,
    CAST(clicks AS INTEGER) AS clicks,
    CAST(conversions AS INTEGER) AS conversions
FROM raw.facebook_ad_insights`, fbDate))
		case adsGoogle:
			depends = append(depends, "raw.google_ad_insights")
			var gaDate string
			switch c.Warehouse {
			case warehouseClickHouse:
				gaDate = "toDate(date)"
			case warehouseSnowflake:
				gaDate = "date::DATE"
			default:
				gaDate = "DATE(date)"
			}
			parts = append(parts, fmt.Sprintf(`-- Google Ads spend
SELECT
    %s AS spend_date,
    'paid_ads' AS channel,
    campaign_name,
    CAST(spend AS DECIMAL(12,2)) AS spend,
    CAST(impressions AS INTEGER) AS impressions,
    CAST(clicks AS INTEGER) AS clicks,
    CAST(conversions AS INTEGER) AS conversions
FROM raw.google_ad_insights`, gaDate))
		case adsTikTok:
			depends = append(depends, "raw.tiktok_ad_insights")
			var ttDate string
			switch c.Warehouse {
			case warehouseClickHouse:
				ttDate = "toDate(stat_datetime)"
			case warehouseSnowflake:
				ttDate = "stat_datetime::DATE"
			default:
				ttDate = "DATE(stat_datetime)"
			}
			parts = append(parts, fmt.Sprintf(`-- TikTok Ads spend
SELECT
    %s AS spend_date,
    'paid_ads' AS channel,
    campaign_name,
    CAST(spend AS DECIMAL(12,2)) AS spend,
    CAST(impressions AS INTEGER) AS impressions,
    CAST(clicks AS INTEGER) AS clicks,
    CAST(conversions AS INTEGER) AS conversions
FROM raw.tiktok_ad_insights`, ttDate))
		}
	}

	// Add marketing platform
	switch c.Marketing {
	case marketingKlaviyo:
		depends = append(depends, "raw.klaviyo_campaigns", "raw.klaviyo_metrics")

		var dateCast string
		switch c.Warehouse {
		case warehouseClickHouse:
			dateCast = "toDate(send_time)"
		case warehouseBigQuery:
			dateCast = "DATE(send_time)"
		default:
			dateCast = "send_time::date"
		}

		parts = append(parts, fmt.Sprintf(`-- Klaviyo email campaigns
SELECT
    %s AS spend_date,
    'email' AS channel,
    name AS campaign_name,
    0.00 AS spend,
    num_recipients AS impressions,
    CAST(click_count AS INTEGER) AS clicks,
    CAST(conversion_count AS INTEGER) AS conversions
FROM raw.klaviyo_campaigns kc
LEFT JOIN raw.klaviyo_metrics km
    ON kc.id = km.campaign_id
WHERE send_time IS NOT NULL`, dateCast))
	case marketingHubSpot:
		depends = append(depends, "raw.hubspot_campaigns")

		var dateCast string
		switch c.Warehouse {
		case warehouseClickHouse:
			dateCast = "toDate(updated_at)"
		case warehouseBigQuery:
			dateCast = "DATE(updated_at)"
		default:
			dateCast = "updated_at::date"
		}

		parts = append(parts, fmt.Sprintf(`-- HubSpot email campaigns
SELECT
    %s AS spend_date,
    'email' AS channel,
    name AS campaign_name,
    0.00 AS spend,
    CAST(num_included AS INTEGER) AS impressions,
    CAST(num_clicks AS INTEGER) AS clicks,
    0 AS conversions
FROM raw.hubspot_campaigns
WHERE name IS NOT NULL`, dateCast))
	}

	// Build header
	depLines := make([]string, 0, len(depends))
	for _, d := range depends {
		depLines = append(depLines, "  - "+d)
	}

	header := fmt.Sprintf(`/* @bruin
name: staging.stg_marketing_spend
type: sql
materialization:
  type: table
depends:
%s
columns:
  - name: spend_date
    type: date
    checks:
      - name: not_null
@bruin */

`, strings.Join(depLines, "\n"))

	return header + strings.Join(parts, "\n\nUNION ALL\n\n") + "\n"
}

func generateStgWebSessions(c *EcommerceChoices) string {
	var depends, sourceQuery, dateCast string

	var timeDateCast string
	switch c.Warehouse {
	case warehouseClickHouse:
		dateCast = "toDate(session_raw_date)"
		timeDateCast = "toDate(e.time)"
	case warehouseBigQuery:
		dateCast = "DATE(session_raw_date)"
		timeDateCast = "DATE(e.time)"
	case warehouseSnowflake:
		dateCast = "session_raw_date::DATE"
		timeDateCast = "e.time::DATE"
	}

	switch c.Analytics {
	case analyticsGA4:
		depends = `  - raw.ga4_sessions
  - raw.ga4_events`

		sourceQuery = `SELECT
    s.date AS session_raw_date,
    s.sessions AS total_sessions,
    s.new_users,
    s.engaged_sessions,
    e.event_count AS purchase_events,
    CASE
        WHEN s.source = 'facebook' THEN 'paid_ads'
        WHEN s.medium = 'email' THEN 'email'
        WHEN s.medium = 'organic' THEN 'organic_search'
        WHEN s.medium = 'cpc' THEN 'paid_search'
        WHEN s.source = '(direct)' THEN 'direct'
        ELSE 'other'
    END AS channel
FROM raw.ga4_sessions s
LEFT JOIN raw.ga4_events e
    ON s.date = e.date
    AND e.event_name = 'purchase'`

	case analyticsMixpanel:
		depends = `  - raw.mixpanel_events`

		sourceQuery = fmt.Sprintf(`SELECT
    s.session_raw_date,
    s.total_sessions,
    s.new_users,
    s.engaged_sessions,
    COALESCE(p.purchase_events, 0) AS purchase_events,
    s.channel
FROM (
    SELECT
        %s AS session_raw_date,
        COUNT(*) AS total_sessions,
        COUNT(CASE WHEN e.is_new_user = true THEN 1 END) AS new_users,
        COUNT(CASE WHEN e.session_duration > 10 THEN 1 END) AS engaged_sessions,
        CASE
            WHEN e.utm_source = 'facebook' THEN 'paid_ads'
            WHEN e.utm_medium = 'email' THEN 'email'
            WHEN e.utm_medium = 'organic' THEN 'organic_search'
            WHEN e.utm_medium = 'cpc' THEN 'paid_search'
            ELSE 'other'
        END AS channel
    FROM raw.mixpanel_events e
    WHERE e.event_name = 'session_start'
    GROUP BY session_raw_date, channel
) s
LEFT JOIN (
    SELECT
        %s AS purchase_date,
        COUNT(*) AS purchase_events
    FROM raw.mixpanel_events e
    WHERE e.event_name = 'purchase'
    GROUP BY purchase_date
) p
    ON s.session_raw_date = p.purchase_date`, timeDateCast, timeDateCast)
	}

	return fmt.Sprintf(`/* @bruin
name: staging.stg_web_sessions
type: sql
materialization:
  type: table
depends:
%s
columns:
  - name: session_date
    type: date
    checks:
      - name: not_null
@bruin */

WITH source AS (
    %s
)
SELECT
    %s AS session_date,
    total_sessions,
    new_users,
    engaged_sessions,
    purchase_events,
    channel
FROM source
`, depends, strings.ReplaceAll(sourceQuery, "\n", "\n    "), dateCast)
}

// --- Report SQL Generation ---

func generateRptDailyRevenue(c *EcommerceChoices) string {
	header := `/* @bruin
name: reports.rpt_daily_revenue
type: sql
materialization:
  type: table
depends:
  - staging.stg_orders
columns:
  - name: order_date
    type: date
    checks:
      - name: not_null
      - name: unique
custom_checks:
  - name: has_rows
    query: "SELECT count(*) > 0 FROM reports.rpt_daily_revenue"
    value: 1
@bruin */

`

	switch c.Warehouse {
	case warehouseClickHouse:
		return header + `SELECT
    toDate(order_date) AS order_date,
    count(*) AS total_orders,
    countIf(payment_status = 'paid') AS paid_orders,
    countIf(cancel_reason IS NOT NULL) AS cancelled_orders,
    sum(order_total) AS gross_revenue,
    sum(CASE WHEN payment_status = 'paid' THEN order_total ELSE 0 END) AS net_revenue,
    sum(discount_amount) AS total_discounts,
    sum(tax_amount) AS total_tax,
    round(net_revenue / nullIf(paid_orders, 0), 2) AS avg_order_value,
    round(cancelled_orders / nullIf(total_orders, 0) * 100, 2) AS cancellation_rate
FROM staging.stg_orders
GROUP BY toDate(order_date)
ORDER BY order_date
`
	case warehouseBigQuery:
		return header + `SELECT
    DATE(order_date) AS order_date,
    count(*) AS total_orders,
    COUNTIF(payment_status = 'paid') AS paid_orders,
    COUNTIF(cancel_reason IS NOT NULL) AS cancelled_orders,
    sum(order_total) AS gross_revenue,
    sum(CASE WHEN payment_status = 'paid' THEN order_total ELSE 0 END) AS net_revenue,
    sum(discount_amount) AS total_discounts,
    sum(tax_amount) AS total_tax,
    round(sum(CASE WHEN payment_status = 'paid' THEN order_total ELSE 0 END) / NULLIF(COUNTIF(payment_status = 'paid'), 0), 2) AS avg_order_value,
    round(COUNTIF(cancel_reason IS NOT NULL) / NULLIF(count(*), 0) * 100, 2) AS cancellation_rate
FROM staging.stg_orders o
WHERE o.financial_status IN ('paid', 'partially_refunded')
GROUP BY DATE(order_date)
ORDER BY order_date
`
	default: // snowflake
		return header + `SELECT
    order_date::DATE AS order_date,
    count(*) AS total_orders,
    COUNT(CASE WHEN payment_status = 'paid' THEN 1 END) AS paid_orders,
    COUNT(CASE WHEN cancel_reason IS NOT NULL THEN 1 END) AS cancelled_orders,
    sum(order_total) AS gross_revenue,
    sum(CASE WHEN payment_status = 'paid' THEN order_total ELSE 0 END) AS net_revenue,
    sum(discount_amount) AS total_discounts,
    sum(tax_amount) AS total_tax,
    round(sum(CASE WHEN payment_status = 'paid' THEN order_total ELSE 0 END) / NULLIF(COUNT(CASE WHEN payment_status = 'paid' THEN 1 END), 0), 2) AS avg_order_value,
    round(COUNT(CASE WHEN cancel_reason IS NOT NULL THEN 1 END) / NULLIF(count(*), 0) * 100, 2) AS cancellation_rate
FROM staging.stg_orders
GROUP BY order_date::DATE
ORDER BY order_date
`
	}
}

func generateRptCustomerCohorts(c *EcommerceChoices) string {
	header := `/* @bruin
name: reports.rpt_customer_cohorts
type: sql
materialization:
  type: table
depends:
  - staging.stg_orders
  - staging.stg_customers
columns:
  - name: cohort_month
    type: date
    checks:
      - name: not_null
@bruin */

`

	switch c.Warehouse {
	case warehouseClickHouse:
		return header + `WITH customer_orders AS (
    SELECT
        o.customer_email,
        toStartOfMonth(c.first_seen_at) AS cohort_month,
        toStartOfMonth(o.order_date) AS order_month,
        o.order_total
    FROM staging.stg_orders o
    INNER JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
),
cohort_sizes AS (
    SELECT
        cohort_month,
        count(DISTINCT customer_email) AS cohort_size
    FROM customer_orders
    GROUP BY cohort_month
)
SELECT
    co.cohort_month,
    cs.cohort_size,
    dateDiff('month', co.cohort_month, co.order_month) AS months_since_first,
    count(DISTINCT co.customer_email) AS active_customers,
    round(active_customers / nullIf(cs.cohort_size, 0) * 100, 2) AS retention_rate,
    sum(co.order_total) AS cohort_revenue,
    round(cohort_revenue / nullIf(cs.cohort_size, 0), 2) AS revenue_per_customer
FROM customer_orders co
INNER JOIN cohort_sizes cs
    ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_size, months_since_first
ORDER BY co.cohort_month, months_since_first
`
	case warehouseBigQuery:
		return header + `WITH customer_orders AS (
    SELECT
        o.customer_email,
        DATE_TRUNC(c.first_seen_at, MONTH) AS cohort_month,
        DATE_TRUNC(o.order_date, MONTH) AS order_month,
        o.order_total
    FROM staging.stg_orders o
    INNER JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
),
cohort_sizes AS (
    SELECT
        cohort_month,
        count(DISTINCT customer_email) AS cohort_size
    FROM customer_orders
    GROUP BY cohort_month
)
SELECT
    co.cohort_month,
    cs.cohort_size,
    DATE_DIFF(co.order_month, co.cohort_month, MONTH) AS months_since_first,
    count(DISTINCT co.customer_email) AS active_customers,
    round(count(DISTINCT co.customer_email) / NULLIF(cs.cohort_size, 0) * 100, 2) AS retention_rate,
    sum(co.order_total) AS cohort_revenue,
    round(sum(co.order_total) / NULLIF(cs.cohort_size, 0), 2) AS revenue_per_customer
FROM customer_orders co
INNER JOIN cohort_sizes cs
    ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_size, months_since_first
ORDER BY co.cohort_month, months_since_first
`
	default: // snowflake
		return header + `WITH customer_orders AS (
    SELECT
        o.customer_email,
        DATE_TRUNC('month', c.first_seen_at) AS cohort_month,
        DATE_TRUNC('month', o.order_date) AS order_month,
        o.order_total
    FROM staging.stg_orders o
    INNER JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
),
cohort_sizes AS (
    SELECT
        cohort_month,
        count(DISTINCT customer_email) AS cohort_size
    FROM customer_orders
    GROUP BY cohort_month
)
SELECT
    co.cohort_month,
    cs.cohort_size,
    DATEDIFF('month', co.cohort_month, co.order_month) AS months_since_first,
    count(DISTINCT co.customer_email) AS active_customers,
    round(count(DISTINCT co.customer_email) / NULLIF(cs.cohort_size, 0) * 100, 2) AS retention_rate,
    sum(co.order_total) AS cohort_revenue,
    round(sum(co.order_total) / NULLIF(cs.cohort_size, 0), 2) AS revenue_per_customer
FROM customer_orders co
INNER JOIN cohort_sizes cs
    ON co.cohort_month = cs.cohort_month
GROUP BY co.cohort_month, cs.cohort_size, months_since_first
ORDER BY co.cohort_month, months_since_first
`
	}
}

const rptProductPerformanceSQL = `/* @bruin
name: reports.rpt_product_performance
type: sql
materialization:
  type: table
depends:
  - staging.stg_products
columns:
  - name: product_id
    type: varchar
    checks:
      - name: not_null
      - name: unique
@bruin */

SELECT
    product_id,
    product_name,
    category,
    vendor,
    price,
    product_status,
    created_at,
    updated_at
FROM staging.stg_products
ORDER BY product_name
`

func generateRptMarketingROI(c *EcommerceChoices) string {
	header := `/* @bruin
name: reports.rpt_marketing_roi
type: sql
materialization:
  type: table
depends:
  - staging.stg_marketing_spend
  - staging.stg_web_sessions
  - staging.stg_orders
columns:
  - name: channel
    type: varchar
    checks:
      - name: not_null
@bruin */

`

	var dateFn, nullIfFn string
	switch c.Warehouse {
	case warehouseClickHouse:
		dateFn = "toDate(o.order_date)"
		nullIfFn = "nullIf"
	case warehouseBigQuery:
		dateFn = "DATE(o.order_date)"
		nullIfFn = "NULLIF"
	case warehouseSnowflake:
		dateFn = "order_date::DATE"
		nullIfFn = "NULLIF"
	}

	return header + fmt.Sprintf(`WITH channel_spend AS (
    SELECT
        spend_date,
        channel,
        sum(spend) AS total_spend,
        sum(impressions) AS total_impressions,
        sum(clicks) AS total_clicks,
        sum(conversions) AS total_conversions
    FROM staging.stg_marketing_spend
    GROUP BY spend_date, channel
),
channel_sessions AS (
    SELECT
        session_date,
        channel,
        sum(total_sessions) AS sessions,
        sum(new_users) AS new_users,
        sum(purchase_events) AS purchases
    FROM staging.stg_web_sessions
    GROUP BY session_date, channel
),
channel_revenue AS (
    SELECT
        %s AS order_date,
        ws.channel,
        sum(o.order_total) AS attributed_revenue
    FROM staging.stg_orders o
    INNER JOIN staging.stg_web_sessions ws
        ON %s = ws.session_date
    WHERE o.payment_status = 'paid'
    GROUP BY %s, ws.channel
)
SELECT
    cs.spend_date AS report_date,
    cs.channel,
    cs.total_spend,
    cs.total_impressions,
    cs.total_clicks,
    cs.total_conversions,
    sess.sessions,
    sess.new_users,
    cr.attributed_revenue,
    round(cr.attributed_revenue / %s(cs.total_spend, 0), 2) AS roas,
    round(cs.total_spend / %s(cs.total_conversions, 0), 2) AS cost_per_acquisition,
    round(cs.total_clicks / %s(cs.total_impressions, 0) * 100, 2) AS click_through_rate
FROM channel_spend cs
LEFT JOIN channel_sessions sess
    ON cs.spend_date = sess.session_date
    AND cs.channel = sess.channel
LEFT JOIN channel_revenue cr
    ON cs.spend_date = cr.order_date
    AND cs.channel = cr.channel
ORDER BY cs.spend_date DESC, cs.total_spend DESC
`, dateFn, dateFn, dateFn, nullIfFn, nullIfFn, nullIfFn)
}

func generateRptDailyKPIs(c *EcommerceChoices) string {
	header := `/* @bruin
name: reports.rpt_daily_kpis
type: sql
materialization:
  type: table
depends:
  - reports.rpt_daily_revenue
  - staging.stg_customers
  - staging.stg_orders
  - staging.stg_web_sessions
  - staging.stg_marketing_spend
columns:
  - name: kpi_date
    type: date
    checks:
      - name: not_null
      - name: unique
@bruin */

`

	switch c.Warehouse {
	case warehouseClickHouse:
		return header + `WITH daily_customers AS (
    SELECT
        toDate(o.order_date) AS order_date,
        countIf(toDate(c.first_seen_at) = toDate(o.order_date)) AS new_customers,
        countIf(toDate(c.first_seen_at) < toDate(o.order_date)) AS returning_customers
    FROM staging.stg_orders o
    LEFT JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
    GROUP BY toDate(o.order_date)
),
daily_sessions AS (
    SELECT
        session_date,
        sum(total_sessions) AS sessions,
        sum(new_users) AS new_visitors,
        sum(purchase_events) AS purchases
    FROM staging.stg_web_sessions
    GROUP BY session_date
),
daily_spend AS (
    SELECT
        spend_date,
        sum(spend) AS total_ad_spend
    FROM staging.stg_marketing_spend
    GROUP BY spend_date
)
SELECT
    r.order_date AS kpi_date,
    r.net_revenue,
    r.total_orders,
    r.paid_orders,
    r.avg_order_value,
    r.cancellation_rate,
    dc.new_customers,
    dc.returning_customers,
    ds.sessions,
    ds.new_visitors,
    round(ds.purchases / nullIf(ds.sessions, 0) * 100, 2) AS conversion_rate,
    sp.total_ad_spend,
    round(r.net_revenue / nullIf(sp.total_ad_spend, 0), 2) AS overall_roas
FROM reports.rpt_daily_revenue r
LEFT JOIN daily_customers dc ON r.order_date = dc.order_date
LEFT JOIN daily_sessions ds ON r.order_date = ds.session_date
LEFT JOIN daily_spend sp ON r.order_date = sp.spend_date
ORDER BY kpi_date DESC
`
	case warehouseBigQuery:
		return header + `WITH daily_customers AS (
    SELECT
        DATE(o.order_date) AS order_date,
        COUNTIF(DATE(c.first_seen_at) = DATE(o.order_date)) AS new_customers,
        COUNTIF(DATE(c.first_seen_at) < DATE(o.order_date)) AS returning_customers
    FROM staging.stg_orders o
    LEFT JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
    GROUP BY DATE(o.order_date)
),
daily_sessions AS (
    SELECT
        session_date,
        sum(total_sessions) AS sessions,
        sum(new_users) AS new_visitors,
        sum(purchase_events) AS purchases
    FROM staging.stg_web_sessions
    GROUP BY session_date
),
daily_spend AS (
    SELECT
        spend_date,
        sum(spend) AS total_ad_spend
    FROM staging.stg_marketing_spend
    GROUP BY spend_date
)
SELECT
    r.order_date AS kpi_date,
    r.net_revenue,
    r.total_orders,
    r.paid_orders,
    r.avg_order_value,
    r.cancellation_rate,
    dc.new_customers,
    dc.returning_customers,
    ds.sessions,
    ds.new_visitors,
    round(ds.purchases / NULLIF(ds.sessions, 0) * 100, 2) AS conversion_rate,
    sp.total_ad_spend,
    round(r.net_revenue / NULLIF(sp.total_ad_spend, 0), 2) AS overall_roas
FROM reports.rpt_daily_revenue r
LEFT JOIN daily_customers dc ON r.order_date = dc.order_date
LEFT JOIN daily_sessions ds ON r.order_date = ds.session_date
LEFT JOIN daily_spend sp ON r.order_date = sp.spend_date
ORDER BY kpi_date DESC
`
	default: // snowflake
		return header + `WITH daily_customers AS (
    SELECT
        o.order_date::DATE AS order_date,
        COUNT(CASE WHEN c.first_seen_at::DATE = o.order_date::DATE THEN 1 END) AS new_customers,
        COUNT(CASE WHEN c.first_seen_at::DATE < o.order_date::DATE THEN 1 END) AS returning_customers
    FROM staging.stg_orders o
    LEFT JOIN staging.stg_customers c
        ON o.customer_email = c.customer_email
    WHERE o.payment_status = 'paid'
    GROUP BY o.order_date::DATE
),
daily_sessions AS (
    SELECT
        session_date,
        sum(total_sessions) AS sessions,
        sum(new_users) AS new_visitors,
        sum(purchase_events) AS purchases
    FROM staging.stg_web_sessions
    GROUP BY session_date
),
daily_spend AS (
    SELECT
        spend_date,
        sum(spend) AS total_ad_spend
    FROM staging.stg_marketing_spend
    GROUP BY spend_date
)
SELECT
    r.order_date AS kpi_date,
    r.net_revenue,
    r.total_orders,
    r.paid_orders,
    r.avg_order_value,
    r.cancellation_rate,
    dc.new_customers,
    dc.returning_customers,
    ds.sessions,
    ds.new_visitors,
    round(ds.purchases / NULLIF(ds.sessions, 0) * 100, 2) AS conversion_rate,
    sp.total_ad_spend,
    round(r.net_revenue / NULLIF(sp.total_ad_spend, 0), 2) AS overall_roas
FROM reports.rpt_daily_revenue r
LEFT JOIN daily_customers dc ON r.order_date = dc.order_date
LEFT JOIN daily_sessions ds ON r.order_date = ds.session_date
LEFT JOIN daily_spend sp ON r.order_date = sp.spend_date
ORDER BY kpi_date DESC
`
	}
}

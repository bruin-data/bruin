/* @bruin

name: silver.sku_daily_sales
type: sf.sql
description: |
  Daily SKU sales mart joining transactions, product metadata, account
  hierarchy, cost, promotion, distribution, and inventory context.
connection: snowflake-default
tags:
  - energy_drink
  - silver
  - sku_daily_sales
  - off_premise
domains:
  - sales
meta:
  asset_grain: One row per sales date, store, and SKU.
  pipeline_role: Normalized sales fact for dashboards and agent drilldowns.

materialization:
  type: table
  strategy: create+replace
  cluster_by:
    - sales_date
    - sku_id

depends:
  - bronze.sales_transactions
  - bronze.products
  - bronze.sku_market_availability
  - bronze.stores
  - bronze.retailers
  - bronze.product_costs
  - bronze.trade_promotions
  - bronze.distribution_points
  - bronze.inventory_snapshots

columns:
  - name: sales_date
    type: DATE
    description: Sales activity date.
  - name: store_id
    type: VARCHAR
    description: Store or ecommerce market identifier.
  - name: sku_id
    type: VARCHAR
    description: SKU identifier.
  - name: retailer_id
    type: VARCHAR
    description: Retailer account identifier.
  - name: retailer_name
    type: VARCHAR
    description: Retailer account name.
  - name: channel
    type: VARCHAR
    description: Off Premise channel.
  - name: country
    type: VARCHAR
    description: Country market.
  - name: region
    type: VARCHAR
    description: Sales region.
  - name: market
    type: VARCHAR
    description: Market name.
  - name: sku_name
    type: VARCHAR
    description: Product display name.
  - name: lifecycle_type
    type: VARCHAR
    description: Product lifecycle category.
  - name: lifecycle_status
    type: VARCHAR
    description: Product lifecycle state used by SKU council decisions.
  - name: decision_cycle
    type: VARCHAR
    description: SKU council decision cycle.
  - name: market_launch_date
    type: DATE
    description: Country-level modeled launch date.
  - name: market_end_date
    type: DATE
    description: Country-level modeled end date.
  - name: benchmark_group
    type: VARCHAR
    description: Benchmark group for SKU comparisons.
  - name: units_sold
    type: INTEGER
    description: Units sold.
    checks:
      - name: non_negative
  - name: net_sales_usd
    type: DOUBLE
    description: Sales after discounts.
    checks:
      - name: non_negative
  - name: gross_margin_usd
    type: DOUBLE
    description: Net sales less unit cost.
  - name: gross_margin_pct
    type: DOUBLE
    description: Gross margin percentage.
  - name: in_distribution
    type: BOOLEAN
    description: Whether the SKU was in distribution for that store week.
  - name: display_compliant
    type: BOOLEAN
    description: Whether display execution was compliant.
  - name: out_of_stock
    type: BOOLEAN
    description: Whether the inventory snapshot was out of stock.
  - name: promotion_id
    type: VARCHAR
    description: Promotion active on the sales date, if any.
  - name: discount_pct
    type: DOUBLE
    description: Planned trade promotion discount percentage.

@bruin */

WITH sales AS (
    SELECT *
    FROM bronze.sales_transactions
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY updated_at DESC
    ) = 1
),
costs AS (
    SELECT *
    FROM bronze.product_costs
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sku_id, cost_month
        ORDER BY updated_at DESC
    ) = 1
),
products AS (
    SELECT *
    FROM bronze.products
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sku_id
        ORDER BY updated_at DESC
    ) = 1
),
availability AS (
    SELECT *
    FROM bronze.sku_market_availability
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY availability_id
        ORDER BY updated_at DESC
    ) = 1
),
stores AS (
    SELECT *
    FROM bronze.stores
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY updated_at DESC
    ) = 1
),
retailers AS (
    SELECT *
    FROM bronze.retailers
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY retailer_id
        ORDER BY updated_at DESC
    ) = 1
),
distribution AS (
    SELECT *
    FROM bronze.distribution_points
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY distribution_id
        ORDER BY updated_at DESC
    ) = 1
),
distribution_weekly AS (
    SELECT
        DATE_TRUNC('week', activity_date)::DATE AS week_start_date,
        store_id,
        sku_id,
        BOOLAND_AGG(authorized) AS authorized,
        BOOLOR_AGG(in_distribution) AS in_distribution,
        BOOLOR_AGG(display_compliant) AS display_compliant,
        MAX(shelf_facings) AS shelf_facings
    FROM distribution
    GROUP BY 1, 2, 3
),
inventory AS (
    SELECT *
    FROM bronze.inventory_snapshots
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY inventory_snapshot_id
        ORDER BY updated_at DESC
    ) = 1
),
inventory_weekly AS (
    SELECT
        DATE_TRUNC('week', snapshot_date)::DATE AS week_start_date,
        store_id,
        sku_id,
        MAX(on_hand_units) AS on_hand_units,
        BOOLOR_AGG(out_of_stock) AS out_of_stock,
        AVG(days_of_supply) AS days_of_supply
    FROM inventory
    GROUP BY 1, 2, 3
),
promotions AS (
    SELECT *
    FROM bronze.trade_promotions
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY promotion_id
        ORDER BY updated_at DESC
    ) = 1
)
SELECT
    sales.sales_date,
    sales.store_id,
    sales.sku_id,
    stores.retailer_id,
    retailers.retailer_name,
    stores.channel,
    stores.country,
    stores.region,
    stores.market,
    products.sku_name,
    products.product_family,
    products.flavor,
    products.pack_size,
    products.lifecycle_type,
    products.lifecycle_status,
    products.launch_date,
    products.planned_end_date,
    products.decision_cycle,
    products.season_year,
    products.source_confidence,
    COALESCE(availability.market_launch_date, products.launch_date) AS market_launch_date,
    COALESCE(availability.market_end_date, products.planned_end_date) AS market_end_date,
    availability.availability_status,
    availability.rollout_tier,
    products.benchmark_group,
    products.target_margin_pct,
    sales.units_sold,
    sales.gross_sales_usd,
    sales.discount_usd,
    sales.net_sales_usd,
    costs.unit_cost_usd,
    costs.list_price_usd,
    ROUND(sales.net_sales_usd - (sales.units_sold * costs.unit_cost_usd), 2) AS gross_margin_usd,
    DIV0(ROUND(sales.net_sales_usd - (sales.units_sold * costs.unit_cost_usd), 2), sales.net_sales_usd) AS gross_margin_pct,
    COALESCE(distribution_weekly.authorized, FALSE) AS authorized,
    COALESCE(distribution_weekly.in_distribution, FALSE) AS in_distribution,
    COALESCE(distribution_weekly.display_compliant, FALSE) AS display_compliant,
    COALESCE(distribution_weekly.shelf_facings, 0) AS shelf_facings,
    COALESCE(inventory_weekly.on_hand_units, 0) AS on_hand_units,
    COALESCE(inventory_weekly.out_of_stock, FALSE) AS out_of_stock,
    COALESCE(inventory_weekly.days_of_supply, 0) AS days_of_supply,
    promotions.promotion_id,
    promotions.promo_type,
    COALESCE(promotions.discount_pct, 0) AS discount_pct,
    COALESCE(promotions.display_support, FALSE) AS display_support,
    sales.updated_at
FROM sales
INNER JOIN products
    ON sales.sku_id = products.sku_id
INNER JOIN stores
    ON sales.store_id = stores.store_id
INNER JOIN retailers
    ON stores.retailer_id = retailers.retailer_id
LEFT JOIN availability
    ON sales.sku_id = availability.sku_id
    AND stores.country = availability.country
LEFT JOIN costs
    ON sales.sku_id = costs.sku_id
    AND DATE_TRUNC('month', sales.sales_date)::DATE = costs.cost_month
LEFT JOIN distribution_weekly
    ON sales.store_id = distribution_weekly.store_id
    AND sales.sku_id = distribution_weekly.sku_id
    AND DATE_TRUNC('week', sales.sales_date)::DATE = distribution_weekly.week_start_date
LEFT JOIN inventory_weekly
    ON sales.store_id = inventory_weekly.store_id
    AND sales.sku_id = inventory_weekly.sku_id
    AND DATE_TRUNC('week', sales.sales_date)::DATE = inventory_weekly.week_start_date
LEFT JOIN promotions
    ON stores.retailer_id = promotions.retailer_id
    AND sales.sku_id = promotions.sku_id
    AND sales.sales_date BETWEEN promotions.start_date AND promotions.end_date
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sales.sales_date, sales.store_id, sales.sku_id
    ORDER BY promotions.display_support DESC, promotions.discount_pct DESC, promotions.start_date DESC, promotions.promotion_id
) = 1

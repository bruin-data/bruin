/* @bruin
name: stripe_stage.invoice_line_items
type: bq.sql
description: >
  Invoice lines flattened from Stripe's expanded invoice.lines payload. Stripe
  can paginate exceptionally large embedded line lists, so validate this model
  before using it as a complete accounting subledger. The source pagination
  indicator is propagated as invoice_lines_has_more for each flattened line.

materialization:
  type: table
  strategy: truncate+insert

depends:
  - stripe_stage.invoices
  - stripe_stage.prices

tags:
  - stripe_stage
  - stripe
  - billing
  - invoice-lines

columns:
  - name: stripe_invoice_line_item_id
    type: STRING
    description: Stripe invoice-line identifier within an invoice.
    checks:
      - name: not_null
  - name: stripe_invoice_id
    type: STRING
    description: Stripe invoice that contains the line.
    checks:
      - name: not_null
  - name: invoice_lines_has_more
    type: BOOL
    description: >
      Whether the source invoice has additional embedded line pages. True
      means this flattened invoice is incomplete.

custom_checks:
  - name: invoice line keys are unique
    description: >
      Stripe invoice-line identifiers must be unique within each source
      invoice after flattening.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT
          stripe_invoice_id,
          stripe_invoice_line_item_id
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: embedded invoice lines are not paginated
    description: >
      Non-blocking signal: a non-zero result means Stripe reported additional
      invoice.lines pages, so this model is not a complete invoice subledger.
    query: |
      SELECT COUNTIF(invoice_lines_has_more IS TRUE)
      FROM {{ this }}
    value: 0
    blocking: false
@bruin */

WITH flattened_lines AS (
  SELECT
    invoice.stripe_invoice_id,
    invoice.stripe_customer_id,
    invoice.invoice_created_at,
    invoice.invoice_status,
    invoice.billing_reason,
    invoice.currency AS invoice_currency,
    invoice.is_subscription_invoice,
    invoice.invoice_lines_has_more,
    line AS line_json
  FROM stripe_stage.invoices AS invoice
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(invoice.invoice_lines, '$.data'), ARRAY<JSON>[])
  ) AS line
),
typed_lines AS (
  SELECT
    *,
    JSON_VALUE(line_json, '$.id') AS stripe_invoice_line_item_id,
    COALESCE(
      JSON_VALUE(line_json, '$.pricing.price_details.price'),
      JSON_VALUE(line_json, '$.price.id'),
      JSON_VALUE(line_json, '$.price'),
      JSON_VALUE(line_json, '$.plan.id')
    ) AS stripe_price_id,
    COALESCE(
      JSON_VALUE(line_json, '$.parent.subscription_item_details.subscription'),
      JSON_VALUE(line_json, '$.subscription')
    ) AS stripe_subscription_id,
    COALESCE(
      JSON_VALUE(line_json, '$.parent.subscription_item_details.subscription_item'),
      JSON_VALUE(line_json, '$.subscription_item')
    ) AS stripe_subscription_item_id
  FROM flattened_lines
)

SELECT
  line.stripe_invoice_line_item_id,
  line.stripe_invoice_id,
  line.stripe_customer_id,
  line.stripe_subscription_id,
  line.stripe_subscription_item_id,
  line.invoice_created_at,
  line.invoice_status,
  line.billing_reason,
  line.is_subscription_invoice,
  line.invoice_lines_has_more,
  line.stripe_price_id,
  price.stripe_product_id,
  price.product_name,
  COALESCE(JSON_VALUE(line.line_json, '$.currency'), line.invoice_currency) AS currency,
  JSON_VALUE(line.line_json, '$.description') AS line_description,
  COALESCE(
    JSON_VALUE(line.line_json, '$.type'),
    JSON_VALUE(line.line_json, '$.parent.type'),
    'unknown'
  ) AS line_type,
  COALESCE(
    SAFE_CAST(JSON_VALUE(line.line_json, '$.quantity') AS INT64),
    1
  ) AS quantity,
  TIMESTAMP_SECONDS(
    SAFE_CAST(JSON_VALUE(line.line_json, '$.period.start') AS INT64)
  ) AS service_period_started_at,
  TIMESTAMP_SECONDS(
    SAFE_CAST(JSON_VALUE(line.line_json, '$.period.end') AS INT64)
  ) AS service_period_ends_at,
  COALESCE(
    SAFE_CAST(JSON_VALUE(line.line_json, '$.proration') AS BOOL),
    SAFE_CAST(
      JSON_VALUE(
        line.line_json,
        '$.parent.subscription_item_details.proration'
      ) AS BOOL
    ),
    FALSE
  ) AS is_proration,
  SAFE_CAST(JSON_VALUE(line.line_json, '$.amount') AS NUMERIC) AS line_amount_minor,
  (
    SELECT COALESCE(
      SUM(SAFE_CAST(JSON_VALUE(discount, '$.amount') AS NUMERIC)),
      CAST(0 AS NUMERIC)
    )
    FROM UNNEST(
      IFNULL(
        JSON_QUERY_ARRAY(line.line_json, '$.discount_amounts'),
        ARRAY<JSON>[]
      )
    ) AS discount
  ) AS line_discount_minor,
  SAFE_CAST(JSON_VALUE(line.line_json, '$.amount') AS NUMERIC)
    - (
      SELECT COALESCE(
        SUM(SAFE_CAST(JSON_VALUE(discount, '$.amount') AS NUMERIC)),
        CAST(0 AS NUMERIC)
      )
      FROM UNNEST(
        IFNULL(
          JSON_QUERY_ARRAY(line.line_json, '$.discount_amounts'),
          ARRAY<JSON>[]
        )
      ) AS discount
    ) AS line_net_amount_minor,
  price.is_recurring AS is_recurring_price,
  price.recurring_interval,
  price.recurring_usage_type
FROM typed_lines AS line
LEFT JOIN stripe_stage.prices AS price
  ON line.stripe_price_id = price.stripe_price_id;

/* @bruin
name: quickbooks_stage.invoice_lines
type: bq.sql
description: >
  Product and discount lines flattened from QuickBooks invoices, one row per
  line, with the item and income account resolved. Discount lines carry a
  negative amount, so summing `amount` per invoice gives the pre-tax total.
  Subtotal lines are dropped, and bundle (group) lines are not expanded.

materialization:
  type: table

depends:
  - quickbooks_raw.invoices
  - quickbooks_stage.invoices

tags:
  - quickbooks_stage
  - quickbooks
  - accounting
  - invoices
  - invoice-lines

columns:
  - name: invoice_id
    type: STRING
    description: Parent invoice; joins to `quickbooks_stage.invoices`.
    primary_key: true
    checks:
      - name: not_null
  - name: line_index
    type: INT64
    description: Zero-based position of the line in the invoice's line array.
    primary_key: true
    checks:
      - name: not_null
  - name: line_id
    type: STRING
    description: QuickBooks line identifier within the invoice; null for discount lines.
  - name: line_type
    type: STRING
    description: '`sales_item` for product or service lines, `discount` for discount lines.'
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - sales_item
          - discount
  - name: customer_id
    type: STRING
    description: Billed customer of the parent invoice.
  - name: invoice_date
    type: DATE
    description: Issue date of the parent invoice.
  - name: currency
    type: STRING
    description: Currency code of the parent invoice.
  - name: item_id
    type: STRING
    description: Product or service sold; null for discount lines.
  - name: item_name
    type: STRING
    description: Product or service name at the time of the last load.
  - name: income_account_id
    type: STRING
    description: >
      Income account the line posts to (the item's income account, or the
      discount account for discount lines); joins to `quickbooks_stage.accounts`.
  - name: income_account_name
    type: STRING
    description: Name of the income or discount account.
  - name: description
    type: STRING
    description: Line description as shown on the invoice.
  - name: quantity
    type: NUMERIC
    description: Quantity sold; null for discount lines.
  - name: unit_price
    type: NUMERIC
    description: Price per unit; null for discount lines.
  - name: amount
    type: NUMERIC
    description: >
      Line amount before tax, in the invoice currency. Negative for discount
      lines.
    checks:
      - name: not_null
  - name: service_date
    type: DATE
    description: Date the service was performed, when entered on the line.

custom_checks:
  - name: invoice line keys are unique
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT invoice_id, line_index
        FROM {{ this }}
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
      )
    value: 0
  - name: invoice lines add up to the invoice total
    description: >
      Non-blocking: invoices whose lines plus tax do not equal the invoice
      total, typically because they contain bundle (group) lines that this
      model does not expand.
    query: |
      SELECT COUNT(*)
      FROM (
        SELECT invoice.invoice_id
        FROM quickbooks_stage.invoices AS invoice
        LEFT JOIN {{ this }} AS line
          ON line.invoice_id = invoice.invoice_id
        GROUP BY invoice.invoice_id, invoice.total_amount, invoice.tax_amount
        HAVING ABS(IFNULL(SUM(line.amount), 0) + invoice.tax_amount - invoice.total_amount) > 0.01
      )
    value: 0
    blocking: false
unit_tests:
  - name: keeps_item_and_discount_lines
    description: Discounts are negative and subtotal lines are dropped.
    inputs:
      - asset: quickbooks_raw.invoices
        rows:
          - id: "i1"
            txn_date: "2026-05-01"
            customer_ref: {value: "c1", name: "Acme"}
            currency_ref: {value: "USD"}
            line:
              - {Id: "1", LineNum: 1, Amount: 1000, Description: "Growth plan", DetailType: SalesItemLineDetail, SalesItemLineDetail: {ItemRef: {value: "19", name: "Platform Subscription"}, ItemAccountRef: {value: "400", name: "Subscription Revenue"}, Qty: 1, UnitPrice: 1000}}
              - {Amount: 1000, DetailType: SubTotalLineDetail, SubTotalLineDetail: {}}
              - {Amount: 100, DetailType: DiscountLineDetail, DiscountLineDetail: {PercentBased: true, DiscountPercent: 10, DiscountAccountRef: {value: "450", name: "Discounts given"}}}
    expected:
      match: exact
      rows:
        - {invoice_id: "i1", line_index: 0, line_id: "1", line_type: sales_item, customer_id: "c1", currency: USD, item_id: "19", income_account_id: "400", quantity: 1, unit_price: 1000, amount: 1000}
        - {invoice_id: "i1", line_index: 2, line_type: discount, item_id: null, income_account_id: "450", income_account_name: "Discounts given", amount: -100}
@bruin */

WITH lines AS (
  SELECT
    invoice.id AS invoice_id,
    line_index,
    line,
    JSON_VALUE(line, '$.DetailType') AS detail_type,
    JSON_VALUE(invoice.customer_ref, '$.value') AS customer_id,
    invoice.txn_date AS invoice_date,
    JSON_VALUE(invoice.currency_ref, '$.value') AS currency
  FROM quickbooks_raw.invoices AS invoice
  CROSS JOIN UNNEST(
    IFNULL(JSON_QUERY_ARRAY(invoice.line), ARRAY<JSON>[])
  ) AS line WITH OFFSET AS line_index
)

SELECT
  invoice_id,
  line_index,
  JSON_VALUE(line, '$.Id') AS line_id,
  IF(detail_type = 'DiscountLineDetail', 'discount', 'sales_item') AS line_type,
  customer_id,
  invoice_date,
  currency,
  JSON_VALUE(line, '$.SalesItemLineDetail.ItemRef.value') AS item_id,
  JSON_VALUE(line, '$.SalesItemLineDetail.ItemRef.name') AS item_name,
  COALESCE(
    JSON_VALUE(line, '$.SalesItemLineDetail.ItemAccountRef.value'),
    JSON_VALUE(line, '$.DiscountLineDetail.DiscountAccountRef.value')
  ) AS income_account_id,
  COALESCE(
    JSON_VALUE(line, '$.SalesItemLineDetail.ItemAccountRef.name'),
    JSON_VALUE(line, '$.DiscountLineDetail.DiscountAccountRef.name')
  ) AS income_account_name,
  JSON_VALUE(line, '$.Description') AS description,
  SAFE_CAST(JSON_VALUE(line, '$.SalesItemLineDetail.Qty') AS NUMERIC) AS quantity,
  SAFE_CAST(JSON_VALUE(line, '$.SalesItemLineDetail.UnitPrice') AS NUMERIC) AS unit_price,
  ROUND(
    SAFE_CAST(JSON_VALUE(line, '$.Amount') AS NUMERIC)
      * IF(detail_type = 'DiscountLineDetail', -1, 1),
    2
  ) AS amount,
  SAFE_CAST(JSON_VALUE(line, '$.SalesItemLineDetail.ServiceDate') AS DATE) AS service_date
FROM lines
WHERE detail_type IN ('SalesItemLineDetail', 'DiscountLineDetail');

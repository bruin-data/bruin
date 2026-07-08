/* @bruin
name: catalog.product_prices
description: Product pricing snapshot used by merchandising analytics.
tags:
  - self-heal-demo
  - schema-drift-check
materialization:
  type: table
columns:
  - name: product_id
    type: VARCHAR
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: product_name
    type: VARCHAR
  - name: price
    type: DOUBLE
    checks:
      - name: positive
  - name: currency
    type: VARCHAR
  - name: effective_date
    type: DATE
@bruin */

SELECT
    product_id,
    product_name,
    unit_price AS price,
    currency,
    effective_date
FROM raw.product_catalog;

"""@bruin

name: customer_regions
description: Demonstrates a dependency-free Python asset. Bruin runs materialize() in the declared Python image, validates the returned rows against the asset schema, and writes a create+replace ClickHouse table that SQL models can reference through normal lineage.
connection: clickhouse-default
tags:
  - layer:reference
  - domain:commerce
  - runtime:python
domains:
  - commerce
meta:
  data_classification: internal
  freshness_sla: daily
  source_system: code-managed-reference-data

materialization:
  type: table
  strategy: create+replace
image: python:3.13
owner: data-foundations@example.com

parameters:
  enforce_schema: true

columns:
  - name: country
    type: String
    description: Country used to join the country revenue mart.
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: sales_region
    type: LowCardinality(String)
    description: Commercial sales region.
    checks:
      - name: not_null
  - name: support_tier
    type: LowCardinality(String)
    description: Support coverage tier for the market.
    checks:
      - name: accepted_values
        value:
          - strategic
          - standard

@bruin"""

def materialize():
    return [
        {
            "country": "United Kingdom",
            "sales_region": "EMEA",
            "support_tier": "strategic",
        },
        {
            "country": "United States",
            "sales_region": "North America",
            "support_tier": "strategic",
        },
        {
            "country": "Austria",
            "sales_region": "EMEA",
            "support_tier": "standard",
        },
    ]

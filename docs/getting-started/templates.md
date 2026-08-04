# Templates

Bruin templates are ready-to-edit pipeline blueprints. They scaffold the folder structure, `pipeline.yml`, starter assets, and example connection configuration for common use cases.

Use this page as a catalog: pick a category, scan the template cards, then initialize the matching template with `bruin init`.

## Start from a template

Run the interactive wizard when you want to browse templates from the CLI:

```bash
bruin init
```

<img alt="Bruin - init" src="/init-wizard.gif" style="margin: 10px;" />

You can also initialize a template directly:

```bash
bruin init [template-name] [folder-name]
```

Arguments:

- **template-name**: The template to use. If omitted, Bruin uses the default template.
- **folder-name**: The folder to create. If omitted, Bruin uses `bruin-pipeline` for the default template and the template name for other templates.

To see the complete list available in your installed Bruin version:

```bash
bruin init --help
```

## Template catalog

These bundled templates are grouped by the job they help you start. Use the docs search for template names, sources, warehouses, or tags such as `DuckDB`, `Snowflake`, `Shopify`, `Python`, or `demo`.

AI agent skills are installed separately with `bruin ai skills`; see [AI Skills](/commands/ai-skills).

### Analytics demos

<div class="template-grid">
  <a class="template-card" href="./templates-docs/ecommerce-README.html">
    <span class="template-card__category">Business analytics</span>
    <strong>ecommerce</strong>
    <span>Interactive ecommerce pipeline with selected raw sources, staging models, and revenue, customer, product, marketing, and KPI reports.</span>
    <span class="template-card__tags"><code>Shopify</code><code>Stripe</code><code>Klaviyo</code><code>GA4</code></span>
  </a>
  <a class="template-card" href="./templates-docs/demo-snowflake-sales-analytics-README.html">
    <span class="template-card__category">Demo pipeline</span>
    <strong>demo-snowflake-sales-analytics</strong>
    <span>Generated retail sales data in Snowflake with bronze, silver, and gold models for SKU and channel decisions.</span>
    <span class="template-card__tags"><code>Snowflake</code><code>Python</code><code>gold models</code></span>
  </a>
  <a class="template-card" href="./templates-docs/demo-snowflake-salesforce-README.html">
    <span class="template-card__category">Demo pipeline</span>
    <strong>demo-snowflake-salesforce</strong>
    <span>Seeds Salesforce CRM demo data, ingests it into Snowflake, and builds relationship, lending, and marketing analytics.</span>
    <span class="template-card__tags"><code>Salesforce</code><code>Snowflake</code><code>CRM</code></span>
  </a>
  <a class="template-card" href="./templates-docs/self-heal-demo-README.html">
    <span class="template-card__category">Agent test pipeline</span>
    <strong>self-heal-demo</strong>
    <span>Local DuckDB project with separate seed and demo pipelines for realistic duplicate, freshness, schema drift, and quality failures.</span>
    <span class="template-card__tags"><code>DuckDB</code><code>AI agents</code><code>data quality</code></span>
  </a>
</div>

### Local and learning templates

<div class="template-grid">
  <a class="template-card" href="./templates-docs/duckdb-README.html">
    <span class="template-card__category">Local SQL</span>
    <strong>duckdb</strong>
    <span>Small DuckDB pipeline with SQL assets, seed data, macros, materialization, and quality checks.</span>
    <span class="template-card__tags"><code>DuckDB</code><code>SQL</code><code>seed</code></span>
  </a>
  <a class="template-card" href="./templates-docs/python-README.html">
    <span class="template-card__category">Python assets</span>
    <strong>python</strong>
    <span>Shows isolated Python assets using multiple Python versions and dependency files.</span>
    <span class="template-card__tags"><code>Python 3.11</code><code>Python 3.12</code><code>Python 3.13</code></span>
  </a>
  <a class="template-card" href="./templates-docs/chess-README.html">
    <span class="template-card__category">Source example</span>
    <strong>chess</strong>
    <span>Copies chess game and profile data into DuckDB, then builds a player summary SQL model.</span>
    <span class="template-card__tags"><code>Chess</code><code>DuckDB</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/frankfurter-README.html">
    <span class="template-card__category">Credential-free source</span>
    <strong>frankfurter</strong>
    <span>Pulls foreign exchange rates into DuckDB and builds cleaned rates plus currency performance insights.</span>
    <span class="template-card__tags"><code>Frankfurter</code><code>DuckDB</code><code>FX</code></span>
  </a>
</div>

### Warehouses and databases

<div class="template-grid">
  <a class="template-card" href="./templates-docs/athena-README.html">
    <span class="template-card__category">Warehouse SQL</span>
    <strong>athena</strong>
    <span>Amazon Athena SQL assets for sample cars, drivers, payments, and travellers data.</span>
    <span class="template-card__tags"><code>Athena</code><code>AWS</code><code>SQL</code></span>
  </a>
  <a class="template-card" href="./templates-docs/clickhouse-README.html">
    <span class="template-card__category">Feature showcase</span>
    <strong>clickhouse</strong>
    <span>ClickHouse showcase with materialization strategies, seed data, Python, quality checks, lineage, and optional PostgreSQL ingestion.</span>
    <span class="template-card__tags"><code>ClickHouse</code><code>PostgreSQL</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/bronze-silver-postgres-README.html">
    <span class="template-card__category">Layered ELT</span>
    <strong>bronze-silver-postgres</strong>
    <span>Ingests Frankfurter rates into PostgreSQL, then builds a curated silver summary model with checks.</span>
    <span class="template-card__tags"><code>Postgres</code><code>bronze</code><code>silver</code></span>
  </a>
</div>

### Apache Iceberg

An Iceberg connection is a **catalog** (where table metadata lives) plus **storage** (where the data files go). These five differ only in those two blocks; the assets are identical, so they double as a reference for wiring up a catalog and storage pair.

<div class="template-grid">
  <a class="template-card" href="./templates-docs/iceberg-sqlite-local-README.html">
    <span class="template-card__category">Credential-free</span>
    <strong>iceberg-sqlite-local</strong>
    <span>Iceberg tables on your own disk with a SQLite catalog. No accounts, no services &mdash; run it as soon as it is generated.</span>
    <span class="template-card__tags"><code>Iceberg</code><code>SQLite</code><code>local</code></span>
  </a>
  <a class="template-card" href="./templates-docs/iceberg-rest-minio-README.html">
    <span class="template-card__category">Local stack</span>
    <strong>iceberg-rest-minio</strong>
    <span>A REST catalog server in front of MinIO, both from the bundled docker-compose file.</span>
    <span class="template-card__tags"><code>Iceberg</code><code>REST</code><code>MinIO</code></span>
  </a>
  <a class="template-card" href="./templates-docs/iceberg-glue-s3-README.html">
    <span class="template-card__category">AWS</span>
    <strong>iceberg-glue-s3</strong>
    <span>AWS Glue as the catalog with data in S3 &mdash; a managed catalog, so there is no metastore to run.</span>
    <span class="template-card__tags"><code>Iceberg</code><code>Glue</code><code>S3</code></span>
  </a>
  <a class="template-card" href="./templates-docs/iceberg-postgres-gcs-README.html">
    <span class="template-card__category">GCP</span>
    <strong>iceberg-postgres-gcs</strong>
    <span>A Postgres catalog you own, with data in Google Cloud Storage via a service-account key.</span>
    <span class="template-card__tags"><code>Iceberg</code><code>Postgres</code><code>GCS</code></span>
  </a>
  <a class="template-card" href="./templates-docs/iceberg-hadoop-gcsinterop-README.html">
    <span class="template-card__category">No catalog service</span>
    <strong>iceberg-hadoop-gcsinterop</strong>
    <span>Metadata kept in the warehouse itself, on GCS through its S3 interoperability API.</span>
    <span class="template-card__tags"><code>Iceberg</code><code>hadoop</code><code>GCS interop</code></span>
  </a>
</div>

### Source-to-warehouse ingestion

<div class="template-grid">
  <a class="template-card" href="./templates-docs/ai-coding-usage-README.html">
    <span class="template-card__category">AI usage analytics</span>
    <strong>ai-coding-usage</strong>
    <span>Ingests Anthropic Claude Code and Cursor usage into DuckDB, then builds normalized summaries and a DAC dashboard.</span>
    <span class="template-card__tags"><code>Anthropic</code><code>Cursor</code><code>DuckDB</code></span>
  </a>
  <a class="template-card" href="./templates-docs/shopify-bigquery-README.html">
    <span class="template-card__category">Commerce source</span>
    <strong>shopify-bigquery</strong>
    <span>Copies Shopify data into BigQuery and includes a starter transformation asset.</span>
    <span class="template-card__tags"><code>Shopify</code><code>BigQuery</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/shopify-duckdb-README.html">
    <span class="template-card__category">Commerce source</span>
    <strong>shopify-duckdb</strong>
    <span>Copies Shopify data into DuckDB for local ecommerce exploration.</span>
    <span class="template-card__tags"><code>Shopify</code><code>DuckDB</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/shopify-clickhouse-README.html">
    <span class="template-card__category">Commerce analytics</span>
    <strong>shopify-clickhouse</strong>
    <span>Ingests Shopify data into ClickHouse and builds configurable models for orders, customers, products, and daily reporting.</span>
    <span class="template-card__tags"><code>Shopify</code><code>ClickHouse</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/gsheet-bigquery-README.html">
    <span class="template-card__category">Spreadsheet source</span>
    <strong>gsheet-bigquery</strong>
    <span>Copies Google Sheets data into BigQuery with a simple source asset.</span>
    <span class="template-card__tags"><code>Google Sheets</code><code>BigQuery</code></span>
  </a>
  <a class="template-card" href="./templates-docs/gsheet-duckdb-README.html">
    <span class="template-card__category">Spreadsheet source</span>
    <strong>gsheet-duckdb</strong>
    <span>Copies Google Sheets data into DuckDB for a local spreadsheet ingestion workflow.</span>
    <span class="template-card__tags"><code>Google Sheets</code><code>DuckDB</code></span>
  </a>
  <a class="template-card" href="./templates-docs/notion-README.html">
    <span class="template-card__category">Workspace source</span>
    <strong>notion</strong>
    <span>Copies a Notion database into BigQuery, then runs a starter SQL model.</span>
    <span class="template-card__tags"><code>Notion</code><code>BigQuery</code><code>ingestr</code></span>
  </a>
  <a class="template-card" href="./templates-docs/gorgias-README.html">
    <span class="template-card__category">Support source</span>
    <strong>gorgias</strong>
    <span>Copies Gorgias customers, tickets, messages, and satisfaction surveys into BigQuery.</span>
    <span class="template-card__tags"><code>Gorgias</code><code>BigQuery</code><code>support</code></span>
  </a>
  <a class="template-card" href="./templates-docs/firebase-README.html">
    <span class="template-card__category">Product analytics</span>
    <strong>firebase</strong>
    <span>Builds BigQuery assets for Firebase Analytics events, parameters, users, and cohorts.</span>
    <span class="template-card__tags"><code>Firebase</code><code>BigQuery</code><code>events</code></span>
  </a>
</div>

## Migration templates

Migration templates provide a review-gated starting point for moving existing
data integrations to Bruin.

| Template | Use when you want to start with |
| --- | --- |
| `migration-fivetran` | A review-gated Fivetran migration workspace with a blank `bruin/` pipeline, migration prompt, plan, and agent skill. |

## Other bundled templates

Bruin also ships smaller starter templates and specialized examples that may not have full walkthrough pages yet.

| Template | Use when you want to start with |
| --- | --- |
| `default` | The standard Bruin starter project. |
| `empty` | A minimal pipeline with almost no scaffolding. |
| `bigquery` | A BigQuery SQL starter with seed data and macros. |
| `databricks` | A Databricks SQL starter. |
| `redshift` | A Redshift SQL starter. |
| `oracle-duckdb` | Oracle source assets with a DuckDB transformation. |
| `stripe-databricks` | Stripe ingestion into Databricks with bronze and silver layers. |
| `nyc-taxi` | A local NYC taxi example with ingestion, lookup data, staging, and reports. |
| `bruin-cloud` | Bruin Cloud metadata ingestion and summary examples. |
| `r` | R assets, including an example with dependencies. |
| `variant-example` | A runnable project showing pipeline variants. |
| `duckdb-lineage` | A DuckDB example focused on lineage. |
| `bootstrap` | A small bootstrap example with seed data. |
| `zoomcamp` | The Data Engineering Zoomcamp-inspired pipeline example. |

Initialize any of these by name:

```bash
bruin init nyc-taxi my-taxi-pipeline
```

## Choosing a template

| Goal | Start with |
| --- | --- |
| Learn Bruin locally without cloud credentials | `duckdb`, `python`, `frankfurter`, `chess`, or `iceberg-sqlite-local` |
| Load into an Iceberg lakehouse | `iceberg-sqlite-local` to try it, then `iceberg-glue-s3`, `iceberg-postgres-gcs`, `iceberg-rest-minio`, or `iceberg-hadoop-gcsinterop` |
| Build a source-to-warehouse ingestion pipeline | `ai-coding-usage`, `shopify-bigquery`, `shopify-clickhouse`, `gsheet-bigquery`, `notion`, or `gorgias` |
| Migrate one Fivetran connection to a review-gated Bruin project | `migration-fivetran` |
| Explore a complete demo with generated data | `demo-snowflake-sales-analytics` or `demo-snowflake-salesforce` |
| Scaffold ecommerce reporting | `ecommerce` |
| Work with a specific database | `athena`, `clickhouse`, `bronze-silver-postgres`, `bigquery`, `databricks`, or `redshift` |

Most templates include placeholder connection values only. Replace the generated `.bruin.yml` values before running the pipeline.

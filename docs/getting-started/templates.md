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
    <span class="template-card__category">Warehouse SQL</span>
    <strong>clickhouse</strong>
    <span>ClickHouse SQL pipeline with a sample table and schema checks.</span>
    <span class="template-card__tags"><code>ClickHouse</code><code>SQL</code><code>checks</code></span>
  </a>
  <a class="template-card" href="./templates-docs/bronze-silver-postgres-README.html">
    <span class="template-card__category">Layered ELT</span>
    <strong>bronze-silver-postgres</strong>
    <span>Ingests Frankfurter rates into PostgreSQL, then builds a curated silver summary model with checks.</span>
    <span class="template-card__tags"><code>Postgres</code><code>bronze</code><code>silver</code></span>
  </a>
</div>

### Source-to-warehouse ingestion

<div class="template-grid">
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
| Learn Bruin locally without cloud credentials | `duckdb`, `python`, `frankfurter`, or `chess` |
| Build a source-to-warehouse ingestion pipeline | `shopify-bigquery`, `gsheet-bigquery`, `notion`, or `gorgias` |
| Explore a complete demo with generated data | `demo-snowflake-sales-analytics` or `demo-snowflake-salesforce` |
| Scaffold ecommerce reporting | `ecommerce` |
| Work with a specific database | `athena`, `clickhouse`, `bronze-silver-postgres`, `bigquery`, `databricks`, or `redshift` |

Most templates include placeholder connection values only. Replace the generated `.bruin.yml` values before running the pipeline.

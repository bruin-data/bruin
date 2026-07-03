# Bruin - Salesforce to Snowflake Demo Template

This template creates an end-to-end demo pipeline for a credit union CRM analytics use case.
It seeds deterministic dummy records into Salesforce, ingests Salesforce standard objects into Snowflake,
and builds silver and gold analytics tables for relationship health, lending pipeline, marketing funnel,
product performance, and banker activity coverage.

## Included assets

- `assets/bronze/seed_salesforce_demo_data.py` generates deterministic Salesforce demo data for the Bruin interval.
- `assets/bronze/*.asset.yml` ingests Salesforce objects into Snowflake with `ingestr`.
- `assets/silver/*.sql` builds curated CRM marts from the bronze Salesforce tables.
- `assets/gold/*.sql` builds dashboard- and agent-ready tables.

The template includes only placeholder connection values. Replace them in `.bruin.yml` before running.

## Setup

Initialize the template:

```bash
bruin init demo-snowflake-salesforce my-salesforce-demo
cd my-salesforce-demo
```

Edit `.bruin.yml` with a Snowflake connection named `snowflake-default` and a Salesforce connection named `salesforce`.

The Salesforce seed asset supports either an OAuth access token:

```yaml
salesforce:
  - name: "salesforce"
    access_token: "YOUR_SALESFORCE_ACCESS_TOKEN"
    domain: "https://your-domain.my.salesforce.com"
```

or username/password/security-token auth:

```yaml
salesforce:
  - name: "salesforce"
    username: "YOUR_SALESFORCE_USERNAME"
    password: "YOUR_SALESFORCE_PASSWORD"
    token: "YOUR_SALESFORCE_SECURITY_TOKEN"
    domain: "https://your-domain.my.salesforce.com"
```

Install the Python dependencies used by the Salesforce seed asset if your Bruin environment does not install them automatically:

```bash
pip install -r assets/bronze/requirements.txt
```

## Running the demo

Validate the pipeline:

```bash
bruin validate --fast .
```

Run a small interval first. The seed asset creates dummy Salesforce records, then the bronze ingestion assets sync them into Snowflake:

```bash
CREDIT_UNION_ACCOUNTS_PER_DAY=1 \
CREDIT_UNION_CONTACTS_PER_ACCOUNT=1 \
CREDIT_UNION_OPPORTUNITIES_PER_ACCOUNT=1 \
CREDIT_UNION_TASKS_PER_OPPORTUNITY=1 \
CREDIT_UNION_LEADS_PER_DAY=1 \
CREDIT_UNION_EVENTS_PER_OPPORTUNITY=1 \
bruin run --full-refresh --start-date 2026-01-01 --end-date 2026-01-03 .
```

To inspect the generated records without writing to Salesforce:

```bash
CREDIT_UNION_DRY_RUN=1 bruin run --start-date 2026-01-01 --end-date 2026-01-02 assets/bronze/seed_salesforce_demo_data.py
```

For longer historical runs, keep the `CREDIT_UNION_*` volume settings low unless the Salesforce org has enough storage.

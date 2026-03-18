# Bronze to Silver Snowflake Pipeline

## Overview

This template demonstrates a **bronze-to-silver data pipeline** using Snowflake as the destination.

It ingests exchange rate data from the Frankfurter API into a **bronze layer** (raw data) and performs transformations to generate aggregated insights in a **silver layer**.

The pipeline showcases a complete **ingestion → transformation → validation workflow** using Bruin.

---

## Architecture

Frankfurter API  
↓  
Bronze Layer (Raw Ingestion - Snowflake)  
↓  
Silver Layer (Aggregations & Metrics - Snowflake)

---

## Features

- Ingestion from a public API (no credentials required)
- Bronze layer for raw data storage
- Silver layer with:
  - Latest exchange rates
  - 7-day and 30-day averages
  - Observation counts
- Data quality checks:
  - Not null checks
  - Positive value checks
  - Freshness validation
  - Uniqueness constraints
- Snowflake-native SQL transformations

---

## Project Structure
bronze-silver-snowflake/
│
├── pipeline.yml
├── .bruin.yml
├── README.md
└── assets/
   ├── bronze_raw_data.asset.yml
   └── silver_aggregated.sql


---

## Snowflake Setup

Before running the pipeline, configure your Snowflake connection in `.bruin.yml`:

```bruin.yaml
snowflake:
  - name: "snowflake-default"
    account: "YOUR_ACCOUNT"
    username: "YOUR_USERNAME"
    password: "YOUR_PASSWORD"
    database: "YOUR_DATABASE"
    warehouse: "COMPUTE_WH"
    schema: "PUBLIC"
```
Replace placeholders with your Snowflake credentials for local testing.
⚠️ Do not commit real credentials to GitHub.

---
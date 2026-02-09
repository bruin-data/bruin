# Bruin - Databricks Pipeline Template

A simple Databricks pipeline template that demonstrates querying data from one catalog and storing aggregated results in another catalog.

## Project Structure

```text
databricks/
├── pipeline.yml                    # Pipeline configuration
├── README.md                      # This file
└── assets/                        # Asset definitions
    └── trips_summary_monthly.sql  # Monthly trips aggregation
```

## Setup

### 1. Initialize the Project

```bash
bruin init databricks my-databricks-project
```

### 2. Set Up Databricks

#### Create a SQL Warehouse

1. In your Databricks workspace, go to **SQL Warehouses** (left sidebar)
2. Click **Create SQL Warehouse**
3. Configure your warehouse (name, cluster size, etc.)
4. Once created, click on the warehouse and go to **Connection details**
5. Copy the **HTTP path** (format: `/sql/1.0/warehouses/...`)

#### Set Up Sample Data (NYC Taxi Trips)

The template uses the `samples.nyctaxi.trips` table which is available in Databricks sample datasets:

1. In Databricks, go to **Catalog** (left sidebar)
2. Navigate to the **samples** catalog
3. Expand **nyctaxi** schema
4. Verify the **trips** table exists (it should be pre-populated with sample data)

If the table doesn't exist, you can create it by running:
```sql
CREATE TABLE samples.nyctaxi.trips AS
SELECT * FROM samples.nyctaxi.trips_raw
LIMIT 1000000;
```

#### Create a New Catalog for Your Results

1. In Databricks, go to **Catalog** (left sidebar)
2. Click **Create Catalog**
3. Name it **bruin** (or your preferred name)
4. Ensure you have **CREATE SCHEMA** permissions on this catalog

Note: When a new catalog is created, it automatically creates a schema named `default`, so no additional schema creation is needed.

### 3. Configure Databricks Connection

Add your Databricks connection to `.bruin.yml` in your project root. You can use either a **token** or **OAuth** for authentication.

#### Option 1: Using Token Authentication

```yaml
connections:
  databricks:
    - name: databricks-default
      token: "your-databricks-token"
      path: "/sql/1.0/warehouses/your-warehouse-id"
      host: "your-workspace.cloud.databricks.com"
      port: 443
      catalog: "bruin"
      schema: "default"
```

#### Option 2: Using OAuth Authentication

For OAuth configuration, see the [Databricks documentation](https://getbruin.com/docs/bruin/platforms/databricks.html) for details on setting up OAuth credentials.

#### Getting Your Credentials

1. **Generate a token** (for token authentication): 
   - Click your username → Settings → Developer → Access tokens → Generate new token

2. **Get HTTP path**: 
   - SQL Warehouses → Your warehouse → Connection details → Copy HTTP path

3. **Get host**: 
   - Found in your browser address bar (e.g., `dbc-example.cloud.databricks.com`)

4. **Port**: Usually `443` for HTTPS

5. **Catalog and Schema**: Set to your results catalog (e.g., `bruin` and `default`)


## Running the Pipeline

**Note:** All Bruin commands should be executed from inside the pipeline folder (e.g., `my-databricks-project/`). If running commands from outside the pipeline folder, use the full path to the asset or pipeline file instead of just the asset name or `pipeline.yml`.

### Test Connection

```bash
bruin connections list
```

### Run with Full Refresh

```bash
bruin run --full-refresh --environment default pipeline.yml
```

### Run Specific Asset

```bash
bruin run assets/trips_summary_monthly.sql
```

## Querying Results

Query the created table:

```bash
bruin query --connection databricks-default --query "SELECT * FROM default.trips_summary_monthly ORDER BY month DESC LIMIT 10"
```

Or in Databricks SQL:
```sql
SELECT * FROM bruin.default.trips_summary_monthly
ORDER BY month DESC;
```

## How It Works

This pipeline:
1. **Reads** from `samples.nyctaxi.trips` (source catalog)
2. **Aggregates** trips by month (total trips and fare amounts)
3. **Stores** results in `bruin.default.trips_summary_monthly` (target catalog)

The asset uses fully qualified table names to read from the `samples` catalog while writing to the `bruin` catalog, allowing you to query data from one catalog and store results in another.

## Resources

- [Databricks Documentation](https://getbruin.com/docs/bruin/platforms/databricks)
- [Bruin CLI Documentation](https://getbruin.com/docs/bruin/)

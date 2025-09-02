# RevenueCat
[RevenueCat](https://www.revenuecat.com/) is a subscription management platform that helps mobile app developers build, analyze, and grow their subscription businesses.

Bruin supports RevenueCat as a source for [Ingestr assets](/assets/ingestr). You can ingest data from RevenueCat into your data platform.

To set up a RevenueCat connection, add a configuration item in the `.bruin.yml` file and in your asset file. The configuration requires `api_key`.

### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
  revenuecat:
    - name: "revenuecat"
      api_key: "rc_api_123"
      project_id: "proj_123123123123"
```
- `api_key`: RevenueCat API key.

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `revenuecat_ingestion.yml`) inside the assets folder with the following content:
```yaml
name: public.revenuecat
type: ingestr

parameters:
  source_connection: revenuecat
  source_table: 'customers'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Always `ingestr` for RevenueCat.
- `source_connection`: The RevenueCat connection name defined in `.bruin.yml`.
- `source_table`: Name of the RevenueCat table to ingest.
- `destination`: The destination connection name.

## Available Source Tables

Table    PK    Inc Key    Inc Strategy    Details
projects    id        merge    Fetches all projects from your RevenueCat account.
customers    id        merge    Fetches all customers with nested purchases and subscriptions data.
products    id        merge    Fetches all products configured in your RevenueCat project.
entitlements    id        merge    Fetches all entitlements configured in your RevenueCat project.
offerings    id        merge    Fetches all offerings configured in your RevenueCat project.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/revenuecat_ingestion.yml
```
Running this command ingests data from RevenueCat into your Postgres database.

## Notes
- The `project_id` parameter is required for customers and products tables but not for projects.



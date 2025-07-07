# Trustpilot
[Trustpilot](https://www.trustpilot.com/) provides a platform for collecting and sharing customer reviews.

Bruin supports Trustpilot as a source for [Ingestr assets](/assets/ingestr), allowing you to ingest reviews into your data platform.

To connect to Trustpilot you must define a connection in the `.bruin.yml` file and reference it from your asset configuration. The connection requires `business_unit_id` and `api_key`.

Follow the steps below to configure Trustpilot and run ingestion.

### Step 1: Add a connection to the `.bruin.yml` file
```yaml
connections:
  trustpilot:
    - name: "trustpilot"
      business_unit_id: "<business-unit-id>"
      api_key: "<api-key>"
```
- `business_unit_id`: Identifier of the business unit to fetch reviews for.
- `api_key`: Trustpilot API key.

### Step 2: Create an asset file for data ingestion
Create an [asset configuration](/assets/ingestr#asset-structure) file to define the data flow:
```yaml
name: public.trustpilot_reviews
type: ingestr

parameters:
  source_connection: trustpilot
  source_table: 'reviews'

  destination: postgres
```
- `source_connection`: The Trustpilot connection name defined in `.bruin.yml`.
- `source_table`: Table to ingest. Currently only `reviews` is supported.
- `destination`: Destination connection name.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/trustpilot_asset.yml
```
Executing this command ingests data from Trustpilot into your Postgres database.

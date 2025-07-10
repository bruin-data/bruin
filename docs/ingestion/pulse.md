# Pulse

[Internet Society Pulse](https://pulse.internetsociety.org/) provides metrics on internet health and security.

Bruin supports Pulse as a source for [Ingestr assets](/assets/ingestr), allowing you to ingest Pulse data into your warehouse.

This uses the `isoc-pulse` source from [Ingestr](https://github.com/bruin-data/ingestr), generating `isoc-pulse://` URIs.
## Step 1: Add a connection to .bruin.yml file

Add a Pulse configuration under the `connections` section:

```yaml
connections:
  pulse:
    - name: "my-pulse"
      token: "your_token_here"
```

- `token`: API token for the Pulse API.

## Step 2: Create an asset file for data ingestion

Create an asset configuration file, e.g. `pulse_ingestion.asset.yml`:

```yaml
name: public.https_adoption_us
type: ingestr
connection: postgres

parameters:
  source_connection: my-pulse
  source_table: "https:US"
  destination: postgres
```

- `source_connection`: The Pulse connection defined in `.bruin.yml`.
- `source_table`: The metric to ingest.

## Step 3: Run the asset

```
bruin run assets/pulse_ingestion.asset.yml
```

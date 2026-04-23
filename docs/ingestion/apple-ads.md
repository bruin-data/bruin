# Apple Ads

[Apple Ads](https://ads.apple.com/) (Apple Search Ads) is Apple's advertising platform for the App Store. App developers buy placements in the App Store's Search results, Today tab, "Suggested" list, and product pages — billed per tap or per install. Ads are rendered from the advertiser's App Store Connect listing; there is no separate creative upload.

Bruin supports Apple Ads as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest the structural data behind those ads — campaigns, ad groups, ads, and creatives — into your data warehouse via the [Apple Ads Campaign Management API v5](https://developer.apple.com/documentation/apple_ads).

In order to set up an Apple Ads connection, you need to add a configuration item in the `.bruin.yml` file and in the `asset` file.

Follow the steps below to correctly set up Apple Ads as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Apple Ads, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      appleads:
        - name: "my-appleads"
          client_id: "SEARCHADS.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          team_id: "SEARCHADS.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          key_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          org_id: "11111111"
          key_path: /path/to/private-key.pem

          # alternatively, you can specify the base64-encoded key inline
          key_base64: "LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0t..."
```

* `client_id`: Apple-issued API client identifier, format `SEARCHADS.<uuid>`.
* `team_id`: Apple-issued team identifier, format `SEARCHADS.<uuid>`. Usually the same value as `client_id`.
* `key_id`: Apple-issued key identifier for your uploaded public key (short UUID).
* `org_id`: Numeric organization/account ID. Comma-separated for multi-account ingestion (e.g. `11111111,98765432`).
* `key_path`: Absolute filesystem path to the EC private key PEM file (P-256).
* `key_base64`: Base64-encoded contents of the EC private key PEM file. Use when no filesystem access is available.

You must provide either `key_path` or `key_base64`.

### Setting up Apple Ads credentials

For Apple's official walkthrough, see [Implementing OAuth for the Apple Search Ads API](https://developer.apple.com/documentation/apple_ads/implementing-oauth-for-the-apple-search-ads-api).

#### 1. Generate an EC key pair

Apple Ads uses ES256 (ECDSA P-256). Generate the pair locally — Apple never sees your private key.

```bash
openssl ecparam -genkey -name prime256v1 -noout -out private-key.pem
openssl ec -in private-key.pem -pubout -out public-key.pem
```

#### 2. Create an API user in Apple Ads

1. Sign in to [Apple Ads](https://app.searchads.apple.com/) with an account that has admin permissions.
2. Go to **Account Settings → API**.
3. Invite a new API user and assign it a role (typically **API Campaign Manager**).
4. Upload the `public-key.pem` file you generated.
5. Apple will display:
   - **Client ID** (`SEARCHADS.<uuid>`)
   - **Team ID** (`SEARCHADS.<uuid>`)
   - **Key ID** (short UUID)

Copy these three values.

#### 3. Find your Organization ID

Organization IDs are numeric and visible in:

- Apple Ads UI → **Settings → Overview**.
- The Apple Ads UI URL when browsing your account.

### Step 2: Create an asset file for data ingestion

To ingest data from Apple Ads, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., `appleads_ingestion.yml`) inside the `assets` folder and add the following content:

```yaml
name: public.campaigns
type: ingestr

parameters:
  source_connection: my-appleads
  source_table: 'campaigns'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr` to use the ingestr data pipeline.
- `parameters`: A list of key–value pairs that specify the parameters for the ingestr asset:
  - `source_connection`: The name of the Apple Ads connection defined in `.bruin.yml`.
  - `source_table`: The name of the table to fetch. Supported tables: `campaigns`, `ad_groups`, `ads`, `creatives`.
  - `destination`: The destination platform where the data will be ingested (e.g., `postgres`).

### Step 3: [Run](/commands/run) asset to ingest data

```
bruin run assets/appleads_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given Apple Ads table into your destination database.

## Tables

| Table | Primary Key | Incremental Key | Strategy | Details |
|---|---|---|---|---|
| `campaigns` | `[orgId, id]` | `modificationTime` | merge | All ad campaigns in the organization, including budget, status, countries/regions, and timeframe. |
| `ad_groups` | `[orgId, id]` | `modificationTime` | merge | Targeting groups inside each campaign — bids, audience/device/geo targeting, and keyword-matching settings. |
| `ads` | `[orgId, id]` | `modificationTime` | merge | Individual ads within each ad group, linking a creative to its ad group and serving status. |
| `creatives` | `[orgId, id]` | `modificationTime` | merge | Creative assets registered to the organization (custom product pages, text, media) that ads reference. |

## Notes

- **The [current API](https://developer.apple.com/documentation/apple_ads/apple-search-ads-campaign-management-api-5) will be sunset on January 26, 2027.** Apple has announced the [Apple Ads Platform API](https://developer.apple.com/documentation/apple_ads) as the replacement. A separate `apple_ads_platform` source may be added to cover that API when it becomes generally available.
- **Apple's `id` is not globally unique.** It is only unique within an organization. Always join on the composite `[orgId, id]` when querying across orgs.

# Square

[Square](https://squareup.com/) is a payments and commerce platform for businesses.

Bruin supports Square as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Square into your data warehouse.

To set up a Square connection, you need a Square access token. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/square.html)

Follow the steps below to correctly set up Square as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to Square, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      square:
        - name: "my_square"
          access_token: "YOUR_SQUARE_ACCESS_TOKEN"
          environment: "production"
```

- `access_token`: Your Square access token. Required.
- `environment` (optional): Either `production` (default) or `sandbox`. Use `sandbox` to read from Square's developer sandbox.

### Step 2: Create an asset file for data ingestion

To ingest data from Square, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., square_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.square
type: ingestr
connection: postgres

parameters:
  source_connection: my_square
  source_table: 'payments'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Square.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Square connection defined in .bruin.yml.
- `source_table`: The name of the data table in Square you want to ingest. For example, `payments` would ingest payment records.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| `payments` | id | updated_at | merge | Payments taken by the account. |
| `refunds` | id | updated_at | merge | Refunds processed against payments. |
| `orders` | id | updated_at | merge | Orders across all of the account's locations. |
| `customers` | id | updated_at | merge | Customer profiles. |
| `catalog_objects` | id | updated_at | merge | All catalog objects (items, variations, categories, taxes, discounts, modifier lists, images, and more). Deletions surface as `is_deleted=true` rows. |
| `team_members` | id | updated_at | merge | Team members (staff). Soft-deletes appear as `status="INACTIVE"`. |
| `inventory` | catalog_object_id, location_id, state | calculated_at | merge | Inventory counts per catalog object, location, and state. |
| `locations` | id | - | replace | The account's locations. |
| `team_member_wages` | id | - | replace | Hourly wage settings per team member. |
| `shifts` | id | - | replace | Worked shifts (Labor API). |
| `bank_accounts` | id | - | replace | Linked bank accounts. |
| `cash_drawers` | id, location_id | - | replace | Cash drawer shifts across all locations. |
| `loyalty` | id | - | replace | Loyalty accounts (requires a paid Square Loyalty subscription). |

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run ingestr.square.asset.yml
```

As a result of this command, Bruin will ingest data from the given Square table into your Postgres database.

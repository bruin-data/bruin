# Klaviyo
[Klaviyo](https://www.Klaviyo.com/) is a marketing automation platform that helps businesses build and manage digital relationships with their customers by connecting through personalized email and enhancing customer loyalty.

Bruin supports Klaviyo as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Klaviyo into your data warehouse.

To set up a Klaviyo connection, you need to have Klaviyo API key and source table. For more information, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/klaviyo.html)

Follow the steps below to correctly set up Klaviyo as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Klaviyo, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      klaviyo:
        - name: "my_klaviyo"
          api_key: "YOUR_KLAVIYO_API_KEY"
```
- `api_key`: The API key used for authentication with the Klaviyo API.

### Step 2: Create an asset file for data ingestion

To ingest data from Klaviyo, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., klaviyo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.klaviyo
type: ingestr
connection: postgres

parameters:
  source_connection: my_klaviyo
  source_table: 'events'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Klaviyo.
- `connection`: This is the destination connection. 
- `source_connection`: The name of the Klaviyo connection defined in .bruin.yml.
- `source_table`: The name of the data table in klaviyo you want to ingest. For example, `events` would ingest data related to events.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| events | id | datetime | merge | Retrieves all events in an account where each event represents an action taken by a profile such as a password reset or a product order. |
| profiles | id | updated | merge | Retrieves all profiles in an account where each profile includes details like organization, job title, email and other attributes. |
| campaigns | id | updated_at | merge | Retrieves all campaigns in an account where each campaign is a targeted message sent to a specific audience. |
| metrics | id | updated | merge | Retrieves all metrics in an account where each metric represents a category of events or actions a person can take. |
| tags | id | – | replace | Retrieves all tags in an account. |
| coupons | id | – | replace | Retrieves all coupons in an account. |
| catalog-variants | id | updated | merge | Retrieves all variants in an account. |
| catalog-categories | id | updated | merge | Retrieves all catalog categories in an account. |
| catalog-items | id | updated | merge | Retrieves all catalog items in an account. |
| flows | id | updated | merge | Retrieves all flows in an account where flow is a sequence of automated actions that is triggered when a person performs a specific action. |
| lists | id | updated | merge | Retrieves all lists in an account. |
| images | id | updated_at | merge | Retrieves all images in an account. |
| segments | id | updated | merge | Retrieves all segments in an account where segment is a dynamic list that contains profiles meeting a certain set of conditions. |
| forms | id | updated_at | merge | Retrieves all forms in an account. |
| templates | id | updated | merge | Retrieves all templates in an account. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run ingestr.klaviyo.asset.yml
```
As a result of this command, Bruin will ingest data from the given Klaviyo table into your Postgres database.
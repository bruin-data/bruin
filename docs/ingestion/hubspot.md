# HubSpot

[HubSpot](https://www.hubspot.com/) is a customer relationship management software that helps businesses attract visitors, connect with customers, and close deals.

Bruin supports HubSpot as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from HubSpot into your data warehouse.

In order to set up HubSpot connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. You will need an `api_key`, which can be either a **private app access token** or a HubSpot **service key** — both are sent to HubSpot as a bearer token, so either works.

- For a **service key**, go to Settings → Integrations → Service Keys in your HubSpot account, create a key, grant it the required scopes, and copy the generated key. HubSpot has moved private apps to "legacy apps" and now recommends service keys for system-to-system data integrations.
- For a **private app access token**, go to Settings → Integrations → Private Apps, create a private app, grant it the required scopes, and copy the generated access token.

> **Note:** HubSpot service keys are currently in **public beta**. The feature may change before general availability, so keep that in mind before relying on it for production workloads.

Follow the steps below to correctly set up HubSpot as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to HubSpot, you need to add a configuration item to the connections section of the .bruin.yml file. This configuration must comply with the following schema:

```yaml
    connections:
      hubspot:
        - name: "my-hubspot"
          api_key: "pat-123"
```

- `name`: The name of the connection
- `api_key`: The credential used for authentication with the HubSpot API. Accepts either a private app access token or a HubSpot service key.

### Step 2: Create an asset file for data ingestion

To ingest data from HubSpot, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., hubspot_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.hubspot
type: ingestr
connection: postgres

parameters:
  source_connection: my-hubspot
  source_table: 'companies'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the hubspot connection defined in .bruin.yml.
- `source_table`: The name of the data table in hubspot that you want to ingest. For example, `companies` is a data table in hubspot that you may want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| --------------- | ----------- | --------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| [companies](https://developers.hubspot.com/docs/reference/api/crm/objects/companies#get-%2Fcrm%2Fv3%2Fobjects%2Fcompanies) | hs_object_id | hs_lastmodifieddate | merge | Retrieves information about organizations. |
| [contacts](https://developers.hubspot.com/docs/reference/api/crm/objects/contacts#get-%2Fcrm%2Fv3%2Fobjects%2Fcontacts) | hs_object_id | lastmodifieddate | merge | Retrieves information about visitors, potential customers, and leads. |
| [deals](https://developers.hubspot.com/docs/reference/api/crm/objects/deals#get-%2Fcrm%2Fv3%2Fobjects%2Fdeals) | hs_object_id | hs_lastmodifieddate | merge | Retrieves deal records and tracks deal progress. |
| [tickets](https://developers.hubspot.com/docs/api-reference/crm-tickets-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Handles requests for help from customers or users. |
| [products](https://developers.hubspot.com/docs/api-reference/crm-products-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves pricing information of products. |
| [quotes](https://developers.hubspot.com/docs/reference/api/crm/objects/quotes#get-%2Fcrm%2Fv3%2Fobjects%2Fquotes) | hs_object_id | hs_lastmodifieddate | merge | Retrieves price proposals that salespeople can create and send to their contacts. |
| [calls](https://developers.hubspot.com/docs/api-reference/crm-calls-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves call engagement records. |
| [emails](https://developers.hubspot.com/docs/api-reference/crm-emails-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves email engagement records. |
| [feedback_submissions](https://developers.hubspot.com/docs/reference/api/crm/objects/feedback-submissions) | hs_object_id | hs_lastmodifieddate | merge | Retrieves customer feedback survey responses. |
| [line_items](https://developers.hubspot.com/docs/api-reference/crm-line-items-v3/guide#crm-api-line-items) | hs_object_id | hs_lastmodifieddate | merge | Retrieves individual products or services associated with deals. |
| [meetings](https://developers.hubspot.com/docs/api-reference/crm-meetings-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves meeting engagement records. |
| [notes](https://developers.hubspot.com/docs/api-reference/crm-notes-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves note engagement records. |
| [tasks](https://developers.hubspot.com/docs/api-reference/crm-tasks-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves task engagement records. |
| [carts](https://developers.hubspot.com/docs/reference/api/crm/objects/carts) | hs_object_id | hs_lastmodifieddate | merge | Retrieves shopping cart records. |
| [discounts](https://developers.hubspot.com/docs/reference/api/crm/objects/discounts) | hs_object_id | hs_lastmodifieddate | merge | Retrieves discount records. |
| [fees](https://developers.hubspot.com/docs/api-reference/crm-fees-v3/guide#crm-api-fees) | hs_object_id | hs_lastmodifieddate | merge | Retrieves fee records. |
| [invoices](https://developers.hubspot.com/docs/api-reference/crm-invoices-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves invoice records. |
| [commerce_payments](https://developers.hubspot.com/docs/api-reference/crm-commerce-payments-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves commerce payment records. |
| [taxes](https://developers.hubspot.com/docs/api-reference/crm-taxes-v3/guide) | hs_object_id | hs_lastmodifieddate | merge | Retrieves tax records. |
| [owners](https://developers.hubspot.com/docs/api-reference/crm-crm-owners-v3/guide#crm-api-owners) | id | – | merge | Retrieves HubSpot users who can be assigned to CRM records. |
| [schemas](https://developers.hubspot.com/docs/reference/api/crm/objects/schemas#get-%2Fcrm-object-schemas%2Fv3%2Fschemas) | id | – | merge | Returns all object schemas that have been defined for your account. |
| [pipelines](https://developers.hubspot.com/docs/reference/api/crm/pipelines) | object_type, pipeline_id | – | replace | Pipeline definitions across all pipelined object types. One row per pipeline. |
| [pipeline_stages](https://developers.hubspot.com/docs/reference/api/crm/pipelines) | object_type, pipeline_id, stage_id | – | replace | Stage definitions for each pipeline. One row per stage. |

## Overriding associations

Each built-in table fetches a default set of associations. You can override that list by appending `:<assoc1>,<assoc2>` to the table name. The suffix **replaces** the default list, so you can narrow it down to just what you need, or include custom object names. Use `<table>:` (colon with empty list) to skip associations entirely.

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'contacts:companies,deals'   # only fetch companies and deals
  destination: postgres
```

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'contacts:'                  # fetch contacts with no associations
  destination: postgres
```

## Custom Objects

HubSpot allows you to create custom objects to store unique business data that's not covered by the standard objects. ingestr supports ingesting data from custom objects using the following format:

```plaintext
custom:<custom_object_name>
```

or with associations to other objects:

```plaintext
custom:<custom_object_name>:<associations>
```

### Parameters

- `custom_object_name`: The name of your custom object in HubSpot (can be either singular or plural form)
- `associations` (optional): Comma-separated list of object types to include as associations (e.g., `companies,deals,tickets,contacts`)

### Examples

Ingesting a custom object called "licenses":

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'custom:licenses'
  destination: postgres
```

Ingesting a custom object with associations to companies, deals, and contacts:

```yaml
parameters:
  source_connection: my-hubspot
  source_table: 'custom:licenses:companies,deals,contacts'
  destination: postgres
```

When you include associations, the response will contain information about the related objects, allowing you to track relationships between your custom objects and standard HubSpot objects.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/hubspot_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given HubSpot table into your Postgres database.

<img width="1124" alt="hubspot" src="https://github.com/user-attachments/assets/c88f2781-1e78-4d5b-8cb1-60b7993ea674">

# Customer.io
[Customer.io](https://customer.io/) is a customer engagement platform that enables businesses to send automated messages across email, push, SMS, and more.

Bruin supports Customer.io as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Customer.io into your data warehouse.

To set up a Customer.io connection, you need to have a Customer.io API key. For more information, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/customerio.html)

Follow the steps below to correctly set up Customer.io as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Customer.io, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      customerio:
        - name: "my_customerio"
          api_key: "YOUR_CUSTOMERIO_API_KEY"
          region: "us"
```
- `api_key`: The API key used for authentication with the Customer.io API.
- `region`: The region of your Customer.io account. Must be either `us` (default) or `eu`.

### Step 2: Create an asset file for data ingestion

To ingest data from Customer.io, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., customerio_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.customerio
type: ingestr
connection: postgres

parameters:
  source_connection: my_customerio
  source_table: 'broadcasts'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for Customer.io.
- `connection`: This is the destination connection.
- `source_connection`: The name of the Customer.io connection defined in .bruin.yml.
- `source_table`: The name of the data table in Customer.io you want to ingest. For example, `broadcasts` would ingest data related to broadcast campaigns.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
|-------|----|---------|--------------| ------- |
| activities | id | – | replace | Retrieves account activity log. |
| broadcasts | id | updated | merge | Retrieves broadcast campaigns. |
| broadcast_actions | id | updated | merge | Retrieves actions for broadcasts. |
| broadcast_action_metrics:period | broadcast_id, action_id, period, step_index | – | replace | Retrieves metrics for broadcast actions. Period: `hours`, `days`, `weeks`, `months`. |
| broadcast_messages | id | – | merge | Retrieves messages sent by broadcasts. |
| broadcast_metrics:period | broadcast_id, period, step_index | – | replace | Retrieves metrics for all broadcasts. Period: `hours`, `days`, `weeks`, `months`. |
| campaigns | id | updated | merge | Retrieves triggered campaigns. |
| campaign_actions | id | updated | merge | Retrieves actions for campaigns. |
| campaign_action_metrics:period | campaign_id, action_id, period, step_index | – | replace | Retrieves metrics for campaign actions. Period: `hours`, `days`, `weeks`, `months`. |
| campaign_messages | id | – | merge | Retrieves messages/deliveries sent from campaigns. |
| campaign_metrics:period | campaign_id, period, step_index | – | replace | Retrieves metrics for all campaigns. Period: `hours`, `days`, `weeks`, `months`. |
| collections | id | updated_at | merge | Retrieves data collections. |
| customers | cio_id | – | replace | Retrieves all customers/people in the workspace. |
| customer_activities | id | – | replace | Retrieves activities performed by each customer. |
| customer_attributes | customer_id | – | replace | Retrieves attributes for each customer. |
| customer_messages | id | – | merge | Retrieves messages sent to each customer. |
| customer_relationships | customer_id, object_type_id, object_id | – | replace | Retrieves object relationships for each customer. |
| exports | id | updated_at | merge | Retrieves export jobs. |
| info_ip_addresses | ip | – | replace | Retrieves IP addresses used by Customer.io. |
| messages | id | – | merge | Retrieves sent messages. |
| newsletters | id | updated | merge | Retrieves newsletters. |
| newsletter_metrics:period | newsletter_id, period, step_index | – | replace | Retrieves metrics for all newsletters. Period: `hours`, `days`, `weeks`, `months`. |
| newsletter_test_groups | id | – | replace | Retrieves test groups for newsletters. |
| object_types | id | – | replace | Retrieves object types in the workspace. |
| objects | object_type_id, object_id | – | replace | Retrieves all objects for each object type. |
| reporting_webhooks | id | – | replace | Retrieves reporting webhooks. |
| segments | id | updated_at | merge | Retrieves customer segments. |
| sender_identities | id | – | replace | Retrieves sender identities. |
| subscription_topics | id | – | replace | Retrieves subscription topics. |
| transactional_messages | id | – | replace | Retrieves transactional message templates. |
| workspaces | id | – | replace | Retrieves workspaces in your account. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run ingestr.customerio.asset.yml
```
As a result of this command, Bruin will ingest data from the given Customer.io table into your Postgres database.

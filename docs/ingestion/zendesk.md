# Zendesk
[Zendesk](https://www.zendesk.com/) is a cloud-based customer service and support platform. It offers a range of features including ticket management, self-service options, knowledgebase management, live chat, customer analytics, and conversations.

Bruin supports Zendesk as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Zendesk into your data warehouse.

In order to set up Zendesk connection, you need to add a configuration item to `connections` in the `.bruin.yml` file and in `asset` file. Depending on the data you are ingesting (source_table), you will need to use either `API Token authentication` or `OAuth Token authentication`. Choose the appropriate method based on your source table. For more details, please refer to the [Ingestr documentation](https://getbruin.com/docs/ingestr/supported-sources/zendesk.html)

Follow the steps below to correctly set up Zendesk as a data source and run ingestion.
### Step 1: Add a connection to .bruin.yml file

To connect to Zendesk, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

API Token Authentication:
```yaml
      connections:
          zendesk:
            - name: "my_zendesk",
              api_token: "xyzKey",
              email: "example.zendesk@gmail.com",
              sub_domain: "myCompany",
```

OAuth Token Authentication:
```yaml
  connections:
        zendesk:
          - name: "my_zendesk",
            oauth_token: "abcToken",
            sub_domain: "myCompany",
```

- `sub_domain`: the unique Zendesk subdomain that can be found in the account URL. For example, if your account URL is https://my_company.zendesk.com/, then `my_company` is your subdomain
- `email`: the email address of the user
- `api_token`: the API token used for authentication with Zendesk
- `oauth_token`: the OAuth token used for authentication with Zendesk

### Step 2: Create an asset file for data ingestion
To ingest data from Zendesk, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., zendesk_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.zendesk
type: ingestr
connection: postgres

parameters:
  source_connection: my_zendesk
  source_table: 'brands'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the Zendesk connection defined in .bruin.yml.
- `source_table`: The name of the data table in Zendesk that you want to ingest. Available tables:

Table    PK    Inc Key    Inc Strategy    Details
tickets    id    updated_at    merge    Retrieves all tickets, which are the means through which customers communicate with agents
ticket_metrics    -    –    replace    Retrieves various metrics about one or more tickets.
ticket_metric_events    id    time    append    Retrieves ticket metric events that occurred on or after the start time
ticket_forms    -    -    replace    Retrieves all ticket forms
users    -    –    replace    Retrieves all users
groups    -    –    replace    Retrieves groups of support agents
organizations    -    –    replace    Retrieves organizations
brands    -    –    replace    Retrieves all brands for your account
sla_policies    -    –    replace    Retrieves different SLA policies.
activities    -    –    replace    Retrieves ticket activities affecting the agent.
automations    -    –    replace    Retrieves the automations for the current account
targets    -    –    replace    Retrieves targets where as targets are data from Zendesk to external applications like Slack when a ticket is updated or created.
calls    id    updated_at    merge    Retrieves all calls specific to channels
addresses    -    –    replace    Retrieves addresses information
greetings    -    –    replace    Retrieves all default or customs greetings
phone_numbers    -    –    replace    Retrieves all available phone numbers.
settings    -    –    replace    Retrieves account settings related to Zendesk voice accounts
lines    -    –    replace    Retrieves all available lines, such as phone numbers and digital lines, in your Zendesk voice account.
agents_activity    -    –    replace    Retrieves activity information for agents
legs_incremental    id    updated_at    merge    Retrieves detailed information about each agent involved in a call.
chats    id    update_timestamp/updated_timestamp    merge    Retrieves available chats.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/zendesk_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given Zendesk table into your Postgres database.

<img width="1082" alt="zendesk" src="https://github.com/user-attachments/assets/b4cb54eb-dc05-4b6e-a113-e07316be9bff">

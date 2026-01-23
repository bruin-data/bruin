# Jira
[Jira](https://www.atlassian.com/software/jira) is a proprietary issue tracking product developed by Atlassian that allows bug tracking and agile project management.


Bruin supports Jira as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from Jira into your data warehouse.

In order to set up Jira connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up Jira as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to Jira, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      jira:
        - name: "my-jira"
          domain: "company.atlassian.net"
          email: "user@company.com"
          api_token: "YOUR_API_TOKEN"

```

* `domain`: is your Jira domain (e.g., company.atlassian.net).
* `email`: is the email address used for authentication with the Jira API.
* `api_token`: is the API token for authentication (required for Jira Cloud).

For details on how to obtain these credentials, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/jira.html#authentication).

### Step 2: Create an asset file for data ingestion

To ingest data from Jira, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., jira_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.jira
type: ingestr
connection: postgres

parameters:
  source_connection: my-jira
  source_table: 'issues'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for Jira.
- connection: This is the destination connection.
- source_connection: The name of the Jira connection defined in .bruin.yml.
- source_table: The name of the data table in Jira you want to ingest.

## Available Source Tables

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| `projects` | - | - | replace | Fetches all projects from your Jira instance. |
| `issues` | id | fields.updated | merge | Fetches all issues with support for incremental loading based on updated timestamp. |
| `users` | - | - | replace | Fetches users from your Jira instance. |
| `issue_types` | - | - | replace | Fetches all issue types configured in your Jira instance. |
| `statuses` | - | - | replace | Fetches all workflow statuses from your Jira instance. |
| `priorities` | - | - | replace | Fetches all issue priorities from your Jira instance. |
| `resolutions` | - | - | replace | Fetches all issue resolutions from your Jira instance. |
| `project_versions` | - | - | replace | Fetches versions for each project. |
| `project_components` | - | - | replace | Fetches components for each project. |
| `events` | - | - | replace | Fetches all issue events (created, updated, etc.) from your Jira instance. |
| `issue_changelogs` | - | - | replace | Fetches changelog history for all issues across all projects. |

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/jira_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given Jira table into your Postgres database.

## Incremental Loading

The `issues` table supports incremental loading based on the `updated` field. This means subsequent runs will only fetch issues that have been modified since the last run, making the data ingestion more efficient for large Jira instances.

> [!NOTE]
> Most tables use "replace" write disposition, meaning they will overwrite existing data on each run. Only the `issues` table supports incremental loading with "merge" disposition.
# SurveyMonkey

[SurveyMonkey](https://www.surveymonkey.com/) is an online survey platform that allows users to create surveys, collect responses, and analyze data.

Bruin supports SurveyMonkey as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from SurveyMonkey into your data platform.

To set up a SurveyMonkey connection, you need to add a configuration entry to your `.bruin.yml` file and create an asset file for data ingestion. You'll need an `access_token` to authenticate.

## Configuration

### Step 1: Add a connection to the .bruin.yml file

```yaml
connections:
  surveymonkey:
    - name: "my_surveymonkey"
      access_token: "your-access-token"
```

- `access_token` (required): The access token used to authenticate with the SurveyMonkey API.
- `region` (optional): The region. Must be `us` (default), `eu`, or `ca`.

For EU or CA accounts:

```yaml
connections:
  surveymonkey:
    - name: "my_surveymonkey_eu"
      access_token: "your-access-token"
      region: "eu"
```

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file (e.g., `surveymonkey.asset.yml`) inside the assets folder:

```yaml
name: public.surveymonkey
type: ingestr

parameters:
  source_connection: my_surveymonkey
  source_table: 'surveys'

  destination: postgres
```

- `source_connection`: The name of the SurveyMonkey connection defined in `.bruin.yml`.
- `source_table`: The table to ingest from SurveyMonkey (see available tables below).
- `destination`: The destination connection name.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/surveymonkey.asset.yml
```

Running this command ingests data from SurveyMonkey into your destination.

## Tables

SurveyMonkey source allows ingesting the following tables:

| Table | PK | Inc Key | Inc Strategy | Details |
| ----- | -- | ------- | ------------ | ------- |
| [surveys](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-get-surveys) | id | date_modified | merge | List of all surveys with metadata |
| [survey_details](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-get-surveys-id-details) | id | date_modified | merge | Full survey details with nested pages and questions as JSON |
| [survey_responses](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-survey-responses) | id | date_modified | merge | Survey response data with answers |
| [collectors](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-get-surveys-survey_id-collectors) | id | date_modified | merge | Survey distribution channels |
| [contact_lists](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-get-contact_lists) | id | - | replace | Contact lists |
| [contacts](https://api.surveymonkey.com/v3/docs?shell#api-endpoints-get-contacts) | id | - | replace | Contacts (all statuses: active, optout, bounced) |

Use these as the `source_table` parameter in the asset configuration.

## Getting an Access Token

1. Go to the [SurveyMonkey Developer Portal](https://developer.surveymonkey.com/apps/) and create a new app.
2. Select the app type:
   - **Private App** (recommended for Enterprise plans): Issues a non-expiring access token.
   - **Public App** (for Basic/free plans): Issues a draft token that expires after 90 days.
3. Under the Scopes section, enable: **View Surveys**, **View Responses**, **View Response Details**, **View Collectors**, **View Contacts**.
4. Click **Update Scopes**.
5. Copy the **Access Token** from the Credentials section.

> [!NOTE]
> For EU accounts, use the [EU Developer Portal](https://developer.eu.surveymonkey.com/apps/) and set `region: eu`. For CA accounts, use the [CA Developer Portal](https://developer.ca.surveymonkey.com/apps/) and set `region: ca`.

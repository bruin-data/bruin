# Salesforce
[Salesforce](https://www.Salesforce.com/) is a cloud-based customer relationship management (CRM) platform that helps businesses manage sales, customer interactions, and business processes. It provides tools for sales automation, customer service, marketing, analytics, and application development.

Bruin supports Salesforce as a source for [ingestr assets](/assets/ingestr), and you can use it to ingest data from Salesforce into your data platform.

To set up a Salesforce connection, you must add a configuration item in the `.bruin.yml` and `asset` file. You need `username`, `password` and `token`. Optionally, you can specify a `domain` for custom Salesforce domains. You can obtain your security token by logging into your Salesforce account and navigating to the user settings under "Reset My Security Token."

Follow the steps below to set up Salesforce correctly as a data source and run ingestion.
### Step 1: Add a connection to the .bruin.yml file
```yaml
connections:
      salesforce:
            - name: "salesforce"
              username: "user_123"
              password: "pass_123"
              token: "token_123"
              domain: "your-domain.my.salesforce.com"  # optional
```
- `username` is your Salesforce account username.
- `password` is your Salesforce account password.
- `token` is your Salesforce security token.
- `domain` is your custom Salesforce domain (optional). If not specified, login.salesforce.com will be used.

### Step 2: Create an asset file for data ingestion
To ingest data from Salesforce, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., salesforce_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.salesforce
type: ingestr
connection: postgres

parameters:
  source_connection: salesforce
  source_table: 'publisher-report'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the asset’s type. Set this to `ingestr` to use the ingestr data pipeline. For Salesforce, it will be always `ingestr`.
- `source_connection`: The name of the Salesforce connection defined in `.bruin.yml`.
- `source_table`: The name of the table in Salesforce to ingest.
- `destination`: The name of the destination connection.

## Available Source Tables

- `user`: Refers to an individual who has access to a Salesforce org or instance.
- `user_role`: A standard object that represents a role within the organization's hierarchy.
- `opportunity`: Represents a sales opportunity for a specific account or contact.
- `opportunity_line_item`: Represents individual line items or products associated with an Opportunity.
- `opportunity_contact_role`: Represents the association between an Opportunity and a Contact.
- `account`: Individual or organization that interacts with your business.
- `contact`: An individual person associated with an account or organization.
- `lead`: Prospective customer/individual/org. that has shown interest in a company's products/services.
- `campaign`: Marketing initiative or project designed to achieve specific goals.
- `campaign_member`: Association between a Contact or Lead and a Campaign.
- `product`: For managing and organizing your product-related data.
- `pricebook`: Used to manage product pricing and create price books.
- `pricebook_entry`: Represents a specific price for a product in a price book.
- `task`: Used to track and manage various activities and tasks.
- `event`: Used to track and manage calendar-based events.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/salesforce_asset.yml
```
As a result of this command, Bruin will ingest data from the given salesforce table into your Postgres database.

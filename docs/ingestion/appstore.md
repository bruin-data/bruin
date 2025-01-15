# Apple AppStore
The [AppStore](https://appstore.com/) is an app marketplace developed and maintained by Apple, for mobile apps on its iOS and iPadOS operating systems. The store allows users to browse and download approved apps developed within Apple's iOS SDK. Apps can be downloaded on the iPhone, iPod Touch, or iPad, and some can be transferred to the Apple Watch smartwatch or 4th-generation or newer Apple TVs as extensions of iPhone apps.

Bruin supports AppStore as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest analytics, sales and performance data into your data warehouse.

In order to set up AppStore connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file. 

Follow the steps below to correctly set up AppStore as a data source and run ingestion:

### Step 1: Add a connection to .bruin.yml file

To connect to AppStore, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      appstore:
        - name: "my-appstore"
          key_id: "my-key-id"
          issuer_id: "my-issuer-id"
          key_path: /path/to/private/key

          # optionally, you can specify the key as a 
          # base64 encoded string
          key_base64: "<base64 encoded private key>"
          
```

* `key_id`: ID of the key.
* `issuer_id`: Issuer ID of the key.
* `key_path`: Path to the private key.
* `key_base64`: base64 encoded contents of the private key.

For details on how to obtain these credentials, please refer [here](https://bruin-data.github.io/ingestr/supported-sources/appstore.html#setting-up-appstore-integration).

### Step 2: Create an asset file for data ingestion

To ingest data from AppStore, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., appstore_integration.asset.yml) inside the assets folder and add the following content:

```yaml
name: public.appstore
type: ingestr
connection: postgres

parameters:
  source_connection: my-appstore
  source_table: 'app-downloads-detailed:123'
  destination: postgres
```

- name: The name of the asset.
- type: Specifies the type of the asset. It will be always ingestr type for AppStore.
- connection: This is the destination connection.
- source_connection: The name of the AppStore connection defined in .bruin.yml.
- source_table: The name of the table in AppStore Connect API that you want to ingest. The table is of the form `<kind>:<app_id>[,<app_id>]`. Where `kind` is a report type and `<app_id>` is the ID of your app. Multiple App IDs can be specified by delimiting them with a comma.

### Step 3: [Run](/commands/run) asset to ingest data
```
bruin run assets/appstore_integration.asset.yml
```
As a result of this command, Bruin will ingest data from the given AppStore report into your Postgres database.
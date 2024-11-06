## Loading Notion Data to PostgreSQL Database Using Bruin
[Notion](https://www.notion.so) is an all-in-one workspace for note-taking, project management, and database management.

### Prerequisites
#### Install bruin

To install Bruin depending upon your machine, follow the installation instructions [here](https://github.com/bruin-data/bruin/blob/f799133136aba1b0eea673d731f236a4b5e78752/docs/getting-started/introduction/installation.md)

If you are on macOS, you can use brew to install Bruin:

```
brew install bruin-data/tap/bruin
```

#### Install the Bruin VSCode Extension
To install bruin vscode extension, follow the instruction [here](https://github.com/bruin-data/bruin/blob/f799133136aba1b0eea673d731f236a4b5e78752/docs/vscode-extension/installation.md)

#### Make sure You have [Notion credentials]() and Postgres credentials to connect.

### Getting Started
#### Create your first Bruin project

To create a bruin project basic structure you can just run


   ```
   bruin init {folder name} [template name]
   ```

If you don't define folder name and template name. It will create `default` template with folder name bruin-pipeline. This command will:

    Create a project named bruin-pipeline 
    Generate a folder called bruin-pipeline containing the following:
       An assets folder (where you add asset file)
      .bruin.yml file (where connection and credentials are added)
       pipeline.yml file to manage your pipeline.

#### Adding a new asset

Adding a new [asset](https://bruin-data.github.io/bruin/assets/ingestr.html) is as simple as creating a new file inside the assets folder. Let's create a new [ingestr asset](https://bruin-data.github.io/bruin/assets/ingestr.html) asset.ingetsr.notion.yml file and add :

 ```
 name: public.notion
type: ingestr
connection: my-postgres

parameters:
  source_connection: my-notion
  source_table: 'databaseId'

  destination: postgres
 ```

- name: The name of the asset.

- type: Specifies the type of the asset. It will be always ingestr type for Notion.

- connection: The name of the connection

- source_connection: The name of the Notion connection defined in .bruin.yml.
- source_table: The databaseId in Notion you want to ingest.
- destination: The name of the destination, you want to store. Here, we are using postgres sql


#### Adding connection and credentials in .bruin.yml
Using Bruin vscode extension
- Add destination connection - postgres sql
- Add source connection - Notion


You can customize your pipeline by adding in pipeline.yml
- You can change schedule to weekly or daily

#### Validate and Run your pipeline
- bruin CLI can run the whole pipeline or any task with the downstreams.


#### This is what Notion data looks like at the destination



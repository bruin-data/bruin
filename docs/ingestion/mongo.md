# MongoDB
MongoDB is a popular, open source NoSQL database known for its flexibility, scalability, and wide adoption in a variety of applications.

Bruin supports MongoDB as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from MongoDB into your data warehouse. 

In order to set up MongoDB connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

### Step 1: Add a connection to .bruin.yml file
To connect to MongoDB, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

Follow the steps below to correctly set up MongoDB as a data source and run ingestion.

```yaml
    connections:
      mongo:
        - name: "localMongo"
          username: "testUser"
          password: "testPass123"
          host: "localhost"
          port: 27018
          database: "testDB"
```
- `name`: The name to identify this MongoDB connection
- `username`: The MongoDB username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the MongoDB server (e.g., localhost or an IP address)
- `port`: The port number the database server is listening on (default: 27017)
- `database`:  The name of the database to connect to

### Step 2: Create an asset file for data ingestion

To ingest data from MongoDB, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., mongo_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.mongo
type: ingestr
connection: postgres

parameters:
  source_connection: localMongo
  source_table: 'users.details'

  destination: postgres
```
- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the MongoDB connection defined in .bruin.yml.
- `source_table`: The name of the data table in MongoDB that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/mongo_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given MongoDB table into your Postgres database.

<img width="1017" alt="image" src="https://github.com/user-attachments/assets/2bad9131-baa1-4f20-8e3d-eed4329e7f90">



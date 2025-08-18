# MySQL
 MySQL is a widely used relational database management system (RDBMS) that is free and open-source, making it ideal for both small and large applications.

Bruin supports MySQL as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from MySQL into your data warehouse.

In order to set up MySQL connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up MySQL as a data source and run ingestion.

### Step 1: Add a connection to .bruin.yml file
To connect to MySQL, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
     mysql:
        - name: "new-mysql"
          username: "test_user"
          password: "test_password"
          host: "localhost"
          port: 3306
          database: "mysql"
          ssl_ca_path: "path/to/ca.pem"
          ssl_cert_path: "path/to/cert.pem"
          ssl_key_path: "path/to/key.pem"
```
- `name`: The name to identify this MySQL connection
- `username`: The MySQL username with access to the database
- `password`: The password for the specified username
- `host`: The host address of the MySQL server (e.g., localhost or an IP address)
- `port`: The port number the database server is listening on (default: 3306)
- `database`:  The name of the database to connect to
- `driver`: (Optional) The name of the database driver to use
- `ssl_ca_path`: (Optional) The path to the CA certificate file
- `ssl_cert_path`: (Optional) The path to the client certificate file
- `ssl_key_path`: (Optional) The path to the client key file

### Step 2: Create an asset file for data ingestion
To ingest data from MySQL, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., mysql_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.mysql
type: ingestr
connection: postgres

parameters:
  source_connection: new-mysql
  source_table: 'mysql.user'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the MySQL connection defined in .bruin.yml.
- `source_table`: The name of the data table in MySQL that you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data
```     
bruin run assets/mysql_ingestion.yml
```
As a result of this command, Bruin will ingest data from the given MySQL table into your Postgres database.

<img width="1081" alt="mysql" src="https://github.com/user-attachments/assets/efd0666c-3c9b-40b3-bfa9-bf9ed05620d7">

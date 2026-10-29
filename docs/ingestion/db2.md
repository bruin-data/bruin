# IBM Db2

[IBM Db2](https://www.ibm.com/products/db2) is a high-performance, enterprise-grade relational database system designed for reliability, scalability, and transactional integrity.

Bruin supports DB2 as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from DB2 into your data warehouse.

In order to set up DB2 connection, you need to add a configuration item in the `.bruin.yml` file and in `asset` file.

Follow the steps below to correctly set up DB2 as a data source and run ingestion.

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to DB2, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
  db2:
    - name: "db2"
      username: "user_123"
      password: "pass_123"
      host: "localhost"
      port: 50000
      database: "testdb"
```

- `username`: The username to connect to the database
- `password`: The password for the user
- `host`: The host address of the database server
- `port`: The port number the database server is listening
- `database`: the name of the database to connect to
- `schema` (optional): the default schema to use for unqualified table names. Defaults to the username, uppercased.
- `ssl` (optional): set to `true` to connect over TLS.
- `timeout` (optional): connection/read timeout in seconds. Defaults to 30.
- `platform` (optional): forces which Db2 catalog dialect is used to read table metadata (`luw`, `zos`, or `ibmi`). ingestr auto-detects this from the server during the connection handshake, so it's only needed if that detection needs overriding.

Bruin connects to Db2 using ingestr's native implementation of the DRDA wire protocol — the same protocol all Db2 platforms speak — so this works against Db2 for Linux/Unix/Windows, Db2 for z/OS, and **Db2 for i (iSeries/AS400)** without any additional JDBC driver or JVM.

### Connecting to Db2 for i (iSeries/AS400)

The same connection type works for Db2 for i, which many ERP systems (including Infor Signature) store their data in:

- `database` must be the **relational database name** the IBM i system is registered under in its relational database directory (`WRKRDBDIRE` on the host), not an arbitrary label. This is often the system's own name.
- `source_table` should reference `LIBRARY.FILE`, where "library" is the IBM i equivalent of a schema and "file" is a physical file exposed as a table, e.g. `MYLIB.MYFILE`.
- The platform is auto-detected, so `platform: "ibmi"` is normally unnecessary. Set it explicitly only if detection picks the wrong catalog dialect.

### Step 2: Create an asset file for data ingestion

To ingest data from DB2, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., db2_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.db1
type: ingestr
connection: neon

parameters:
  source_connection: db2
  source_table: 'test.user'

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to ingestr to use the ingestr data pipeline.
- `connection`: This is the destination connection, which defines where the data should be stored. For example: `postgres` indicates that the ingested data will be stored in a Postgres database.
- `source_connection`: The name of the DB2 connection defined in .bruin.yml.
- `source_table`: The name of the data table in DB2 that you want to ingest.

#### Example: ingesting a Signature (Db2 for i) table

```yaml
name: public.ar_customers
type: ingestr
connection: neon

parameters:
  source_connection: signature
  source_table: 'MYLIB.MYFILE'

  destination: postgres
```

Here `signature` is a `db2` connection in `.bruin.yml` pointed at the IBM i LPAR that hosts Signature, and `MYLIB.MYFILE` is a library/file pair from the Signature data dictionary.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/db2_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given DB2 table into your Postgres database.

<img alt="applovinmax" src="./media/db2.png">

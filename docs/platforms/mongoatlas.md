# MongoDB Atlas

MongoDB Atlas is a fully-managed cloud database service built on MongoDB. It provides automated backups, monitoring, and scaling capabilities across major cloud providers.

Bruin supports MongoDB Atlas as a data platform for both ingestion sources and destinations.

> [!NOTE]
> MongoDB Atlas is supported as both a source and destination for ingestion using [Ingestr Assets](../assets/ingestr.md). It cannot be used for SQL-based transformations or other asset types, but you can run ad-hoc queries against it with [`bruin query`](../commands/query.md) and verify it with [`bruin connections test`](../commands/connections.md).

## Connection

To set up a MongoDB Atlas connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mongo_atlas:
        - name: "connection_name"
          username: "your-username"
          password: "your-password"
          host: "cluster0.example.mongodb.net"
```

**Parameters**:

- `name`: The name to identify this MongoDB Atlas connection in Bruin assets and pipeline defaults
- `username`: MongoDB Atlas database username
- `password`: MongoDB Atlas database password
- `host`: MongoDB Atlas cluster hostname, without the `mongodb+srv://` protocol (e.g., `cluster0.example.mongodb.net`)
- `database`: Optional. If set, Bruin appends it to the connection URI path. For ingestr assets, the target database is usually provided in `source_table` for source assets or in the asset `name` for destination assets, using `database.collection` format.

> [!NOTE]
> The connection uses the `mongodb+srv://` protocol, which is the standard for MongoDB Atlas connections. You don't need to specify the protocol or a port in the configuration.

Bruin turns this configuration into a MongoDB Atlas URI in the following form:

```text
mongodb+srv://your-username:your-password@cluster0.example.mongodb.net
```

If your username or password contains special characters, Bruin URL-encodes them when it builds the URI.

## Querying

You can run ad-hoc queries against a MongoDB Atlas connection with the [`query` command](../commands/query.md). Because MongoDB is not SQL, the query is a JSON object describing a find or aggregation against one collection:

```bash
bruin query --connection connection_name \
  --query '{"collection":"users","filter":{"age":{"$gt":21}},"sort":{"age":-1},"limit":10}'
```

See [Querying MongoDB](../commands/query.md#querying-mongodb) for the full envelope syntax.

## Using MongoDB Atlas as a Destination

MongoDB Atlas can be used as a destination for [Ingestr Assets](../assets/ingestr.md). This allows you to load data from various sources into your MongoDB Atlas cluster.

### Example: Load data from PostgreSQL to MongoDB Atlas

```yaml
name: mydb.users
type: ingestr
connection: connection_name

parameters:
  source_connection: postgres
  source_table: 'public.users'

  destination: mongo_atlas
```

This configuration will:

1. Extract data from the `public.users` table in PostgreSQL
2. Load the data into the `users` collection in the `mydb` MongoDB Atlas database

`connection` must match the `name` of a `mongo_atlas` connection in `.bruin.yml`. Bruin passes the asset `name` as ingestr's destination table, so use `database.collection` format in `name`.

## Using MongoDB Atlas as a Source

MongoDB Atlas can also be used as a source for [Ingestr Assets](../assets/ingestr.md). This allows you to ingest data from your MongoDB Atlas cluster into various destinations such as data warehouses.

### Example: Load data from MongoDB Atlas to PostgreSQL

```yaml
name: public.atlas_users
type: ingestr
connection: postgres

parameters:
  source_connection: connection_name
  source_table: 'users.details'

  destination: postgres
```

This configuration will:

1. Extract data from the `details` collection in the `users` MongoDB Atlas database
2. Load the data into the `public.atlas_users` table in PostgreSQL

`source_connection` must match the `name` of a `mongo_atlas` connection in `.bruin.yml`. `source_table` uses `database.collection` format.

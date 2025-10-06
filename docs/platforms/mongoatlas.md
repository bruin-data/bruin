# MongoDB Atlas

MongoDB Atlas is a fully-managed cloud database service built on MongoDB. It provides automated backups, monitoring, and scaling capabilities across major cloud providers.

Bruin supports MongoDB Atlas as a data platform for ingestion destinations.

> [!NOTE]
> MongoDB Atlas is only supported as a destination for ingestion using [Ingestr Assets](../assets/ingestr.md). It cannot be used for SQL-based transformations or other asset types.

## Connection

To set up a MongoDB Atlas connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mongo_atlas:
        - name: "connection_name"
          username: "your-username"
          password: "your-password"
          host: "cluster0.example.mongodb.net"
          database: "your-database"
```

**Parameters**:
- `username`: MongoDB Atlas database username
- `password`: MongoDB Atlas database password
- `host`: MongoDB Atlas cluster hostname (e.g., `cluster0.example.mongodb.net`)
- `database`: The database name to connect to

> [!NOTE]
> The connection uses the `mongodb+srv://` protocol which is the standard for MongoDB Atlas connections. You don't need to specify the protocol in the configuration.

## Using MongoDB Atlas as a Destination

MongoDB Atlas can be used as a destination for [Ingestr Assets](../assets/ingestr.md). This allows you to load data from various sources into your MongoDB Atlas cluster.

### Example: Load data from PostgreSQL to MongoDB Atlas

```yaml
name: ingest.users
type: ingestr
connection: postgres

parameters:
  source_connection: postgres
  source_table: 'public.users'

  destination: mongo_atlas
  destination_connection: mongo_atlas_connection
  destination_table: 'users'
```

This configuration will:
1. Extract data from the `public.users` table in PostgreSQL
2. Load the data into the `users` collection in your MongoDB Atlas database

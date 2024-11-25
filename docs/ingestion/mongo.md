# Mongo DB

In order to have set up a Mongo DB connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mongo:
        - name: "connection_name"
          username: "mongo_user"
          password: "XXXXXXXXXX"
          host: "mongo-db-shard.somedomain.com"
          port: 27017
          database: "dev"
```

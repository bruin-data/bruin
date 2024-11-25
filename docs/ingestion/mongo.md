# Mongo DB

In order to have set up a Mongo DB connection, you need to add a configuration item to `connections` in the `.bruin.yml` file complying with the following schema.

```yaml
    connections:
      mongo:
        - name: "localMongo"
          username: "kollywoodUser"
          password: "password123"
          host: "localhost"
          port: 27018
          database: "kollywood"
```

```yaml
name: public.mongo
type: ingestr
connection: postgres

parameters:
  source_connection: localMongo
  source_table: 'kollywood.Horror'

  destination: postgres
```

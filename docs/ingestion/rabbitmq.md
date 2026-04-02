# RabbitMQ

[RabbitMQ](https://www.rabbitmq.com/) is an open-source message broker that implements the Advanced Message Queuing Protocol (AMQP). It is widely used for building distributed systems, microservices communication, and asynchronous task processing.

Bruin supports RabbitMQ as a source for [Ingestr assets](/assets/ingestr), and you can use it to ingest data from RabbitMQ into your data warehouse.

In order to set up a RabbitMQ connection, you need to add a configuration item to connections in the .bruin.yml file, following the schema below. The required fields include `host`, `username`, and `password`. The `port`, `vhost`, and `tls` fields are optional. For more information on RabbitMQ, please refer [here](https://getbruin.com/docs/ingestr/supported-sources/rabbitmq.html)

Follow the steps below to correctly set up RabbitMQ as a data source and run ingestion:

## Configuration

### Step 1: Add a connection to .bruin.yml file

To connect to RabbitMQ, you need to add a configuration item to the connections section of the `.bruin.yml` file. This configuration must comply with the following schema:

```yaml
    connections:
      rabbitmq:
        - name: "my_rabbitmq"
          host: "localhost"
          port: "5672"
          username: "guest"
          password: "guest"
          # vhost: "/"    # optional, defaults to /
          # tls: true     # optional, defaults to false
```

- `host`: The RabbitMQ server hostname.
- `port`: The AMQP port, defaults to `5672` (or `5671` with TLS).
- `username`: The username for authentication.
- `password`: The password for authentication.
- `vhost`: The virtual host to connect to, defaults to `/`.
- `tls`: Set to `true` for TLS-encrypted connections (`amqps://`), defaults to `false`.

### Step 2: Create an asset file for data ingestion

To ingest data from RabbitMQ, you need to create an [asset configuration](/assets/ingestr#asset-structure) file. This file defines the data flow from the source to the destination. Create a YAML file (e.g., rabbitmq_ingestion.yml) inside the assets folder and add the following content:

```yaml
name: public.rabbitmq
type: ingestr
connection: postgres

parameters:
  source_connection: my_rabbitmq
  source_table: 'rabbitmq.my_queue'
  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. It will be always ingestr type for RabbitMQ.
- `connection`: This is the destination connection.
- `source_connection`: The name of the RabbitMQ connection defined in .bruin.yml.
- `source_table`: The name of the queue in RabbitMQ you want to ingest.

### Step 3: [Run](/commands/run) asset to ingest data

```bash
bruin run assets/rabbitmq_ingestion.yml
```

As a result of this command, Bruin will ingest data from the given RabbitMQ queue into your Postgres database.

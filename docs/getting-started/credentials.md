# Credentials

Bruin allows you to store all of your credentials in a non-versioned file called `.bruin.yml`. This file is not checked into your version control system and is not shared with your team.

The `.bruin.yml` file contains a list of credentials for each connection type, as well as among different environments.

Here's an example of a `.bruin.yml` file:

```yaml
default_environment: default
environments:
  default:
    connections:
      generic:
        - name: MY_SECRET
          value: secretvalue
      duckdb:
        - name: "duckdb-default"
          path: "chess.db"
      chess:
        - name: "chess-default"
          players:
            - "MagnusCarlsen"
            - "Hikaru"

  another_environment:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
```

When you run a pipeline, Bruin will find this file in the repo root, parse the connections there, build client objects internally to interact with these external platforms and then run your assets.

> [!INFO]
> The first time you run `bruin validate` or `bruin run`, Bruin will create an empty `.bruin.yml` file and add it to `.gitignore` automatically.

## Credential Types
Bruin supports many different types of credentials for each platform. Please visit the corresponding platform page from the sidebar to see the supported credential types.

## Generic Credentials
Generic credentials are key-value pairs that can be used to inject secrets into your assets from outside.

```yaml
default_environment: default
environments:
  default:
    connections:
      generic:
        - name: MY_SECRET
          value: someothersecret
```

Common use cases for generic credentials are API keys, passwords, etc.

## Environment Variables

Bruin supports injecting environment variables into your credentials in case you have the secrets defined elsewhere as well.

```yaml
default_environment: default
environments:
  default:
    connections:
      postgres:
        - name: my_postgres_connection
          username: ${POSTGRES_USERNAME}
          password: ${POSTGRES_PASSWORD}
          host: ${POSTGRES_HOST}
          port: ${POSTGRES_PORT}
          database: ${POSTGRES_DATABASE}
```

> [!INFO]
> Environment variables are not expanded in the `.bruin.yml` file. They are expanded when Bruin runs your assets.

## Custom Credentials File
Bruin looks for a `.bruin.yml` file in the project root by default; however, in some cases you might want to override the value per project.

In order to do that, you can simply use the `--config-file` flag in many commands, or you can use `BRUIN_CONFIG_FILE` environment variable.
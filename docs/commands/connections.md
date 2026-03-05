# Connections

Bruin has various commands to handle connections via its CLI.

Bruin CLI offers convenience methods to manage connections when using `.bruin.yml` as our [secrets backend](../secrets/overview.md).

## List Connections

To list all the connections in the `.bruin.yml` file, run the following command:

```bash
bruin connections list
```

The output will look like this:

```plaintext
Environment: default
+---------+-----------+
| TYPE    | NAME      |
+---------+-----------+
| generic | MY_SECRET |
| gorgias | my_conn   |
+---------+-----------+

Environment: someother
+---------+-----------+
| TYPE    | NAME      |
+---------+-----------+
| generic | MY_SECRET |
+---------+-----------+
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |

## Add Connection

### Interactive mode

When run without flags in a terminal, `bruin connections add` launches an interactive wizard that walks you through:

1. Selecting an environment
2. Entering a connection name
3. Choosing a connection type (with search/filter)
4. Filling in the credential fields for that type

```bash
bruin connections add
```

### Flag-based mode

You can also provide all parameters via flags for scripting and CI/CD usage:

```bash
bruin connections add --env someother --type generic --name MY_SECRET --credentials '{"value": "someothersecret"}'
```

This will add the connection to the `.bruin.yml` file and the connection will be available in the given environment.

The parameter after `--credentials` is the value of the connection in JSON format, as you would write it in the `.bruin.yml` file. For further reference, you can check the [Connections](/core-concepts/connections) documentation.

> [!INFO]
> When using flags, all four of `--env`, `--name`, `--type`, and `--credentials` must be provided together. Providing only some of them will result in an error.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--environment`, `-e`, `--env` | str | - | The name of the environment to add the connection to. |
| `--name` | str | - | The name of the connection. |
| `--type` | str | - | The type of the connection. |
| `--credentials` | str | - | The JSON object containing the credentials. |
| `--config-file` | str | - | The path to the .bruin.yml file. |

### Example: a GCP connection

```bash
bruin connections add \
    --env default \
    --type google_cloud_platform \
    --name MY_GCP_CONNECTION \
    --credentials '{"project_id": "my-gcp-project", "service_account_file": "path/to/service/account/file.json"}'
```

### Example: a generic secret

```bash
bruin connections add \
    --env staging \
    --type generic \
    --name MY_SECRET \
    --credentials '{"value": "secret-password"}'
```

## Delete Connection

To delete a connection from a specific environment, use the following command:

```bash
bruin connections delete --env staging --name MY_SECRET
```

You can define a different path for the repo with an extra argument if you'd like:

```bash
bruin connections delete --env staging --name MY_SECRET <path-to-repo>
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |

### Example

Delete a connection named "my-connection" from the "production" environment:

```bash
bruin connections delete -e staging -n test-connection -o json
```

## Test Connection

To test if a connection is valid, you can use the test command.
This command runs a simple validation check for the connection.

```bash
bruin connections test --name <connection-name> [--env <environment>]
```

If no environment flag (`--env`) is provided, the default environment from our `.bruin.yml` will be used.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the `.bruin.yml` file. |

### Examples

Test a connection in the default environment:

```bash
bruin connections test --name my-bigquery-conn
```

Test a connection in a specific environment:

```bash
bruin connections test --name my-snowflake-conn --env production
```

You can also get the output in JSON format:

```bash
bruin connections test --name my-postgres-conn --env staging --output json
```

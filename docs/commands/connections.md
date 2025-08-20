# Connections

Bruin has various commands to handle connections via its CLI. 

Bruin CLI offers convenience methods to manage connections when using `.bruin.yml` as our [secrets backend](../secrets/overview.md).

## List Connections

To list all the connections in the `.bruin.yml` file, run the following command:

```bash
bruin connections list
```

The output will look like this:
```
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

To add a connection to a specific environment, run the following command:

```bash
bruin connections add --env someother --type generic --name MY_SECRET --credentials '{"value": "someothersecret"}'
```

This will add the connection to the `.bruin.yml` file and the connection will be available in the given environment.

The parameter after `--credentials` is the value of the connection in JSON format, as you would write it in the `.bruin.yml` file. For further reference, you can check the [Connections section](../getting-started/concepts.md#connection) of the documentation.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the .bruin.yml file. |



> [!INFO]
> This command is meant to be used programmatically rather than human beings, since the `credentials` parameter is in JSON format.

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
```
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

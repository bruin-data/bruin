# `environments` Command

The `environments` command allows you to manage environments defined in the `.bruin.yml` configuration file.
It supports listing all available environments in the current Git repository and creating new ones.

### Usage
```bash
bruin environments [subcommand]
```

## `list` Subcommand

Displays the environments defined in the `.bruin.yml` configuration file in the current Git repository. The environments can be displayed in plain text or JSON format.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config-file` | str | - | The path to the `.bruin.yml` file. |

## Usage

```bash
bruin environments list [flags]
```

## `create` Subcommand

Creates a new environment entry in the `.bruin.yml` configuration file.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--name` | str | - | Name of the environment to create. |
| `--schema-prefix` | str | - | Optional schema prefix to use for the environment. |
| `--config-file` | str | - | The path to the `.bruin.yml` file. |

### Usage

```bash
bruin environments create --name dev [--schema-prefix my_prefix]
```

## `clone` Subcommand

Creates a copy of an existing environment with a new name. All connections from the source environment are copied to the target environment. Optionally allows setting or overriding the schema prefix for the cloned environment.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--source, -s` | str | Default environment | Name of the environment to clone from. |
| `--target, -t` | str | - | **Required.** Name of the new environment. |
| `--schema-prefix, -p` | str | - | Optional schema prefix for the cloned environment. If not provided, uses the source environment's schema prefix. |
| `--output, -o` | str | plain | Output format: `plain` or `json`. |
| `--config-file` | str | - | The path to the `.bruin.yml` file. |

### Usage

```bash
# Clone the default environment to a new environment
bruin environments clone --target staging

# Clone a specific environment to a new environment
bruin environments clone --source production --target staging

# Clone with a custom schema prefix
bruin environments clone --target dev --schema-prefix dev_

# Clone with JSON output
bruin environments clone --target test --output json
```

### Examples

Clone the default environment:
```bash
bruin environments clone --target staging
```

Clone a specific environment with a schema prefix:
```bash
bruin environments clone --source production --target dev --schema-prefix dev_
```

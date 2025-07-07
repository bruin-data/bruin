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

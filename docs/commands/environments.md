# `environments` Command

The `environments` command allows you to manage environments defined in the `.bruin.yml` configuration file. 
It currently supports listing all available environments in the current Git repository.

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

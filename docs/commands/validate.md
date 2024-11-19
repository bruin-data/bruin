# `validate` Command

The `validate` command checks the Bruin pipeline configurations for all pipelines in a specified directory or validates a single asset. 
It ensures that the configurations meet specific requirements and are properly configured for the selected environment.

## Usage

```bash
bruin validate [path to pipelines] [flags]
```
**Arguments:**

**[path to pipelines]:** Path to the pipeline directory or a specific asset.
Defaults to the current directory (".") if not provided.

**Flags:**

| Flag                     | Alias     | Description                                                                 |
|--------------------------|-----------|-----------------------------------------------------------------------------|
| `--environment`          | `-e, --env` | Specifies the environment to use for validation.                            |
| `--force`                | `-f`       | Forces validation even if the environment is a production environment.      |
| `--output [format]`      | `-o`       | Specifies the output type, possible values: `plain`, `json`.                |
| `--exclude-warnings`     |            | Excludes warnings from the validation output.                               |


## Examples

**1. Validate all pipelines in the current directory:**

```bash
bruin validate
```

**2. Validate pipelines in a specified directory:**

```bash
bruin validate
```


**3. Validate with JSON output:**

```bash
bruin validate --output json

```


**4. Validate a specific asset:**

```bash
bruin validate path/to/specific-asset

```
# `validate` Command

The `validate` command checks the Bruin pipeline configurations for all pipelines in a specified directory or validates a single asset. 
It ensures that the configurations meet specific requirements and are properly configured for the selected environment.

## Usage

```bash
bruin validate [path to pipelines] [flags]
```
<img alt="Bruin - validate" src="/validate.gif" style="margin: 10px;" />

**Arguments:**

- **[path to pipelines]:** Path to the pipeline directory or a specific asset.
Defaults to the current directory (".") if not provided.

**Flags:**

| Flag                     | Alias     | Description                                                                 |
|--------------------------|-----------|-----------------------------------------------------------------------------|
| `--environment`          | `-e, --env` | Specifies the environment to use for validation.                            |
| `--force`                | `-f`       | Forces validation even if the environment is a production environment.      |
| `--output [format]`      | `-o`       | Specifies the output type, possible values: `plain`, `json`.                |
| `--exclude-warnings`     |            | Excludes warnings from the validation output.                               |


### Dry-run Validation
One of the beneficial features of the `validate` command is the ability to perform a dry-run validation on the destination data platform. This means, effectively Bruin runs a dry-run version of the query to ensure that the query is valid and can be executed on the destination data platform. This gives a very strong peace of mind in terms of the accuracy of the queries from a syntactical and semantical perspective, and also ensures that the query can be executed on the destination data platform.

Dry-run is automatically enabled for BigQuery and Snowflake.

However, there are also scenarios where dry-run is not the best suited tool:
- Dry-run requires all the tables/views to be there, which means if you are running validation on a table that you haven't created yet, it'll fail.
- Due to the same reason, dry-run will also fail if you are adding a new column to a table and its upstream, but you haven't created them in the destination yet.

In the end, it is better to treat dry-run as an extra check, and accept that it might give false negatives from time to time.

## Examples

**1. Validate all pipelines in the current directory:**

```bash
bruin validate
```

**2. Validate with JSON output:**

```bash
bruin validate --output json

```


**3. Validate a specific asset:**

```bash
bruin validate path/to/specific-asset

```
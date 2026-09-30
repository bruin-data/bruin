# `semantic` Command

The `semantic` command validates the models in your repository's [semantic layer](/core-concepts/semantic-layer) and runs the [quality checks](/core-concepts/semantic-layer#quality-checks) defined on them.

Semantic models live in the `semantic` directory next to `.bruin.yml`. Both subcommands load every model in that directory, so joins and model checks can reference other models.

## `semantic validate`

Validates every semantic model and its quality check definitions. If a model has a connection, Bruin also validates it against the warehouse.

```bash
bruin semantic validate [path to semantic directory] [flags]
```

Validation happens in two steps:

1. **Structural validation** always runs. It checks each model against the schema, and checks metric references, joins, window definitions, and check definitions, such as unknown check names or missing values.
2. **Warehouse validation** runs when Bruin can find a connection for the model. Bruin builds a separate query for each dimension, time granularity, metric, joined dimension, segment, and check, and validates each one against the warehouse:
   - On BigQuery, Snowflake, Postgres, and DuckDB, Bruin uses the platform's dry run or `EXPLAIN`. This resolves tables and columns without reading any data.
   - On other platforms, Bruin runs each query with a `WHERE 1 = 0` condition. The warehouse still plans the query, but it returns no rows.

Bruin takes the connection from the `--connection` flag, or from `source.connection` in the model file if the flag is not set:

- A model without a connection only gets structural validation, and Bruin reports a warning.
- If `source.connection` names a connection that doesn't exist in the selected environment, Bruin also reports a warning. This lets validation pass in environments without credentials, such as CI.
- If the connection passed to `--connection` doesn't exist, the command fails.

**Flags:**

| Flag            | Alias         | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| `--environment` | `-e`, `--env` | Environment to use. Its `schema_prefix` is applied to the model tables.     |
| `--config-file` |               | Path to the `.bruin.yml` file. Defaults to the one at the repository root.  |
| `--connection`  | `-c`          | Connection to use for every model. Overrides `source.connection`.           |
| `--model`       | `-m`          | Only validate this model. Can be repeated.                                  |
| `--output`      | `-o`          | Output format: `plain` or `json`.                                           |

**Examples:**

```bash
# validate every model, using the connections set in the model files
bruin semantic validate

# validate one model against a specific connection
bruin semantic validate --model orders --connection warehouse

# JSON output, for CI
bruin semantic validate --output json
```

The command exits with code 1 if any model is invalid.

## `semantic check`

Runs the quality checks defined on semantic models and reports the result of each one.

```bash
bruin semantic check [path to semantic directory] [flags]
```

Every model that has checks needs a connection, so pass `--connection` or set `source.connection` in the model. Bruin processes models one at a time and runs each model's checks in parallel.

The command exits with code 1 if any check fails or errors, if a model's checks cannot run (for example, because the model has no connection), or if the selected models define no checks at all.

`checks` and `run-checks` are aliases for `check`.

**Flags:**

| Flag            | Alias         | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| `--environment` | `-e`, `--env` | Environment to use. Its `schema_prefix` is applied to the model tables.     |
| `--config-file` |               | Path to the `.bruin.yml` file. Defaults to the one at the repository root.  |
| `--connection`  | `-c`          | Connection to use for every model. Overrides `source.connection`.           |
| `--model`       | `-m`          | Only run this model's checks. Can be repeated.                              |
| `--output`      | `-o`          | Output format: `plain` or `json`.                                           |

**Examples:**

```bash
# run every semantic quality check
bruin semantic check

# run one model's checks in the prod environment
bruin semantic check --model orders --env prod

# JSON output, including each check's compiled SQL and message
bruin semantic check --output json
```

Plain output groups the results by model:

```text
Running semantic quality checks in '/repo/semantic'...

orders
  ✓ dimension order_id: not_null (12ms)
  ✓ dimension order_id: unique (9ms)
  ✘ metric revenue: positive (8ms)
      └── metric 'revenue' is -40, expected a positive value
  ✓ check completed_revenue (10ms)

✘ 3 checks passed, 1 check failed, please check above.
```

JSON output has these top-level fields:

- `path`: the semantic directory that was checked.
- `results`: one entry per check, with its `id`, `model`, `scope`, `target`, `name`, `description`, compiled `sql`, `status` (`passed`, `failed`, or `error`), `message`, and `duration_ms`.
- `summary`: the number of checks with each status.
- `errors`: models whose checks could not run, with the reason. Omitted when there are none.

# Row-level inference example

The runnable two-ticket pipeline in this directory uses a structured classification column with Zen's paid `muse-spark-1.3` model. It uses a named Bruin OpenCode connection and can incur API charges. It does **not** require TypeSafe credentials.

The classification asset uses `input_asset: raw.tickets`: Bruin adds the dependency, reads all source columns, and merges the Arrow result into a separate DuckDB table.

Add these connections to a local `.bruin.yml` (use a disposable path). The API key shown is a placeholder, not a working key:

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: inference-duckdb
          path: /tmp/bruin-inference-example.duckdb
      opencode:
        - name: inference-opencode
          api_key: "<your-opencode-api-key>"
```

The pipeline maps its `opencode` default to `inference-opencode`, so no credential environment variable is needed. Then run:

```shell
bruin run examples/inference
bruin run examples/inference/assets/classifications.asset.yml
```

The second run should report two cached results and zero model calls. Changing one ticket body causes only that row to be inferred again. The Zen free-tier model has rejected direct API use, so this example uses a paid model. Do not replace synthetic tickets with private data without checking provider data-use terms.

See the [inference documentation](../../docs/assets/inference.md) for multi-column and multi-provider examples, provider constraints, typed schemas, caching, materialization, and retries.

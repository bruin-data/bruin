# Row-level inference example

This pipeline classifies two synthetic support tickets using Zen's paid `muse-spark-1.3` model and materializes them into DuckDB. It requires `OPENCODE_API_KEY` and incurs API usage charges.

The classification asset uses `input_asset: raw.tickets`: Bruin adds the dependency and reads all source columns automatically. Results are handed to ingestr as Arrow and merged into a separate table.

Add this connection to your local `.bruin.yml` (use a disposable database path):

```yaml
default_environment: default
environments:
  default:
    connections:
      duckdb:
        - name: inference-duckdb
          path: /tmp/bruin-inference-example.duckdb
```

Provide `OPENCODE_API_KEY` through your environment, then run:

```shell
bruin run examples/inference
bruin run examples/inference/assets/classifications.asset.yml
```

The second run should report two cached results and zero model calls. Changing one ticket's body causes only that row to be inferred again. To test this, edit `tickets.sql` and run the pipeline again.

Live verification returned `billing` for ticket 1 and `technical` for ticket 7. An unchanged rerun made zero model calls. Changing one ticket's body made one new call and updated its category through `merge`.

**Zen free-tier limitation:** `muse-spark-1.3-contributor-free` rejected direct requests both with and without an API key with “OpenCode's free tier can only be used in OpenCode.” The example therefore uses the non-free model.

For OpenRouter, change `provider` to `openrouter`, set an accessible OpenRouter `model`, and provide `OPENROUTER_API_KEY`. OpenRouter requests can incur charges.

Direct `openai`, `anthropic`, and `google` providers are also supported. Set their model ID and `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `GOOGLE_API_KEY`, respectively.

Contributor models may use prompts and responses for training. Do not replace the synthetic examples with private data without approval.

See [inference documentation](../../docs/assets/inference.md) for materialization, cache, retry, and first-version limitations.

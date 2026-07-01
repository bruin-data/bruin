# `unit-test` Command

`bruin unit-test` runs an asset's [unit tests](/quality/unit-tests) against its configured connection. Each test mocks the tables the query reads, runs one read-only `SELECT`, and compares the output to what the test expects — it never writes to your warehouse.

For how to write unit tests (the `unit_tests:` block, inputs, shared fixtures, CTE assertions, and freezing time), see the [Unit Tests](/quality/unit-tests) guide. This page covers the command itself.

## Usage

```bash
bruin unit-test [FLAGS] [path]
```

The path is optional and behaves like `bruin validate`:

- **No path or a directory** runs the unit tests of every pipeline found beneath it. With no path it defaults to the current directory, so running `bruin unit-test` from a repo root tests the whole repo — convenient in CI.
- **A single asset file** runs only that asset's `unit_tests:`.

The command exits non-zero if any test fails.

## Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--environment` | str | - | Environment whose connections to use, from `.bruin.yml`. |
| `--var` | str | - | Override a pipeline variable for the run. Same syntax as `bruin run`'s `--var`; repeatable. |
| `--start-date` | str | today | Start date passed to the asset's Jinja render. |
| `--end-date` | str | today | End date passed to the asset's Jinja render. |

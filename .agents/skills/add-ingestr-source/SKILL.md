---
name: add-ingestr-source
description: Add Bruin CLI support for a new ingestr source. Use when a task asks to implement a new ingestr connection/source type, wire that source into Bruin ingestr assets, update available source tables, add ingestion docs or example assets, regenerate connection schema expectations, or address review feedback for a newly added ingestr source.
---

# Add Ingestr Source

## Workflow

1. Read the ingestr docs first: https://getbruin.com/docs/ingestr/. Then read the upstream source-specific docs or PR. Capture the source URI scheme, required URI parameters, optional URI parameters, supported `source_table` values, incremental behavior, primary keys, and any file/table hints.
2. Inspect similar Bruin sources before editing. Prefer an existing package with the same shape: API/table source, OAuth/API-key source, database-like source, or file/path source.
3. Implement the connection config and URI builder.
4. Wire the connection into config loading, connection manager registration, and ingestr asset source discovery.
5. Add source-table registry entries, docs, example assets, tests, and regenerated expectations.
6. Run the checks required by the repo instructions before committing.

## Implementation Checklist

Add or update these files for a new source named `<source>`:

- `pkg/<source>/config.go`: the `Config` struct and `GetIngestrURI()`.
- `pkg/<source>/db.go`: the `Client` and `NewClient`.
- `pkg/config/connections.go`: add `<Source>Connection` with YAML/JSON/mapstructure tags and `GetName()`.
- `pkg/config/manager.go`: add the connection slice to `Connections`, then update `AddConnection`, `DeleteConnection`, and `MergeFrom`.
- `pkg/connection/connection.go`: import the package, add a `Manager` map, add `Add<Source>ConnectionFromConfig`, store it in `availableConnections` and `AllConnectionDetails`, and process it in `NewManagerFromConfigWithContext`.
- `pkg/pipeline/pipeline.go`: add a `defaultMapping` entry `"<source>": "<source>-default"`.
- `pkg/ingestr/sources.go`: add `SourceTablesRegistry["<source>"]`.
- `docs/ingestion/<source>.md`: document `.bruin.yml` config, ingestr asset YAML, source tables, options, incremental behavior, and example assets.
- `docs/.vitepress/config.mjs`: add the docs sidebar entry.
- `pkg/config/manager_test.go`: cover the new connection in `AddConnection`/`DeleteConnection`/`MergeFrom`.
- `pkg/config/testdata/simple.yml`: add a sample connection block.
- `pkg/config/testdata/simple_win.yml`: add the same block (keep in sync with `simple.yml`).
- `integration-tests/expectations/expected_connections_schema.json`: regenerate after the connection schema changes.

Add **every** required and optional parameter from the source URI — do not stop at the common ones. Take the authoritative list from the ingestr source code (the URI/DSN parser), not just the docs, and match each param's name and type exactly.

For sources without fixed tables, add representative `source_table` formats instead of pretending the source has enumerated tables. File/path sources usually need examples for exact paths, globs, and format hints.

## URI Builder Pattern

Build ingestr URIs with `net/url.Values`; do not concatenate unescaped query parameters.

Validate required fields in deterministic order using a slice, not a map. This keeps user-facing errors stable and makes tests precise.

```go
type requiredField struct {
    key   string
    value string
}

requiredFields := []requiredField{
    {"tenant_id", c.TenantID},
    {"client_id", c.ClientID},
}

for _, field := range requiredFields {
    field.value = strings.TrimSpace(field.value)
    if field.value == "" {
        return "", fmt.Errorf("%s: %s must be provided", sourceName, field.key)
    }
    params.Set(field.key, field.value)
}
```

Use pointer fields for optional numeric values where zero is meaningful, such as `max_files: 0`. Reject negative limits before adding them to the URI.

## Tests

Add focused tests for:

- URI encoding, optional parameters, zero-valued pointer options, missing required fields, and invalid limits in `pkg/<source>`.
- `Add<Source>ConnectionFromConfig` registering the client and connection details.
- `Config.AddConnection`, `Config.DeleteConnection`, and `Connections.MergeFrom`.
- `GetSourceTables("<source>")` when source-table examples are added.

Regenerate the connection schema expectation after adding a connection type:

```bash
go run -tags="no_duckdb_arrow" . internal connections | jq . > integration-tests/expectations/expected_connections_schema.json
```

Inspect the generated diff. Ordering-only movement can happen, but keep only intentional changes.

## Docs

Base docs on ingestr's source documentation, not guesses. Include:

- `.bruin.yml` connection config with required and optional fields.
- One minimal ingestr asset.
- Several realistic example assets when `source_table` syntax has variants.
- A table of available fixed tables, or available `source_table` formats for path/query-based sources.
- Primary keys, metadata columns, and whether extraction is incremental or full-refresh.

Keep examples executable-looking but generic. Use the same destination pattern as nearby ingestion docs unless the source needs something different.

## Version Caveat

Check the pinned ingestr version before bumping it, usually in `pkg/python/uv.go`. If the upstream source exists only in an unreleased ingestr PR or commit, wire Bruin support but do not bump to a nonexistent release. Mention the release dependency in the final note or PR when relevant.

## Validation

During iteration, run focused package tests first. Before finishing app-code changes, follow the repo instructions exactly: run `make format` and `make test`.

For docs-only follow-ups, use the smallest relevant validation such as `git diff --check`, Markdown inspection, or docs build if the repo has a fast docs check.

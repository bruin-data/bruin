# 1Password

::: warning Experimental
Reading connection settings from 1Password is experimental and may change.
:::

A connection in [`.bruin.yml`](./bruinyml) can read some or all of its settings from a [1Password](https://1password.com/) item, so secrets don't have to be written in the file. Unlike the [`--secrets-backend`](./overview) options, this works per connection: everything else in `.bruin.yml` stays as it is.

## Requirements

Bruin reads items with the [1Password CLI](https://developer.1password.com/docs/cli/get-started/) (`op`), so `op` must be installed and signed in. The simplest way is to turn on **Settings → Developer → Integrate with 1Password CLI** in the 1Password desktop app; 1Password then asks you to approve access when Bruin first reads an item. Any other way `op` can sign in, such as a service account token, works too.

## Linking a connection to an item

Add `bruin_external_vault` to the connection and remove the settings that are now in the item:

```yaml
environments:
  default:
    connections:
      postgres:
        - name: analytics_pg
          database: analytics
          bruin_external_vault: op://Employee/analytics-postgres
```

The link is `op://<vault>/<item>`, using the vault and item names as they appear in 1Password, or their IDs. Each connection in each environment can point to its own item.

## How item fields are matched

Each field of the item whose label is a setting name is used for that setting, for example a field labelled `password` sets `password`.

- Labels are matched ignoring case, and spaces or dashes count as underscores: `Password` and `pool max conns` both match.
- 1Password's built-in labels `server` and `hostname` set `host`, so a 1Password **Database** item works as it is.
- Fields that match no setting, such as notes, are ignored.
- A setting can't be both in `.bruin.yml` and in the item, and two fields can't set the same setting. Bruin reports either case as an error, without showing the values.
- Relative file paths read from the item, such as a DuckDB `path`, are resolved against the folder of `.bruin.yml`, like paths written in the file.

## When Bruin contacts 1Password

Bruin reads an item only when a command opens that connection, for example when running an asset that uses it or testing it with `bruin connections test`. Commands that don't open connections, such as `render`, `validate`, `lineage` and `connections list`, never contact 1Password. Each item is read at most once per command.

Values read from 1Password are never written back to `.bruin.yml`: commands that save the file, such as `bruin connections add`, keep the link as it is.

## Settings to keep in `.bruin.yml`

Some settings are read straight from `.bruin.yml` without opening the connection, so keep them in the file:

- `database`, `catalog` and `project_id`, used by column lineage
- `max_concurrent_assets`
- `use_cloud`
- `query_results_path` for Athena, and `cluster` for ClickHouse

Values in the item are used as they are: `${ENV_VAR}` references in them are not expanded.

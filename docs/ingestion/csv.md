# Local CSV Files

Bruin supports local CSV files via [Ingestr assets](/assets/ingestr). You can load a CSV file into a data warehouse, or export data from another source into a local CSV file.

ingestr uses CSV URIs in the form `csv://path/to/file.csv`. Absolute paths use three slashes, for example `csv:///tmp/file.csv`. ingestr automatically handles UTF-8 and UTF-16 files with byte order marks; for other encodings, set the `encoding` query parameter through the Bruin connection.

## Reading data from a local CSV file

### Step 1: Add a connection to `.bruin.yml`

```yaml
connections:
  csv:
    - name: local-csv
      path: ./data/customers.csv
```

- `path`: The local CSV file path. Relative paths are resolved from the working directory where Bruin runs.
- `encoding`: Optional. Use this when the CSV is not UTF-8 or UTF-16 with a byte order mark, for example `windows-1252` or `latin1`.

For example, to read a Windows-1252 encoded file:

```yaml
connections:
  csv:
    - name: local-csv
      path: /tmp/customers.csv
      encoding: windows-1252
```

### Step 2: Create an ingestr asset

```yaml
name: raw.customers
type: ingestr
connection: postgres

parameters:
  source_connection: local-csv
  source_table: sample
  destination: postgres
```

- `source_connection`: The name of the `csv` connection from `.bruin.yml`.
- `source_table`: The table name ingestr assigns to the CSV source. Use `sample` unless you need a specific source table label.
- `destination`: The destination platform.

### Step 3: Run the asset

```bash
bruin run assets/customers.asset.yml
```

## File type hints

For local CSV connections, the `csv://` URI already selects the CSV reader. If the same pattern is used with another file connector or a file-like path that needs an explicit format hint, set `file_type: csv` in the asset parameters:

```yaml
name: raw.customers
type: ingestr
connection: postgres

parameters:
  source_connection: local-csv
  source_table: sample
  file_type: csv
  destination: postgres
```

Bruin passes this to ingestr as a source-table suffix.

## Writing data to a local CSV file

Configure the `csv` connection as the destination connection and set the output path:

```yaml
connections:
  csv:
    - name: csv-export
      path: ./exports/customers.csv
```

Then create an ingestr asset that writes to that connection:

```yaml
name: raw.customers
type: ingestr
connection: csv-export

parameters:
  source_connection: postgres
  source_table: public.customers
  destination: csv
```

If you need destination layout control, set `layout` on the connection:

```yaml
connections:
  csv:
    - name: csv-export
      path: ./exports
      layout: "{table_name}.{ext}"
```

The CSV URI behavior follows the current [ingestr CSV documentation](https://getbruin.com/docs/ingestr/supported-sources/csv.html).

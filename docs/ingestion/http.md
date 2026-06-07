# HTTP

Bruin supports public HTTP and HTTPS file URLs as a source for [Ingestr assets](/assets/ingestr). You can use this source to ingest publicly accessible CSV, JSON, JSON Lines, and Parquet files into your data warehouse.

HTTP sources do not support authentication, custom headers, or cookies. The file must be accessible directly from the configured URL.

## Configuration

### Step 1: Add a connection to .bruin.yml file

Add a lowercase `http` connection to the connections section of your `.bruin.yml` file:

```yaml
connections:
  http:
    - name: http
      url: "https://example.com/path/to/file.csv"
```

- `name`: The name to identify this HTTP connection.
- `url`: A public `http://` or `https://` URL for the source file.

### Step 2: Create an asset file for data ingestion

Create an [asset configuration](/assets/ingestr#asset-structure) file, for example `assets/http_ingestion.yml`:

```yaml
name: public.http_file
type: ingestr
connection: postgres

parameters:
  source_connection: http
  source_table: "data"

  destination: postgres
```

- `name`: The name of the asset.
- `type`: Specifies the type of the asset. Set this to `ingestr`.
- `connection`: The destination connection where the data will be stored.
- `source_connection`: The name of the HTTP connection defined in `.bruin.yml`.
- `source_table`: A required source table name. Use a format suffix here when the URL extension does not identify the file type.
- `destination`: The destination type.

### Step 3: Run asset to ingest data

```bash
bruin run assets/http_ingestion.yml
```

## Supported file formats

The HTTP source supports these public file formats:

| Format | URL extension or source table suffix |
|--------|--------------------------------------|
| CSV with headers | `.csv` or `#csv` |
| CSV without headers | `#csv_headless` |
| JSON | `.json` or `#json` |
| JSON Lines | `.jsonl` or `#jsonl` |
| Parquet | `.parquet` or `#parquet` |

If the URL does not have a recognizable extension, add the format suffix to `source_table`:

```yaml
name: public.http_export
type: ingestr
connection: postgres

parameters:
  source_connection: http
  source_table: "data#jsonl"

  destination: postgres
```

For CSV files without headers, use `#csv_headless`:

```yaml
name: public.http_csv_headless
type: ingestr
connection: postgres

parameters:
  source_connection: http
  source_table: "data#csv_headless"

  destination: postgres
```

## Notes

- Only public URLs are supported.
- Incremental loading is not supported for HTTP sources.
- The source file is downloaded before processing, so use a storage-specific source such as S3 or GCS for very large files.

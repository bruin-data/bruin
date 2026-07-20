# SharePoint

[SharePoint Online](https://www.microsoft.com/microsoft-365/sharepoint/collaboration) is Microsoft's document management platform.

Bruin supports SharePoint as a source for [Ingestr assets](/assets/ingestr). You can ingest Excel and CSV files from a SharePoint Online document library into a destination supported by ingestr.

## Configuration

### Step 1: Add a connection to `.bruin.yml`

```yaml
connections:
  sharepoint:
    - name: my-sharepoint
      tenant_id: your-tenant-id
      client_id: your-client-id
      client_secret: your-client-secret
      hostname: example.sharepoint.com
      site: sites/Example
      library: Documents # optional
      max_file_size: 104857600 # optional, bytes
      max_files: 10000 # optional, use 0 for unlimited
      download_timeout: 30m # optional, Go duration
```

- `tenant_id`: Azure AD tenant ID for the app registration.
- `client_id`: app registration client ID.
- `client_secret`: app registration client secret.
- `hostname`: SharePoint tenant hostname, such as `example.sharepoint.com`.
- `site`: server-relative site path, such as `sites/Example`.
- `library`: optional document library name. If omitted, ingestr uses the site's default Documents library.
- `max_file_size`: optional maximum bytes for a single downloaded file. Use `0` for unlimited.
- `max_files`: optional maximum number of files a glob may match. Defaults to `10000`; use `0` for unlimited.
- `download_timeout`: optional per-request HTTP timeout as a Go duration, such as `30m` or `600s`. It defaults to `10m`; raise it for large files on slow connections.

Authentication uses the OAuth2 client-credentials flow. The app registration needs application permission to read the site's files, such as `Sites.Read.All` or `Files.Read.All`, granted with admin consent.

### Step 2: Create an ingestr asset

```yaml
name: raw.sharepoint_budget
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/budget.xlsx#sheet=North,date_cols=Date"
  destination: duckdb
```

- `source_connection`: the name of the SharePoint connection defined in `.bruin.yml`.
- `source_table`: the file path in the document library, optionally followed by ingestr hints such as `sheet`, `sheets`, `skip`, `drop_empty`, `date_cols`, `xlsx`, or `csv`.

### Example assets

These examples use DuckDB as the destination. Replace `connection` with the destination connection name and `destination` with the destination platform/type you want to load into.

Single Excel sheet:

```yaml
name: raw.sharepoint_products
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/products.xlsx#xlsx,sheet=Sheet1"
  destination: duckdb
```

Excel sheet with skipped rows and date conversion:

```yaml
name: raw.sharepoint_forecast
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/parameters.xlsx#sheet=Forecast,skip=4,date_cols=Date|Month"
  destination: duckdb
```

Multiple sheets from one workbook:

```yaml
name: raw.sharepoint_regional_data
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/regional data.xlsx#sheets=North|South|East|West"
  destination: duckdb
```

Multiple sheets from every workbook matched by a glob:

```yaml
name: raw.sharepoint_monthly
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/monthly/*.xlsx#sheets=Jan|Feb|Mar"
  destination: duckdb
```

CSV with explicit encoding and tab separator:

```yaml
name: raw.sharepoint_export
type: ingestr
connection: duckdb

parameters:
  source_connection: my-sharepoint
  source_table: "Reports/export.csv#csv,encoding=utf-16le,sep=tab"
  destination: duckdb
```

### Available `source_table` formats

SharePoint does not expose fixed table names. The `source_table` value is a file path, or a glob of file paths, relative to the configured document library root.

| Format | Example | Description |
| --- | --- | --- |
| `<path/to/file.xlsx>` | `Reports/products.xlsx` | Reads the first sheet from an Excel workbook. |
| `<path/to/file.xlsx>#sheet=<sheet_name>` | `Reports/products.xlsx#sheet=Sheet1` | Reads a single named Excel sheet. |
| `<path/to/file.xlsx>#sheets=<a>\|<b>` | `Reports/budget.xlsx#sheets=North\|South` | Reads and stacks multiple Excel sheets, unioning columns by name. |
| `<path/to/files/*.xlsx>#sheets=<a>\|<b>` | `Reports/monthly/*.xlsx#sheets=Jan\|Feb\|Mar` | Reads every matching workbook and stacks the requested sheets. |
| `<path/to/file.csv>` | `Exports/customers.csv` | Reads a CSV file using default CSV parsing. |
| `<path/to/file.csv>#csv,encoding=<enc>,sep=<sep>` | `Exports/customers.csv#csv,encoding=utf-16le,sep=tab` | Reads a CSV file with explicit format, encoding, or separator hints. |
| `<path/to/files/**>` | `Reports/**` | Reads all matching files recursively, detecting `.xlsx` or `.csv` per file. |

Paths can include spaces, `&`, and glob wildcards: `*`, `**`, `?`, `[...]`, and `{a,b}`. If a file path contains a literal `#`, encode it as `%23`.

### Source table hints

Hints are appended after `#` and separated with commas. A bare token is a format or behavior flag; `key=value` sets a named option.

| Hint | Applies to | Description |
| --- | --- | --- |
| `xlsx` or `csv` | all | Overrides the file format when it cannot be inferred from the extension. |
| `sheet=<name>` | Excel | Reads a single named sheet. |
| `sheets=<a>\|<b>` | Excel | Reads and stacks multiple sheets separated by `\|`. |
| `skip=<n>` | all | Skips `n` rows before reading the header row. |
| `drop_empty` | all | Skips rows where every data column is empty. |
| `date_cols=<a>\|<b>` | Excel | Converts the named Excel serial date columns to ISO date strings. |
| `raw` | Excel | Reads raw values. This is the default. |
| `formatted` | Excel | Reads displayed/formatted cell text instead of raw values. |
| `encoding=<enc>` | CSV | Sets input encoding, such as `utf-16le`. |
| `sep=<sep>` | CSV | Sets field separator. Use `tab` or `\t` for tab-delimited files. |

Every row includes metadata columns:

| Column | Description |
| --- | --- |
| `_source_file` | The document-library path of the file the row came from. |
| `_sheet_name` | The Excel sheet name. This is null for CSV files. |
| `_row_idx` | Zero-based row position in the source sheet/file after `skip` and the header row. |

SharePoint extraction is not incremental. The source defaults to `replace`; `append`, `merge`, and `delete+insert` can be selected, but each run still reads the full file or glob.

### Step 3: Run the asset

```bash
bruin run assets/sharepoint_ingestion.yml
```

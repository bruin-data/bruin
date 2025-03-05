# `render` Command

The `render` command processes a Bruin SQL asset and generates a SQL query or materialized output for execution. 
It supports multiple databases and output formats, making it a flexible tool for rendering assets in Bruin pipelines.

## Usage

```bash
bruin render [path to asset definition] [flags]
```
<img alt="Bruin - clean" src="/render.gif" style="margin: 10px;" />
### Arguments

**path-to-asset-definition* (required):
- The file path to the Bruin SQL asset you want to render.


### Flags

| Flag               | Alias | Description                                                           |
|--------------------|-------|-----------------------------------------------------------------------|
| `--full-refresh`   | `-r`  | Truncate the table before running the query.                          |
| `--start-date`     |       | Specify the start date in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` format.|
| `--end-date`       |       | Specify the end date in `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS` format. |
| `--output [format]`| `-o`  | Specify the output format (e.g., `json`). Defaults to console output.  |
| `--config-file`    |       | The path to the `.bruin.yml` file. |


### Examples

**Render an Asset with Default Settings :**

```bash
bruin render path/to/asset.yml
```
**Render an Asset with a Date Range:**
```bash
bruin render path/to/asset.yml --start-date 2024-01-01 --end-date 2024-01-31
```
**Render an Asset in JSON Format:**
```bash
bruin render path/to/asset.yml --output json
```

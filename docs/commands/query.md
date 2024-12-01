# Fetch Command 

The `query` command executes and retrieves the results of a query on a specified connection and 
returns the results on the terminal in either table format or as a JSON object.

**Flags:**

| Flag                 | Alias | Description                                                                 |
|----------------------|-------|-----------------------------------------------------------------------------|
| `--connection`       | `-c`  |  The name of the connection to use (required).                            |
| `--query`            | `-q`  | The SQL query to execute (required).     |
| `--output [format]`  | `-o`  | Specifies the output type, possible values: `plain`, `json`.                |


### Example

```bash
query --connection my_connection --query "SELECT * FROM table"
```
**Example output:**
```plaintext
+-------------+-------------+----------------+
|   Column1   |   Column2   |    Column3     |
+-------------+-------------+----------------+
| Value1      | Value2      | Value3         |
| Value4      | Value5      | Value6         |
| Value7      | Value8      | Value9         |
+-------------+-------------+----------------+
```

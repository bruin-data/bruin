# Fetch Command 

The `fetch query` command executes and retrieves the results of a query on a specified connection and 
returns the results on the terminal in either table format or as a JSON object.

### Flags

- **`--connection`** : The name of the connection to use (required).
- **`--query`**: The SQL query to execute (required).
- **`--output`** : Output format (`json` for JSON; leave empty for table).

### Example

```bash
fetch query --connection my_connection --query "SELECT * FROM table"
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

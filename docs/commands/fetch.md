# Fetch Command 

The `fetch query` command executes and retrieves the results of a query on a specified connection.

### Flags

- **`--connection`** : The name of the connection to use (required).
- **`--query`**: The SQL query to execute (required).
- **`--output`** : Output format (`json` for JSON; leave empty for table).

### Example

```bash
fetch query --connection my_connection --query "SELECT * FROM table"

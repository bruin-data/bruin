#ifndef POLYGLOT_FFI_H
#define POLYGLOT_FFI_H

#ifdef __cplusplus
extern "C" {
#endif

// Free a string previously returned by any polyglot_* function.
void polyglot_free_string(char *ptr);

// Extract table names from SQL. Returns JSON: {"tables":["..."], "error":"..."}
char *polyglot_get_tables(const char *sql, const char *dialect);

// Rename tables in SQL. Input is JSON: {"query":"...", "dialect":"...", "table_mapping":{...}}
// Returns JSON: {"query":"...", "error":"..."}
char *polyglot_rename_tables(const char *request_json);

// Add LIMIT to SQL. Input is JSON: {"query":"...", "limit":N, "dialect":"..."}
// Returns JSON: {"query":"...", "error":"..."}
char *polyglot_add_limit(const char *request_json);

// Check if SQL is a single SELECT query.
// Returns JSON: {"is_single_select":bool, "error":"..."}
char *polyglot_is_single_select(const char *sql, const char *dialect);

// Extract column lineage from SQL.
// Input is JSON: {"query":"...", "dialect":"...", "schema":{...}}
// Returns JSON: {"columns":[...], "non_selected_columns":[...], "errors":[...]}
char *polyglot_column_lineage(const char *request_json);

#ifdef __cplusplus
}
#endif

#endif // POLYGLOT_FFI_H

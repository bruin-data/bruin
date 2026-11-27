package sail

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/tablename"
)

// Queryer runs a query and returns its rows.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// RelationExists reports whether a table or view named tableName exists, so an
// incremental asset can create its table on the first run.
func (c *Client) RelationExists(ctx context.Context, tableName string) (bool, error) {
	capability, ok := tablename.For("sail")
	if !ok {
		return false, errors.New("sail table-name capability not found")
	}
	if err := capability.CheckName(tableName); err != nil {
		return false, err
	}

	return SparkSQLRelationExists(ctx, c.connection, strings.Split(tableName, "."))
}

// SparkSQLRelationExists reports whether a table or view exists under the
// identifier parts, using the catalog commands that Spark and Sail share.
//
// The materializers write the name as backtick-quoted parts, which the engine
// resolves against the session's current catalog and namespace. SHOW TABLES IN
// the same parts resolves them the same way, and lists views and temporary views
// as well as tables. Names are compared case-insensitively, because whether they
// are case-sensitive depends on the catalog.
//
// SHOW TABLES fails when the namespace does not exist. The table is only
// reported missing then if listing the parent's namespaces confirms that the
// namespace is gone, or if listing the tables again succeeds without it, so
// that any other failure stays an error.
func SparkSQLRelationExists(ctx context.Context, conn Queryer, parts []string) (bool, error) {
	if len(parts) == 0 {
		return false, errors.New("table name cannot be empty")
	}
	table := parts[len(parts)-1]
	namespace := parts[:len(parts)-1]

	showTables := "SHOW TABLES"
	if len(namespace) > 0 {
		showTables += " IN " + quoteSparkSQLIdentifier(namespace)
	}
	tables, err := queryStrings(ctx, conn, showTables, "tableName")
	if err == nil {
		return containsFold(tables, table), nil
	}
	err = fmt.Errorf("failed to check whether table '%s' exists: %w", strings.Join(parts, "."), err)
	if len(namespace) == 0 {
		return false, err
	}

	parent := namespace[:len(namespace)-1]
	showNamespaces := "SHOW NAMESPACES"
	if len(parent) > 0 {
		showNamespaces += " IN " + quoteSparkSQLIdentifier(parent)
	}
	namespaces, nsErr := queryStrings(ctx, conn, showNamespaces, "")
	if nsErr != nil {
		return false, err
	}
	// Nested namespaces are listed with their full, possibly quoted, path, so
	// only their last part is compared.
	for i, listed := range namespaces {
		listed = strings.ReplaceAll(listed, "`", "")
		namespaces[i] = listed[strings.LastIndex(listed, ".")+1:]
	}
	if !containsFold(namespaces, namespace[len(namespace)-1]) {
		return false, nil
	}

	// The namespace exists: either SHOW TABLES failed for another reason, or the
	// namespace was created in between, for example by another asset of the same
	// run. Listing its tables again tells the two apart.
	tables, retryErr := queryStrings(ctx, conn, showTables, "tableName")
	if retryErr != nil {
		return false, err
	}

	return containsFold(tables, table), nil
}

// queryStrings returns one column of a catalog command's result: the column
// named column, or the table name column of SHOW TABLES output when there is no
// such column, or the first column when column is empty.
func queryStrings(ctx context.Context, conn Queryer, query, column string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s returned no columns", query)
	}

	index := 0
	if column != "" {
		// SHOW TABLES returns (namespace, tableName, isTemporary).
		if len(columns) > 1 {
			index = 1
		}
		for i, name := range columns {
			if strings.EqualFold(name, column) {
				index = i
				break
			}
		}
	}

	values := make([]string, 0)
	row := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range row {
		pointers[i] = &row[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		switch value := row[index].(type) {
		case nil:
		case string:
			values = append(values, value)
		case []byte:
			values = append(values, string(value))
		default:
			values = append(values, fmt.Sprint(value))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return values, nil
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}

func quoteSparkSQLIdentifier(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = "`" + strings.ReplaceAll(part, "`", "``") + "`"
	}
	return strings.Join(quoted, ".")
}

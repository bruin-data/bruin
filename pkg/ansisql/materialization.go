package ansisql

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// BuildTruncateInsertQuery creates a truncate+insert query that works for standard ANSI SQL databases.
// This can be used by platforms that support standard TRUNCATE TABLE syntax with transactions.
func BuildTruncateInsertQuery(task *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"BEGIN TRANSACTION",
		"TRUNCATE TABLE " + task.Name,
		fmt.Sprintf("INSERT INTO %s %s", task.Name, strings.TrimSuffix(query, ";")),
		"COMMIT",
	}

	return strings.Join(queries, ";\n") + ";", nil
}

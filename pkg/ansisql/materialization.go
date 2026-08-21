package ansisql

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func GetColumnsWithMergeLogic(asset *pipeline.Asset) []pipeline.Column {
	var columns []pipeline.Column
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			continue
		}
		if col.MergeSQL != "" || col.UpdateOnMerge {
			columns = append(columns, col)
		}
	}
	return columns
}

// AddIncrementalPredicate appends the user-provided SQL predicate to a merge
// match condition. The predicate is intentionally treated as plain SQL so it
// can use database-specific syntax and the target/source aliases exposed by
// materializers.
func AddIncrementalPredicate(conditions []string, predicate string) []string {
	predicate = strings.TrimSpace(predicate)
	if predicate == "" {
		return conditions
	}

	return append(conditions, "("+predicate+")")
}

// BuildCreateTableIfNotExistsAsQuery builds an empty CTAS statement that
// bootstraps a table without consuming the rows returned by the asset query.
// Materializers can then run their normal incremental DML on both the first
// and subsequent executions.
func BuildCreateTableIfNotExistsAsQuery(tableName, tableOptions, query string) string {
	query = strings.TrimSuffix(strings.TrimSpace(query), ";")
	tableOptions = strings.TrimSpace(tableOptions)
	if tableOptions != "" {
		tableOptions = " " + tableOptions
	}

	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s%s AS\nSELECT * FROM (\n%s\n) AS __bruin_bootstrap WHERE 1 = 0",
		tableName,
		tableOptions,
		query,
	)
}

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

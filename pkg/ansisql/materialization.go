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

// BuildIncrementalDelete builds the DELETE of a delete+insert. With an
// incremental_predicate, the target is aliased as `target` and the predicate
// is ANDed onto the condition; without one, the statement is unchanged.
func BuildIncrementalDelete(table, condition, predicate string) string {
	if strings.TrimSpace(predicate) == "" {
		return fmt.Sprintf("DELETE FROM %s WHERE %s", table, condition)
	}

	conditions := AddIncrementalPredicate([]string{condition}, predicate)
	return fmt.Sprintf("DELETE FROM %s AS target WHERE %s", table, strings.Join(conditions, " AND "))
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

package pipeline

import "github.com/bruin-data/bruin/pkg/sqlparser"

type Parser interface {
	ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error)
}

type LineageExtractor struct {
	// parser Parser
}

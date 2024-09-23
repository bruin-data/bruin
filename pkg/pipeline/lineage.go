package pipeline

import "github.com/bruin-data/bruin/pkg/sqlparser"

type Parser interface {
	ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error)
}

type LineageExtractor struct {
	parser Parser
}

//
//func (l *LineageExtractor) Extract(pipeline *Pipeline, asset *Asset) (*sqlparser.Lineage, error) {
//	sql := asset.Query
//	schema := asset.Schema
//	dialect := asset.Dialect
//
//	return l.parser.ColumnLineage(sql, dialect, schema)
//}

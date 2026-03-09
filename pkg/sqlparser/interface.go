package sqlparser

import (
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

type Parser interface {
	Start() error
	ColumnLineage(sql, dialect string, schema Schema) (*Lineage, error)
	UsedTables(sql, dialect string) ([]string, error)
	RenameTables(sql, dialect string, tableMapping map[string]string) (string, error)
	AddLimit(sql string, limit int, dialect string) (string, error)
	IsSingleSelectQuery(sql string, dialect string) (bool, error)
	GetMissingDependenciesForAsset(asset *pipeline.Asset, pipeline *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error)
	Close() error
}

var _ Parser = (*SQLParser)(nil)
var _ Parser = (*RustSQLParser)(nil)

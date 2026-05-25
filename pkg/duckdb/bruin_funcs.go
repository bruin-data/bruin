package duck

import (
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformDuckDB, jinja.MergeBuiltinOverrides(jinja.SplitPartURLHelpers("varchar"), map[string]any{
		"deduplicate":     ansisql.DeduplicateQualify,
		"date_spine":      jinja.DuckDBDateSpine,
		"generate_series": jinja.DuckDBGenerateSeries,
	}))
}

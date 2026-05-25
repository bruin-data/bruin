package postgres

import (
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformPostgres, jinja.MergeBuiltinOverrides(jinja.SplitPartURLHelpers("varchar"), map[string]any{
		"deduplicate": ansisql.DeduplicateDistinctOn,
		"date_spine":  jinja.PostgresDateSpine,
	}))
}

package mssql

import (
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformMSSQL, jinja.MergeBuiltinOverrides(jinja.TSQLURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWith("varchar", ansisql.HashBytesHashFn),
		"deduplicate":            ansisql.DeduplicateSubquery,
		"pivot":                  jinja.PivotWithIdentifierQuote(jinja.BracketQuoteIdentifier),
		"width_bucket":           jinja.TSQLWidthBucket,
		"date_spine":             jinja.TSQLDateSpine,
	}))
}

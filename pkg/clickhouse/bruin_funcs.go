package clickhouse

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformClickhouse, jinja.MergeBuiltinOverrides(jinja.ClickHouseURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWithCoalesceExpr(func(field string) string {
			return fmt.Sprintf("coalesce(toString(%s), '%s')", field, jinja.SurrogateKeyNullValue())
		}, jinja.ConcatFunction, func(expr string) string {
			return fmt.Sprintf("lower(hex(MD5(%s)))", expr)
		}),
		"deduplicate":  ansisql.DeduplicateQualify,
		"pivot":        jinja.PivotWithIdentifierQuote(jinja.BacktickQuoteIdentifier),
		"width_bucket": jinja.ClickHouseWidthBucket,
		"date_spine":   jinja.ClickHouseDateSpine,
	}))
}

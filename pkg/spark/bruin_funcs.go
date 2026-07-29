package spark

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformSpark, jinja.MergeBuiltinOverrides(jinja.SparkURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWith("string", func(expr string) string {
			return fmt.Sprintf("md5(%s)", expr)
		}),
		"deduplicate": ansisql.DeduplicateQualify,
		"pivot":       jinja.PivotWithIdentifierQuote(jinja.BacktickQuoteIdentifier),
		"date_spine":  jinja.SparkDateSpine,
	}))
}

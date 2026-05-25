package mysql

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformMySQL, jinja.MergeBuiltinOverrides(jinja.MySQLURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWith("char", func(expr string) string {
			return fmt.Sprintf("md5(%s)", expr)
		}),
		"pivot":      jinja.PivotWithIdentifierQuote(jinja.BacktickQuoteIdentifier),
		"date_spine": jinja.MySQLDateSpine,
	}))
}

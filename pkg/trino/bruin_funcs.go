package trino

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformTrino, jinja.MergeBuiltinOverrides(jinja.PrestoURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWithConcat("varchar", jinja.ConcatOperator, func(expr string) string {
			return fmt.Sprintf("to_hex(md5(to_utf8(%s)))", expr)
		}),
		"deduplicate":  ansisql.DeduplicateArrayAgg,
		"width_bucket": jinja.NativeWidthBucket,
		"date_spine":   jinja.PrestoDateSpine,
	}))
}

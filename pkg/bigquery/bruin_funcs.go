package bigquery

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformBigQuery, jinja.MergeBuiltinOverrides(jinja.BigQueryURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWith("string", func(expr string) string {
			return fmt.Sprintf("to_hex(md5(%s))", expr)
		}),
		"deduplicate": ansisql.DeduplicateQualify,
		"haversine_distance": jinja.HaversineDistanceWithRadians(func(expr string) string {
			return fmt.Sprintf("(%s) * acos(-1) / 180", expr)
		}),
		"pivot":        jinja.PivotWithIdentifierQuote(jinja.BigQueryQuoteIdentifier),
		"width_bucket": jinja.BigQueryWidthBucket,
		"date_spine":   jinja.BigQueryDateSpine,
	}))
}

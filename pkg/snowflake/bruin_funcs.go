package snowflake

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformSnowflake, jinja.MergeBuiltinOverrides(jinja.SplitPartURLHelpers("varchar"), map[string]any{
		"deduplicate":  ansisql.DeduplicateQualify,
		"width_bucket": jinja.NativeWidthBucket,
		"date_spine": func(datepart, startDate, endDate string) string {
			castFn := "to_date"
			if jinja.IsTimestampDatepart(datepart) {
				castFn = "to_timestamp"
			}
			return jinja.DateSpineWithRecursiveDateAdd(true, "", func(datepart, n, start string) string {
				return fmt.Sprintf("dateadd(%s, %s, %s)", datepart, n, start)
			})(datepart, fmt.Sprintf("%s(%s)", castFn, startDate), fmt.Sprintf("%s(%s)", castFn, endDate))
		},
	}))
}

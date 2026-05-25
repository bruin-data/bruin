package redshift

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformRedshift, jinja.MergeBuiltinOverrides(jinja.SplitPartURLHelpers("varchar"), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWithConcat("varchar", jinja.ConcatOperator, func(expr string) string {
			return fmt.Sprintf("md5(%s)", expr)
		}),
		"deduplicate": ansisql.DeduplicateQualify,
		"date_spine": func(datepart, startDate, endDate string) string {
			return jinja.DateSpineWithRecursiveDateAdd(true, "", func(datepart, n, start string) string {
				return fmt.Sprintf("dateadd(%s, %s, %s)", datepart, n, start)
			})(datepart, fmt.Sprintf("cast(%s as timestamp)", startDate), fmt.Sprintf("cast(%s as timestamp)", endDate))
		},
	}))
}

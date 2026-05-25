package vertica

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformVertica, jinja.MergeBuiltinOverrides(jinja.SplitPartURLHelpers("varchar"), map[string]any{
		"date_spine": func(datepart, startDate, endDate string) string {
			return jinja.DateSpineWithRecursiveDateAdd(true, "", func(datepart, n, start string) string {
				return fmt.Sprintf("timestampadd(%s, %s, %s)", datepart, n, start)
			})(datepart, fmt.Sprintf("cast(%s as timestamp)", startDate), fmt.Sprintf("cast(%s as timestamp)", endDate))
		},
	}))
}

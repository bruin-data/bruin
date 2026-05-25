package oracle

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/jinja"
)

func init() { //nolint:gochecknoinits
	jinja.RegisterPlatformOverrides(jinja.PlatformOracle, jinja.MergeBuiltinOverrides(jinja.OracleURLHelpers(), map[string]any{
		"generate_surrogate_key": jinja.SurrogateKeyWithConcat("varchar2(4000)", jinja.ConcatOperator, func(expr string) string {
			return fmt.Sprintf("lower(rawtohex(standard_hash(%s, 'MD5')))", expr)
		}),
		"deduplicate": ansisql.DeduplicateNaturalJoinNoAs,
		"date_spine":  jinja.OracleDateSpine,
	}))
}

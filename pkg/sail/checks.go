package sail

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

// NewColumnCheckOperator builds the column-check runner for Sail. The generic
// numeric/null checks come from ansisql, but accepted_values and pattern need
// Spark SQL syntax (see the types below) rather than the Presto-flavored SQL the
// athena checks emit.
func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        ansisql.NewNotNullCheck(manager),
		"unique":          ansisql.NewUniqueCheck(manager),
		"positive":        ansisql.NewPositiveCheck(manager),
		"non_negative":    ansisql.NewNonNegativeCheck(manager),
		"negative":        ansisql.NewNegativeCheck(manager),
		"min":             ansisql.NewMinCheck(manager),
		"max":             ansisql.NewMaxCheck(manager),
		"accepted_values": &AcceptedValuesCheck{conn: manager},
		"pattern":         &PatternCheck{conn: manager},
	})
}

type AcceptedValuesCheck struct {
	conn config.ConnectionGetter
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.StringArray == nil && ti.Check.Value.IntArray == nil {
		return errors.Errorf("unexpected value for accepted_values check, the values must to be an array, instead %T", ti.Check.Value)
	}

	if ti.Check.Value.StringArray != nil && len(*ti.Check.Value.StringArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	if ti.Check.Value.IntArray != nil && len(*ti.Check.Value.IntArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	var val []string
	if ti.Check.Value.StringArray != nil {
		val = *ti.Check.Value.StringArray
	} else {
		for _, v := range *ti.Check.Value.IntArray {
			val = append(val, strconv.Itoa(v))
		}
	}

	res := strings.Join(val, "','")
	res = fmt.Sprintf("'%s'", res)

	// Spark casts to STRING (CAST(... AS VARCHAR) without a length is a parse
	// error in Spark SQL).
	qq := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE CAST(%s AS STRING) NOT IN (%s)", ti.GetAsset().Name, ti.Column.Name, res)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "accepted_values", func(count int64) error {
		return errors.Errorf("column '%s' has %d rows that are not in the accepted values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

func NewAcceptedValuesCheck(conn config.ConnectionGetter) *AcceptedValuesCheck {
	return &AcceptedValuesCheck{conn: conn}
}

type PatternCheck struct {
	conn config.ConnectionGetter
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return errors.Errorf("unexpected value %s for pattern check, the value must be a string", ti.Check.Value.ToString())
	}

	// Spark SQL uses the RLIKE operator for regex matching (Presto's
	// REGEXP_LIKE is not available).
	qq := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE NOT (%s RLIKE '%s')",
		ti.GetAsset().Name,
		ti.Column.Name,
		*ti.Check.Value.String,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "pattern", func(count int64) error {
		return errors.Errorf("column %s has %d values that don't satisfy the pattern %s", ti.Column.Name, count, *ti.Check.Value.String)
	}).Check(ctx, ti)
}

func NewPatternCheck(conn config.ConnectionGetter) *PatternCheck {
	return &PatternCheck{conn: conn}
}

package oracle

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type AcceptedValuesCheck struct {
	conn config.ConnectionGetter
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.StringArray == nil && ti.Check.Value.IntArray == nil {
		return fmt.Errorf("unexpected value for accepted_values check, the values must to be an array, instead %T", ti.Check.Value)
	}

	if ti.Check.Value.StringArray != nil && len(*ti.Check.Value.StringArray) == 0 {
		return fmt.Errorf("no values provided for accepted_values check")
	}

	if ti.Check.Value.IntArray != nil && len(*ti.Check.Value.IntArray) == 0 {
		return fmt.Errorf("no values provided for accepted_values check")
	}

	var val []string
	if ti.Check.Value.StringArray != nil {
		for _, v := range *ti.Check.Value.StringArray {
			// Escape single quotes to prevent SQL injection / syntax errors
			val = append(val, strings.ReplaceAll(v, "'", "''"))
		}
	} else {
		for _, v := range *ti.Check.Value.IntArray {
			val = append(val, strconv.Itoa(v))
		}
	}

	res := strings.Join(val, "','")
	res = fmt.Sprintf("'%s'", res)

	if err := validateIdentifier(ti.GetAsset().Name, "table name"); err != nil {
		return err
	}
	if err := validateIdentifier(ti.Column.Name, "column name"); err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE CAST(%s as VARCHAR2(4000)) NOT IN (%s)", ti.GetAsset().Name, ti.Column.Name, res)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "accepted_values", func(count int64) error {
		return fmt.Errorf("column '%s' has %d rows that are not in the accepted values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type PatternCheck struct {
	conn config.ConnectionGetter
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return fmt.Errorf("unexpected value %s for pattern check, the value must be a string", ti.Check.Value.ToString())
	}

	// Escape single quotes to prevent SQL injection / syntax errors
	escapedPattern := strings.ReplaceAll(*ti.Check.Value.String, "'", "''")

	if err := validateIdentifier(ti.GetAsset().Name, "table name"); err != nil {
		return err
	}
	if err := validateIdentifier(ti.Column.Name, "column name"); err != nil {
		return err
	}

	// Oracle uses REGEXP_LIKE for regex matching, so NOT REGEXP_LIKE finds lines that do not match the pattern.
	qq := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE NOT REGEXP_LIKE(%s, '%s')",
		ti.GetAsset().Name,
		ti.Column.Name,
		escapedPattern,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "pattern", func(count int64) error {
		return fmt.Errorf("column %s has %d values that don't satisfy the pattern %s", ti.Column.Name, count, *ti.Check.Value.String)
	}).Check(ctx, ti)
}

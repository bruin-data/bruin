package mysql

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

type AcceptedValuesCheck struct {
	conn config.ConnectionGetter
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.StringArray == nil && ti.Check.Value.IntArray == nil {
		return errors.Errorf("unexpected value for accepted_values check, expected an array but received %T", ti.Check.Value)
	}

	if ti.Check.Value.StringArray != nil && len(*ti.Check.Value.StringArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	if ti.Check.Value.IntArray != nil && len(*ti.Check.Value.IntArray) == 0 {
		return errors.Errorf("no values provided for accepted_values check")
	}

	var values []string
	if ti.Check.Value.StringArray != nil {
		values = *ti.Check.Value.StringArray
	} else {
		for _, v := range *ti.Check.Value.IntArray {
			values = append(values, strconv.Itoa(v))
		}
	}

	escaped := strings.Join(values, "','")
	escaped = fmt.Sprintf("'%s'", escaped)

	queryString := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE CAST(%s AS CHAR) NOT IN (%s)",
		ti.GetAsset().Name,
		ti.Column.Name,
		escaped,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: queryString}, "accepted_values", func(count int64) error {
		return errors.Errorf("column '%s' has %d rows that are not in the accepted values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type PatternCheck struct {
	conn config.ConnectionGetter
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return errors.Errorf("unexpected value %s for pattern check, expected a string", ti.Check.Value.ToString())
	}

	queryString := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s WHERE %s NOT LIKE '%s'",
		ti.GetAsset().Name,
		ti.Column.Name,
		*ti.Check.Value.String,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: queryString}, "pattern", func(count int64) error {
		return errors.Errorf("column %s has %d values that do not satisfy the pattern %s", ti.Column.Name, count, *ti.Check.Value.String)
	}).Check(ctx, ti)
}

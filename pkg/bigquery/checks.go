package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type PatternCheck struct {
	conn connectionFetcher
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return errors.Errorf("unexpected value %s for pattern check, the value must be a string", ti.Check.Value.ToString())
	}
	qq := fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE REGEXP_CONTAINS(%s, r'%s')",
		ti.GetAsset().Name,
		ti.Column.Name,
		*ti.Check.Value.String,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "pattern", func(count int64) error {
		return errors.Errorf("column %s has %d values that don't satisfy the pattern %s", ti.Column.Name, count, *ti.Check.Value.String)
	}).Check(ctx, ti)
}

type AcceptedValuesCheck struct {
	conn connectionFetcher
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

	res, err := json.Marshal(val)
	if err != nil {
		return errors.Wrap(err, "failed to marshal accepted values for the query result")
	}

	sz := len(res)
	res = res[1 : sz-1]

	qq := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE CAST(%s as STRING) NOT IN (%s)", ti.GetAsset().Name, ti.Column.Name, res)
	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "accepted_values", func(count int64) error {
		return errors.Errorf("column %s has %d rows that are not in the accepted values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

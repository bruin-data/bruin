package fabric_warehouse

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

func NewColumnCheckOperator(manager config.ConnectionGetter) *ansisql.ColumnCheckOperator {
	return ansisql.NewColumnCheckOperator(map[string]ansisql.CheckRunner{
		"not_null":        &NotNullCheck{conn: manager},
		"unique":          &UniqueCheck{conn: manager},
		"positive":        &PositiveCheck{conn: manager},
		"non_negative":    &NonNegativeCheck{conn: manager},
		"negative":        &NegativeCheck{conn: manager},
		"min":             &MinCheck{conn: manager},
		"max":             &MaxCheck{conn: manager},
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

	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE CAST(%s AS VARCHAR) NOT IN (%s)", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name), res)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "accepted_values", func(count int64) error {
		return errors.Errorf("column '%s' has %d rows that are not in the accepted values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type PatternCheck struct {
	conn config.ConnectionGetter
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	if ti.Check.Value.String == nil {
		return errors.Errorf("unexpected value %s for pattern check, the value must be a string", ti.Check.Value.ToString())
	}

	qq := fmt.Sprintf(
		"SELECT COUNT_BIG(*) FROM %s WHERE %s NOT LIKE '%s'",
		QuoteIdentifier(ti.GetAsset().Name),
		QuoteIdentifier(ti.Column.Name),
		*ti.Check.Value.String,
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "pattern", func(count int64) error {
		return errors.Errorf("column %s has %d values that don't satisfy the pattern %s", ti.Column.Name, count, *ti.Check.Value.String)
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	conn config.ConnectionGetter
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf(
		"SELECT COUNT_BIG(%s) - COUNT_BIG(DISTINCT %s) FROM %s",
		QuoteIdentifier(ti.Column.Name),
		QuoteIdentifier(ti.Column.Name),
		QuoteIdentifier(ti.GetAsset().Name),
	)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "unique", func(count int64) error {
		return errors.Errorf("column '%s' has %d non-unique values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type NotNullCheck struct {
	conn config.ConnectionGetter
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s IS NULL", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name))

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "not_null", func(count int64) error {
		return errors.Errorf("column '%s' has %d null values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type PositiveCheck struct {
	conn config.ConnectionGetter
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s <= 0", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name))

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "positive", func(count int64) error {
		return errors.Errorf("column '%s' has %d non-positive values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type NonNegativeCheck struct {
	conn config.ConnectionGetter
}

func (c *NonNegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s < 0", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name))

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "non_negative", func(count int64) error {
		return errors.Errorf("column '%s' has %d negative values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type NegativeCheck struct {
	conn config.ConnectionGetter
}

func (c *NegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s >= 0", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name))

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "negative", func(count int64) error {
		return errors.Errorf("column '%s' has %d non negative values", ti.Column.Name, count)
	}).Check(ctx, ti)
}

type MinCheck struct {
	conn config.ConnectionGetter
}

func (c *MinCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "min")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s < %s", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name), threshold)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "min", func(count int64) error {
		return errors.Errorf("column '%s' has %d values below minimum %s", ti.Column.Name, count, ti.Check.Value.ToString())
	}).Check(ctx, ti)
}

type MaxCheck struct {
	conn config.ConnectionGetter
}

func (c *MaxCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "max")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT COUNT_BIG(*) FROM %s WHERE %s > %s", QuoteIdentifier(ti.GetAsset().Name), QuoteIdentifier(ti.Column.Name), threshold)

	return ansisql.NewCountableQueryCheck(c.conn, 0, &query.Query{Query: qq}, "max", func(count int64) error {
		return errors.Errorf("column '%s' has %d values above maximum %s", ti.Column.Name, count, ti.Check.Value.ToString())
	}).Check(ctx, ti)
}

func thresholdSQLValue(intPtr *int, floatPtr *float64, stringPtr *string, checkName string) (string, error) {
	switch {
	case intPtr != nil:
		return strconv.Itoa(*intPtr), nil
	case floatPtr != nil:
		return strconv.FormatFloat(*floatPtr, 'f', 6, 64), nil
	case stringPtr != nil:
		return fmt.Sprintf("'%s'", *stringPtr), nil
	default:
		return "", errors.Errorf("unexpected value for %s check, the value must be an int, float or string", checkName)
	}
}

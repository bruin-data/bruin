package ansisql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type CountableQueryCheck struct {
	conn                config.ConnectionGetter
	expectedQueryResult int64
	queryInstance       *query.Query
	checkName           string
	customError         func(count int64) error
}

func NewCountableQueryCheck(conn config.ConnectionGetter, expectedQueryResult int64, queryInstance *query.Query, checkName string, customError func(count int64) error) *CountableQueryCheck {
	return &CountableQueryCheck{
		conn:                conn,
		expectedQueryResult: expectedQueryResult,
		queryInstance:       queryInstance,
		checkName:           checkName,
		customError:         customError,
	}
}

func (c *CountableQueryCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	conn, err := ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	return c.check(ctx, conn)
}

func (c *CountableQueryCheck) CustomCheck(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	conn, err := ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	return c.check(ctx, conn)
}

func (c *CountableQueryCheck) check(ctx context.Context, connectionName string) error {
	q := c.conn.GetConnection(connectionName)
	if q == nil {
		return errors.Errorf("failed to get connection '%s' for '%s' check", connectionName, c.checkName)
	}

	s, ok := q.(selector)
	if !ok {
		return errors.Errorf("connection '%s' cannot be used for the check '%s'", connectionName, c.checkName)
	}

	res, err := s.Select(ctx, c.queryInstance)
	if err != nil {
		return errors.Wrapf(err, "failed '%s' check", c.checkName)
	}

	count, err := helpers.CastResultToInteger(res)
	if err != nil {
		return errors.Wrapf(err, "failed to parse '%s' check result", c.checkName)
	}

	if count != c.expectedQueryResult {
		return c.customError(count)
	}

	return nil
}

type NotNullCheck struct {
	conn config.ConnectionGetter
}

func NewNotNullCheck(conn config.ConnectionGetter) *NotNullCheck {
	return &NotNullCheck{conn: conn}
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", ti.GetAsset().Name, ti.Column.Name)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "not_null", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       annotatedQuery,
		checkName:           "not_null",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d null values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	conn config.ConnectionGetter
}

func NewUniqueCheck(conn config.ConnectionGetter) *UniqueCheck {
	return &UniqueCheck{conn: conn}
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT(%s) - COUNT(DISTINCT %s) FROM %s", ti.Column.Name, ti.Column.Name, ti.GetAsset().Name)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "unique", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "unique",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-unique values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type PositiveCheck struct {
	conn config.ConnectionGetter
}

func NewPositiveCheck(conn config.ConnectionGetter) *PositiveCheck {
	return &PositiveCheck{conn: conn}
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s <= 0", ti.GetAsset().Name, ti.Column.Name)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "positive", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "positive",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-positive values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NonNegativeCheck struct {
	conn config.ConnectionGetter
}

func NewNonNegativeCheck(conn config.ConnectionGetter) *NonNegativeCheck {
	return &NonNegativeCheck{conn: conn}
}

func (c *NonNegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s < 0", ti.GetAsset().Name, ti.Column.Name)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "non_negative", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "non_negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d negative values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NegativeCheck struct {
	conn config.ConnectionGetter
}

func NewNegativeCheck(conn config.ConnectionGetter) *NegativeCheck {
	return &NegativeCheck{conn: conn}
}

func (c *NegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s >= 0", ti.GetAsset().Name, ti.Column.Name)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "negative", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non negative values", ti.Column.Name, count)
		},
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

type MinCheck struct {
	conn config.ConnectionGetter
}

func NewMinCheck(conn config.ConnectionGetter) *MinCheck { return &MinCheck{conn: conn} }

//nolint:dupl
func (c *MinCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "min")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s < %s", ti.GetAsset().Name, ti.Column.Name, threshold)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "min", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "min",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d values below minimum %s", ti.Column.Name, count, ti.Check.Value.ToString())
		},
	}).Check(ctx, ti)
}

type MaxCheck struct {
	conn config.ConnectionGetter
}

func NewMaxCheck(conn config.ConnectionGetter) *MaxCheck { return &MaxCheck{conn: conn} }

//nolint:dupl
func (c *MaxCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "max")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s > %s", ti.GetAsset().Name, ti.Column.Name, threshold)

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Column.Name, "max", ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: annotatedQuery,
		checkName:     "max",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d values above maximum %s", ti.Column.Name, count, ti.Check.Value.ToString())
		},
	}).Check(ctx, ti)
}

type CustomCheck struct {
	conn     config.ConnectionGetter
	renderer jinja.RendererInterface
}

func NewCustomCheck(conn config.ConnectionGetter, renderer jinja.RendererInterface) *CustomCheck {
	return &CustomCheck{conn: conn, renderer: renderer}
}

func (c *CustomCheck) Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	qq := ti.Check.Query
	if c.renderer != nil {
		r, err := c.renderer.CloneForAsset(ctx, ti.GetPipeline(), ti.GetAsset())
		if err != nil {
			return errors.Wrap(err, "failed to create renderer for asset")
		}
		qry, err := r.Render(qq)
		if err != nil {
			return errors.Wrap(err, "failed to render custom check query")
		}

		qq = qry
	}
	expected := ti.Check.Value
	if ti.Check.Count != nil {
		expected = *ti.Check.Count
		qq = fmt.Sprintf("SELECT count(*) FROM (%s) AS t", qq)
	}

	q := &query.Query{Query: qq}
	annotatedQuery, err := AddCustomCheckAnnotationComment(ctx, q, ti.GetAsset().Name, ti.Check.Name, ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}

	return NewCountableQueryCheck(c.conn, expected, annotatedQuery, ti.Check.Name, func(count int64) error {
		return errors.Errorf("custom check '%s' has returned %d instead of the expected %d", ti.Check.Name, count, expected)
	}).CustomCheck(ctx, ti)
}

type CheckRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	checkRunners map[string]CheckRunner
}

func NewColumnCheckOperator(checks map[string]CheckRunner) *ColumnCheckOperator {
	return &ColumnCheckOperator{
		checkRunners: checks,
	}
}

func (o ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column check instance")
	}

	executor, ok := o.checkRunners[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the check type, check cannot be run: " + test.Check.Name)
	}

	return executor.Check(ctx, test)
}

type CustomCheckRunner interface {
	Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error
}

type CustomCheckOperator struct {
	checkRunner CustomCheckRunner
}

func NewCustomCheckOperator(manager config.ConnectionGetter, r jinja.RendererInterface) *CustomCheckOperator {
	return &CustomCheckOperator{
		checkRunner: &CustomCheck{conn: manager, renderer: r},
	}
}

func (o *CustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	instance, ok := ti.(*scheduler.CustomCheckInstance)
	if !ok {
		return errors.New("cannot run a non-custom check instance")
	}

	return o.checkRunner.Check(ctx, instance)
}

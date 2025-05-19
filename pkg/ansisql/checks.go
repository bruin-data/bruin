package ansisql

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type connectionFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type CountableQueryCheck struct {
	conn                connectionFetcher
	expectedQueryResult int64
	queryInstance       *query.Query
	checkName           string
	customError         func(count int64) error
}

func NewCountableQueryCheck(conn connectionFetcher, expectedQueryResult int64, queryInstance *query.Query, checkName string, customError func(count int64) error) *CountableQueryCheck {
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
	q, err := c.conn.GetConnection(connectionName)
	if err != nil {
		return errors.Wrapf(err, "failed to get connection '%s' for '%s' check", connectionName, c.checkName)
	}

	s, ok := q.(selector)
	if !ok {
		return errors.New("connection does not implement selector interface")
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
	conn connectionFetcher
}

func NewNotNullCheck(conn connectionFetcher) *NotNullCheck {
	return &NotNullCheck{conn: conn}
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", ti.GetAsset().Name, ti.Column.Name)

	return (&CountableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "not_null",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d null values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	conn connectionFetcher
}

func NewUniqueCheck(conn connectionFetcher) *UniqueCheck {
	return &UniqueCheck{conn: conn}
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT(%s) - COUNT(DISTINCT %s) FROM %s", ti.Column.Name, ti.Column.Name, ti.GetAsset().Name)
	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "unique",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-unique values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type PositiveCheck struct {
	conn connectionFetcher
}

func NewPositiveCheck(conn connectionFetcher) *PositiveCheck {
	return &PositiveCheck{conn: conn}
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s <= 0", ti.GetAsset().Name, ti.Column.Name)
	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "positive",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-positive values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NonNegativeCheck struct {
	conn connectionFetcher
}

func NewNonNegativeCheck(conn connectionFetcher) *NonNegativeCheck {
	return &NonNegativeCheck{conn: conn}
}

func (c *NonNegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s < 0", ti.GetAsset().Name, ti.Column.Name)
	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "non_negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d negative values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NegativeCheck struct {
	conn connectionFetcher
}

func NewNegativeCheck(conn connectionFetcher) *NegativeCheck {
	return &NegativeCheck{conn: conn}
}

func (c *NegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s >= 0", ti.GetAsset().Name, ti.Column.Name)
	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non negative values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type CustomCheck struct {
	conn     connectionFetcher
	renderer jinja.RendererInterface
}

func NewCustomCheck(conn connectionFetcher) *CustomCheck {
	// TODO: this needs to use an actual renderer instead of the yesterday, since this `NewRendererWithYesterday` does not honor
	// the parameters passed to the `bruin run` command.
	return &CustomCheck{conn: conn, renderer: jinja.NewRendererWithYesterday("your-pipeline-name", "your-run-id")}
}

func (c *CustomCheck) Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	qq := ti.Check.Query
	if c.renderer != nil {
		r := c.renderer.CloneForAsset(ctx, ti.GetAsset())
		qry, err := r.Render(qq)

		if err != nil {
			return errors.Wrap(err, "failed to render custom check query")
		}

		qq = qry
	}
	return NewCountableQueryCheck(c.conn, ti.Check.Value, &query.Query{Query: qq}, ti.Check.Name, func(count int64) error {
		return errors.Errorf("custom check '%s' has returned %d instead of the expected %d", ti.Check.Name, count, ti.Check.Value)
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

func NewCustomCheckOperator(manager connectionFetcher, r jinja.RendererInterface) *CustomCheckOperator {
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

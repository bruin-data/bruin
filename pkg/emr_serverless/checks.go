package emr_serverless //nolint

import (
	"cmp"
	"context"
	"errors"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type CheckRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type CustomCheckRunner interface {
	Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error
}

type builder[T any] func(conn *connectionRemapper) T

type ColumnCheckOperator struct {
	checks map[string]builder[CheckRunner]
	conn   config.ConnectionGetter
}

func (o *ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column check instance")
	}

	executor, ok := o.checks[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the check type, check cannot be run: " + test.Check.Name)
	}

	conn := newConnectionRemapper(o.conn, ti)
	return executor(conn).Check(ctx, test)
}

func NewColumnCheckOperator(conn config.ConnectionGetter) *ColumnCheckOperator {
	return &ColumnCheckOperator{
		conn: conn,
		checks: map[string]builder[CheckRunner]{
			"not_null":        func(c *connectionRemapper) CheckRunner { return ansisql.NewNotNullCheck(c) },
			"unique":          func(c *connectionRemapper) CheckRunner { return ansisql.NewUniqueCheck(c) },
			"positive":        func(c *connectionRemapper) CheckRunner { return ansisql.NewPositiveCheck(c) },
			"non_negative":    func(c *connectionRemapper) CheckRunner { return ansisql.NewNonNegativeCheck(c) },
			"negative":        func(c *connectionRemapper) CheckRunner { return ansisql.NewNegativeCheck(c) },
			"accepted_values": func(c *connectionRemapper) CheckRunner { return athena.NewAcceptedValuesCheck(c) },
			"pattern":         func(c *connectionRemapper) CheckRunner { return athena.NewPatternCheck(c) },
		},
	}
}

type CustomCheckOperator struct {
	conn    config.ConnectionGetter
	builder builder[CustomCheckRunner]
}

func (o *CustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	instance, ok := ti.(*scheduler.CustomCheckInstance)
	if !ok {
		return errors.New("cannot run a non-custom check instance")
	}
	conn := newConnectionRemapper(o.conn, ti)
	return o.builder(conn).Check(ctx, instance)
}

func NewCustomCheckOperator(conn config.ConnectionGetter, renderer jinja.RendererInterface) *CustomCheckOperator {
	return &CustomCheckOperator{
		conn: conn,
		builder: func(c *connectionRemapper) CustomCheckRunner {
			return ansisql.NewCustomCheck(c, renderer)
		},
	}
}

type connectionRemapper struct {
	connGetter config.ConnectionGetter
	ti         scheduler.TaskInstance
}

func (cr *connectionRemapper) GetConnection(string) any {
	name := cmp.Or(
		cr.ti.GetAsset().Parameters["athena_connection"],
		cr.ti.GetPipeline().DefaultConnections["athena"],
	)
	return cr.connGetter.GetConnection(name)
}

func newConnectionRemapper(conn config.ConnectionGetter, ti scheduler.TaskInstance) *connectionRemapper {
	return &connectionRemapper{
		connGetter: conn,
		ti:         ti,
	}
}

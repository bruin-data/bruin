package emr_serverless

import (
	"context"
	"errors"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/athena"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type CheckRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type checkBuilder func(conn connectionFetcher) CheckRunner

type ColumnCheckOperator struct {
	checks map[string]checkBuilder
	conn   connectionFetcher
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

func NewColumnCheckOperator(conn connectionFetcher) *ColumnCheckOperator {
	return &ColumnCheckOperator{
		conn: conn,
		checks: map[string]checkBuilder{
			"not_null":        func(c connectionFetcher) CheckRunner { return ansisql.NewNotNullCheck(c) },
			"unique":          func(c connectionFetcher) CheckRunner { return ansisql.NewUniqueCheck(c) },
			"positive":        func(c connectionFetcher) CheckRunner { return ansisql.NewPositiveCheck(c) },
			"non_negative":    func(c connectionFetcher) CheckRunner { return ansisql.NewNonNegativeCheck(c) },
			"negative":        func(c connectionFetcher) CheckRunner { return ansisql.NewNegativeCheck(c) },
			"accepted_values": func(c connectionFetcher) CheckRunner { return athena.NewAcceptedValuesCheck(c) },
			"pattern":         func(c connectionFetcher) CheckRunner { return athena.NewPatternCheck(c) },
		},
	}
}

type connectionRemapper struct {
	connectionFetcher
	ti scheduler.TaskInstance
}

func (cr *connectionRemapper) GetConnection(string) (interface{}, error) {
	asset := cr.ti.GetAsset()
	connectionName := asset.Parameters["athena_connection"]
	if strings.TrimSpace(connectionName) == "" {
		connectionName = "athena-default"
	}
	return cr.connectionFetcher.GetConnection(connectionName)
}

func newConnectionRemapper(conn connectionFetcher, ti scheduler.TaskInstance) *connectionRemapper {
	return &connectionRemapper{
		connectionFetcher: conn,
		ti:                ti,
	}
}

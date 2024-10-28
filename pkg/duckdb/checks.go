package duck

import (
	"context"
	"errors"

	"github.com/bruin-data/bruin/pkg/scheduler"
)

type selectorFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type AcceptedValuesCheck struct {
	conn selectorFetcher
}

func (c *AcceptedValuesCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	return errors.New("duckDB not supported")
}

type PatternCheck struct {
	conn connectionFetcher
}

func (c *PatternCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	return errors.New("duckDB not supported")
}

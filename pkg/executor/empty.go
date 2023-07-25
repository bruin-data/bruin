package executor

import (
	"context"

	"github.com/bruin-data/bruin/pkg/scheduler"
)

type NoOpOperator struct{}

func (e NoOpOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return nil
}

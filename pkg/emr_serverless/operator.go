package emr_serverless

import (
	"context"
	"errors"

	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type BasicOperator struct{}

func (op *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return nil
}

func NewBasicOperator(cm *connection.Manager) (*BasicOperator, error) {
	return nil, errors.New("not implemented")
}

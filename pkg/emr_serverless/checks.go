package emr_serverless

import (
	"context"

	"github.com/bruin-data/bruin/pkg/scheduler"
)

type ColumnCheckOperator struct {
}

func (o *ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return nil
}

func NewColumnCheckOperator(manager connectionFetcher) *ColumnCheckOperator {
	return &ColumnCheckOperator{}
}

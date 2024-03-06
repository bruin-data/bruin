package ingestr

import (
	"context"

	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type BasicOperator struct{}

func NewBasicOperator() *BasicOperator {
	return &BasicOperator{}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	return nil
}

type IngestrCheckOperator struct {
	conn    *connection.Manager
	configs *map[pipeline.AssetType]executor.Config
}

func (i IngestrCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	assetType, err := asset.GetIngestrDestinationType()
	if err != nil {
		return err
	}

	columnChecker := (*i.configs)[assetType][scheduler.TaskInstanceTypeColumnCheck]

	return columnChecker.Run(ctx, ti)
}

func NewColumnCheckOperator(conn *connection.Manager, configs *map[pipeline.AssetType]executor.Config) *IngestrCheckOperator {
	return &IngestrCheckOperator{
		conn:    conn,
		configs: configs,
	}
}

type IngestrCustomCheckOperator struct {
	conn    *connection.Manager
	configs *map[pipeline.AssetType]executor.Config
}

func (i IngestrCustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	assetType, err := asset.GetIngestrDestinationType()
	if err != nil {
		return err
	}

	columnChecker := (*i.configs)[assetType][scheduler.TaskInstanceTypeCustomCheck]

	return columnChecker.Run(ctx, ti)
}

func NewCustomCheckOperator(conn *connection.Manager, configs *map[pipeline.AssetType]executor.Config) *IngestrCustomCheckOperator {
	return &IngestrCustomCheckOperator{
		conn:    conn,
		configs: configs,
	}
}

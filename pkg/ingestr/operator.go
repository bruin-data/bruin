package ingestr

import (
	"context"
	"errors"

	"fmt"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/client"
)

const IngestrVersion = "v0.2.2"

type BasicOperator struct {
	client *client.Client
}

func NewBasicOperator() (*BasicOperator, error) {
	dockerClient, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	return &BasicOperator{client: dockerClient}, nil
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	return errors.New("ingestr tasks not implemented")
}

type IngestrCheckOperator struct {
	configs *map[pipeline.AssetType]executor.Config
}

func (i IngestrCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	assetType, err := helpers.GetIngestrDestinationType(asset)
	if err != nil {
		return err
	}

	columnChecker, ok := (*i.configs)[assetType][scheduler.TaskInstanceTypeColumnCheck]
	if !ok {
		return errors.New("missing column check configuration")
	}

	return columnChecker.Run(ctx, ti)
}

func NewColumnCheckOperator(configs *map[pipeline.AssetType]executor.Config) *IngestrCheckOperator {
	return &IngestrCheckOperator{
		configs: configs,
	}
}

type IngestrCustomCheckOperator struct {
	configs *map[pipeline.AssetType]executor.Config
}

func (i IngestrCustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	assetType, err := helpers.GetIngestrDestinationType(asset)
	if err != nil {
		return err
	}

	columnChecker, ok := (*i.configs)[assetType][scheduler.TaskInstanceTypeCustomCheck]
	if !ok {
		return errors.New("missing column check configuration")
	}

	return columnChecker.Run(ctx, ti)
}

func NewCustomCheckOperator(configs *map[pipeline.AssetType]executor.Config) *IngestrCustomCheckOperator {
	return &IngestrCustomCheckOperator{
		configs: configs,
	}
}

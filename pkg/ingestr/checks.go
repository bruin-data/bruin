package ingestr

import (
	"context"
	"errors"

	"fmt"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"io"
)

const IngestrVersion = "v0.2.2"

type BasicOperator struct {
	client *client.Client
}

func NewBasicOperator() (*BasicOperator, error) {
	ctx := context.TODO()
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	defer dockerClient.Close()

	dockerImage := fmt.Sprintf("ghcr.io/bruin-data/ingestr:%s", IngestrVersion)
	reader, err := dockerClient.ImagePull(ctx, dockerImage, types.ImagePullOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch docker image: %s", err.Error())
	}
	defer reader.Close()
	io.Copy(io.Discard, reader)
	//io.Copy(os.Stdout, reader)

	resp, err := dockerClient.ContainerCreate(ctx, &container.Config{
		Image: "ingestr",
		Cmd:   []string{"echo", "hello world"},
		Tty:   false,
	}, nil, nil, nil, "")
	if err != nil {
		panic(err)
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

package ingestr

import (
	"context"
	"fmt"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"io"
)

const IngestrVersion = "v0.2.2"

type BasicOperator struct {
	client *client.Client
	conn   *connection.Manager
}

func NewBasicOperator(conn *connection.Manager) (*BasicOperator, error) {
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
	//io.Copy(os.Stdout, reader) // To see output

	return &BasicOperator{client: dockerClient, conn: conn}, nil
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	resp, err := o.client.ContainerCreate(ctx, &container.Config{
		Image: "ingestr",
		Cmd: []string{
			"ingestr",
			"ingest",
			"--source-uri",
			ti.GetAsset().Parameters["source_uri"],
			"--source-table",
			"some-table",
			"--destination-uri",
			"some-uri",
			"--destination-table",
			"some-table",
		},
		Tty: false,
		Env: []string{"FOO=bar"},
	}, nil, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create docker container: %s", err.Error())
	}

	err = o.client.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start docker container: %s", err.Error())
	}

	statusCh, errCh := o.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed after waiting for docker container to start: %s", err.Error())
		}
	case <-statusCh:
	}

	return nil
}

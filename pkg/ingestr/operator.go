package ingestr

import (
	"context"
	"errors"
	"fmt"

	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"io"
)

const (
	IngestrVersion = "v0.2.2"
	DockerImage    = "ghcr.io/bruin-data/ingestr:" + IngestrVersion
)

type BasicOperator struct {
	client containerClient
	conn   *connection.Manager
}

type pipelineConnection interface {
	GetConnectionURI() (string, error)
}

type containerClient interface {
	Close() error
	ImagePull(ctx context.Context, refStr string, options types.ImagePullOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, container string, options container.StartOptions) error
	ContainerWait(ctx context.Context, container string, condition container.WaitCondition) (<-chan container.WaitResponse, <-chan error)
}

type clientInitiator func(ops ...client.Opt) (*client.Client, error)

func NewBasicOperator(conn *connection.Manager, initiator clientInitiator) (*BasicOperator, error) {
	ctx := context.TODO()
	dockerClient, err := initiator(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	defer dockerClient.Close()

	dockerImage := "ghcr.io/bruin-data/ingestr:" + IngestrVersion
	reader, err := dockerClient.ImagePull(ctx, dockerImage, types.ImagePullOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch docker image: %s", err.Error())
	}
	defer reader.Close()
	_, err = io.Copy(io.Discard, reader)
	// _, err = io.Copy(os.Stdout, reader)
	if err != nil {
		return nil, fmt.Errorf("error while copying output: %s", err.Error())
	}

	return &BasicOperator{client: dockerClient, conn: conn}, nil
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	sourceConnectionName, ok := ti.GetAsset().Parameters["source_connection"]
	if !ok {
		return errors.New("source connection not configured")
	}
	sourceConnection, err := o.conn.GetConnection(sourceConnectionName)
	if err != nil {
		return fmt.Errorf("source connection %s not found", sourceConnectionName)
	}
	sourceURI, err := sourceConnection.(pipelineConnection).GetConnectionURI()
	if err != nil {
		return errors.New("could not get the source uri")
	}
	sourceTable, ok := ti.GetAsset().Parameters["source_table"]
	if !ok {
		return errors.New("source table not configured")
	}

	destConnectionName, ok := ti.GetAsset().Parameters["destination_connection"]
	if !ok {
		return errors.New("destination connection not configured")
	}
	destConnection, err := o.conn.GetConnection(destConnectionName)
	if err != nil {
		return fmt.Errorf("destination connection %s not found", destConnectionName)
	}
	destURI, err := destConnection.(pipelineConnection).GetConnectionURI()
	if err != nil {
		return errors.New("could not get the source uri")
	}
	destTable, ok := ti.GetAsset().Parameters["destination_table"]
	if !ok {
		return errors.New("source table not configured")
	}

	resp, err := o.client.ContainerCreate(ctx, &container.Config{
		Image: DockerImage,
		Cmd: []string{
			"ingest",
			"--source-uri",
			sourceURI,
			"--source-table",
			sourceTable,
			"--dest-uri",
			destURI,
			"--dest-table",
			destTable,
			"--yes",
		},
		// AttachStdout: true,
		Env: []string{},
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
		return nil
	}

	return nil
}

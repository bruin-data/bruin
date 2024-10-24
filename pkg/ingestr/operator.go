package ingestr

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/pkg/errors"
)

const (
	IngestrVersion = "v0.8.5"
	DockerImage    = "ghcr.io/bruin-data/ingestr:" + IngestrVersion
)

type connectionFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type BasicOperator struct {
	client *client.Client
	conn   connectionFetcher
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn *connection.Manager) (*BasicOperator, error) {
	ctx := context.TODO()
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}
	defer dockerClient.Close()

	reader, err := dockerClient.ImagePull(ctx, DockerImage, types.ImagePullOptions{})
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

func (o *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	cmdArgs, err := o.ConvertTaskInstanceToIngestrCommand(ctx, ti)
	if err != nil {
		return err
	}

	writer := ctx.Value(executor.KeyPrinter).(io.Writer)
	_, _ = writer.Write([]byte("Triggering ingestr...\n"))

	resp, err := o.client.ContainerCreate(ctx, &container.Config{
		Image:        DockerImage,
		Cmd:          cmdArgs,
		AttachStdout: false,
		AttachStderr: true,
		Tty:          true,
		Env:          []string{},
	}, &container.HostConfig{
		NetworkMode: "host",
	}, nil, nil, "")
	if err != nil {
		return fmt.Errorf("failed to create docker container: %s", err.Error())
	}

	err = o.client.ContainerStart(ctx, resp.ID, container.StartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start docker container: %s", err.Error())
	}

	go func() {
		reader, err := o.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: false,
		})
		if err != nil {
			panic(err)
		}
		defer reader.Close()

		scanner := bufio.NewScanner(reader)

		if ctx.Value(executor.KeyPrinter) == nil {
			return
		}

		for scanner.Scan() {
			message := scanner.Text()
			if !strings.HasSuffix(message, "\n") {
				message += "\n"
			}
			_, _ = writer.Write([]byte(message))
		}
	}()

	statusCh, errCh := o.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)

	select {
	case <-ctx.Done():
		_, _ = writer.Write([]byte("Stopping the ingestr container\n"))
		_ = o.client.ContainerStop(ctx, resp.ID, container.StopOptions{})
		return ctx.Err()
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("failed after waiting for docker container to start: %s", err.Error())
		}
	case res := <-statusCh:
		if res.Error != nil {
			return fmt.Errorf("failed after waiting for docker container to finish: %s", res.Error.Message)
		}

		if res.StatusCode != 0 {
			return fmt.Errorf("ingestr container failed with status code %d, please check the logs above for errors", res.StatusCode)
		}

		_, _ = writer.Write([]byte(fmt.Sprintf("ingestr container completed with response code: %d\n", res.StatusCode)))
	}

	return nil
}

func (o *BasicOperator) ConvertTaskInstanceToIngestrCommand(ctx context.Context, ti scheduler.TaskInstance) ([]string, error) {
	sourceConnectionName, ok := ti.GetAsset().Parameters["source_connection"]
	if !ok {
		return nil, errors.New("source connection not configured")
	}

	sourceConnection, err := o.conn.GetConnection(sourceConnectionName)
	if err != nil {
		return nil, errors.Wrapf(err, "source connection %s not found", sourceConnectionName)
	}

	sourceURI, err := sourceConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return nil, errors.New("could not get the source uri")
	}

	// some connection types can be shared among sources, therefore inferring source URI from the connection type is not
	// always feasible. In the case of GSheets, we have to reuse the same GCP credentials, but change the prefix with gsheets://
	if ti.GetAsset().Parameters["source"] == "gsheets" {
		sourceURI = strings.ReplaceAll(sourceURI, "bigquery://", "gsheets://")
	}

	sourceTable, ok := ti.GetAsset().Parameters["source_table"]
	if !ok {
		return nil, errors.New("source table not configured")
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return nil, err
	}

	destConnection, err := o.conn.GetConnection(destConnectionName)
	if err != nil {
		return nil, fmt.Errorf("destination connection %s not found", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return nil, errors.New("could not get the source uri")
	}

	destTable := ti.GetAsset().Name

	cmdArgs := []string{
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
		"--progress",
		"log",
	}

	incrementalStrategy, ok := ti.GetAsset().Parameters["incremental_strategy"]
	if ok {
		cmdArgs = append(cmdArgs, "--incremental-strategy", incrementalStrategy)
	}

	incrementalKey, ok := ti.GetAsset().Parameters["incremental_key"]
	if ok {
		cmdArgs = append(cmdArgs, "--incremental-key", incrementalKey)
	}

	primaryKeys := ti.GetAsset().ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		for _, pk := range primaryKeys {
			cmdArgs = append(cmdArgs, "--primary-key", pk)
		}
	}

	loaderFileFormat, ok := ti.GetAsset().Parameters["loader_file_format"]
	if ok {
		cmdArgs = append(cmdArgs, "--loader-file-format", loaderFileFormat)
	}

	sqlBackend, ok := ti.GetAsset().Parameters["sql_backend"]
	if ok {
		cmdArgs = append(cmdArgs, "--sql-backend", sqlBackend)
	}

	injectIntervals, ok := ti.GetAsset().Parameters["inject_intervals"]
	if ok {
		boolInject, err := strconv.ParseBool(injectIntervals)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse inject_intervals")
		}

		if boolInject {
			startDateString := ctx.Value(pipeline.RunConfigStartDate).(time.Time).Format(time.RFC3339)
			endDateString := ctx.Value(pipeline.RunConfigEndDate).(time.Time).Format(time.RFC3339)

			cmdArgs = append(cmdArgs, "--interval-start", startDateString, "--interval-end", endDateString)
		}
	}

	fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
	if fullRefresh != nil && fullRefresh.(bool) {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}

	return cmdArgs, nil
}

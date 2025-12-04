package dataprocserverless

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	"cloud.google.com/go/logging/logadmin"
	"cloud.google.com/go/storage"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/env"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/poll"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type BasicOperator struct {
	connection config.ConnectionGetter
	env        map[string]string
}

func (op *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	logger := log.New(
		ctx.Value(executor.KeyPrinter).(io.Writer), "", 0,
	)
	asset := ti.GetAsset()
	connID, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return fmt.Errorf("error looking up connection name: %w", err)
	}
	conn, ok := op.connection.GetConnection(connID).(*Client)
	if !ok {
		return fmt.Errorf("'%s' either does not exist or is not a Dataproc Serverless connection", connID)
	}

	if asset.Type == pipeline.AssetTypeDataprocServerlessPyspark && conn.Workspace == "" {
		return fmt.Errorf("connection %q is missing field: workspace", connID)
	}

	params := parseParams(conn, asset.Parameters)
	clientOptions := conn.getClientOptions()

	batchClient, err := dataproc.NewBatchControllerClient(ctx, clientOptions...)
	if err != nil {
		return fmt.Errorf("error creating dataproc batch client: %w", err)
	}
	defer batchClient.Close()

	storageClient, err := storage.NewClient(ctx, clientOptions...)
	if err != nil {
		return fmt.Errorf("error creating storage client: %w", err)
	}
	defer storageClient.Close()

	logClient, err := logadmin.NewClient(ctx, conn.ProjectID, clientOptions...)
	if err != nil {
		return fmt.Errorf("error creating logging client: %w", err)
	}
	defer logClient.Close()

	envVars, err := env.SetupVariables(ctx, ti.GetPipeline(), asset, cloneEnv(op.env))
	if err != nil {
		return fmt.Errorf("error setting up environment variables: %w", err)
	}

	job := Job{
		logger:        logger,
		batchClient:   batchClient,
		storageClient: storageClient,
		logClient:     logClient,
		params:        params,
		asset:         asset,
		pipeline:      ti.GetPipeline(),
		poll: &poll.Timer{
			BaseDuration: time.Second,
			// maximum backoff: 32 seconds
			MaxRetry: 5,
		},
		env: envVars,
	}

	return job.Run(ctx)
}

func NewBasicOperator(conn config.ConnectionGetter, env map[string]string) (*BasicOperator, error) {
	return &BasicOperator{
		connection: conn,
		env:        env,
	}, nil
}

func cloneEnv(env map[string]string) map[string]string {
	clone := make(map[string]string, len(env))
	for k, v := range env {
		clone[k] = v
	}
	return clone
}

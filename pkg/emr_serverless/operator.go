package emr_serverless //nolint

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bruin-data/bruin/pkg/env"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type connectionFetcher interface {
	GetConnection(name string) any
}

type BasicOperator struct {
	connection connectionFetcher
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
	conn, ok := op.connection.GetConnection(connID).(Client)
	if !ok {
		return fmt.Errorf("'%s' either does not exist or is not a EMR Serverless connection", connID)
	}

	if asset.Type == pipeline.AssetTypeEMRServerlessPyspark && conn.Workspace == "" {
		return fmt.Errorf("connection %q is missing field: workspace", connID)
	}

	params := parseParams(&conn, asset.Parameters)
	cfg, err := awsCfg.LoadDefaultConfig(
		ctx,
		awsCfg.WithRegion(conn.Region),
		awsCfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				conn.AccessKey, conn.SecretKey, "",
			),
		),
	)
	if err != nil {
		return fmt.Errorf("error loading aws config: %w", err)
	}

	env, err := env.SetupVariables(ctx, ti.GetPipeline(), asset, cloneEnv(op.env))
	if err != nil {
		return fmt.Errorf("error setting up environment variables: %w", err)
	}

	job := Job{
		logger:    logger,
		s3Client:  s3.NewFromConfig(cfg),
		emrClient: emrserverless.NewFromConfig(cfg),
		params:    params,
		asset:     asset,
		pipeline:  ti.GetPipeline(),
		poll: &PollTimer{
			BaseDuration: time.Second,

			// maximum backoff: 32 seconds
			MaxRetry: 5,
		},
		env: env,
	}

	return job.Run(ctx)
}

func NewBasicOperator(conn connectionFetcher, env map[string]string) (*BasicOperator, error) {
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

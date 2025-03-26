package emr_serverless

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type BasicOperator struct {
	client *emrserverless.Client
}

func (op *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()

	// TODO: validation
	applicationId := asset.Parameters["application_id"]
	executionRole := asset.Parameters["execution_role"]
	entryPoint := asset.Parameters["entrypoint"]

	result, err := op.client.StartJobRun(ctx, &emrserverless.StartJobRunInput{
		ApplicationId:           aws.String(applicationId),
		Name:                    aws.String(asset.Name),
		ExecutionRoleArn:        aws.String(executionRole),
		ExecutionTimeoutMinutes: aws.Int64(5),
		JobDriver: &types.JobDriverMemberSparkSubmit{
			Value: types.SparkSubmit{
				EntryPoint: aws.String(entryPoint),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("error submitting job run: %w", err)
	}
	fmt.Println("started", *result.JobRunId)
	return nil
}

func NewBasicOperator(cm *connection.Manager) (*BasicOperator, error) {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error loading aws config: %w", err)
	}
	op := &BasicOperator{
		client: emrserverless.NewFromConfig(cfg),
	}
	return op, nil
}

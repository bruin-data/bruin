package emr_serverless

import (
	"context"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

var (
	terminalJobRunStates = []types.JobRunState{
		types.JobRunStateCancelled,
		types.JobRunStateFailed,
		types.JobRunStateSuccess,
	}
)

type BasicOperator struct {
	client *emrserverless.Client
}

func (op *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {

	logger := log.New(
		ctx.Value(executor.KeyPrinter).(io.Writer), "", 0,
	)

	asset := ti.GetAsset()

	// TODO: validation
	applicationId := asset.Parameters["application_id"]
	executionRole := asset.Parameters["execution_role"]
	entryPoint := asset.Parameters["entrypoint"]
	timeout := time.Duration(0)
	args := []string{}

	if asset.Parameters["timeout"] != "" {
		t, err := time.ParseDuration(asset.Parameters["timeout"])
		if err == nil {
			timeout = t
		}
	}

	if asset.Parameters["args"] != "" {
		arglist := strings.Split(strings.TrimSpace(asset.Parameters["args"]), " ")
		for _, arg := range arglist {
			arg = strings.TrimSpace(arg)
			if arg != "" {
				args = append(args, arg)
			}
		}
	}

	result, err := op.client.StartJobRun(ctx, &emrserverless.StartJobRunInput{
		ApplicationId:           aws.String(applicationId),
		Name:                    aws.String(asset.Name),
		ExecutionRoleArn:        aws.String(executionRole),
		ExecutionTimeoutMinutes: aws.Int64(int64(timeout.Minutes())),
		JobDriver: &types.JobDriverMemberSparkSubmit{
			Value: types.SparkSubmit{
				EntryPoint:            aws.String(entryPoint),
				EntryPointArguments:   args,
				SparkSubmitParameters: aws.String(asset.Parameters["config"]),
			},
		},
	})

	if err != nil {
		return fmt.Errorf("error submitting job run: %w", err)
	}
	logger.Printf("job run submitted: %s", *result.JobRunId)

	previousState := types.JobRunState("unknown")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			runs, err := op.client.ListJobRunAttempts(ctx, &emrserverless.ListJobRunAttemptsInput{
				ApplicationId: aws.String(applicationId),
				JobRunId:      result.JobRunId,
			})
			if err != nil {
				return fmt.Errorf("error checking job run status: %w", err)
			}
			if len(runs.JobRunAttempts) == 0 {
				return fmt.Errorf("job runs not found")
			}
			latestRun := runs.JobRunAttempts[len(runs.JobRunAttempts)-1]
			if previousState != latestRun.State {
				logger.Printf("job run: %s: %s", *result.JobRunId, latestRun.State)
				previousState = latestRun.State
			}
			if slices.Contains(terminalJobRunStates, latestRun.State) {
				return nil
			}
			time.Sleep(time.Second)
		}
	}
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

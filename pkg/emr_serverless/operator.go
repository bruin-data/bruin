package emr_serverless

import (
	"context"
	"fmt"
	"io"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
	connections map[string]config.AwsConnection
}

func (op *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	logger := log.New(
		ctx.Value(executor.KeyPrinter).(io.Writer), "", 0,
	)
	asset := ti.GetAsset()
	pipeline := ti.GetPipeline()
	connID, err := pipeline.GetConnectionNameForAsset(asset)
	if err != nil {
		return fmt.Errorf("error looking up connection name: %w", err)
	}
	conn, exists := op.connections[connID]

	if !exists {
		return fmt.Errorf("aws connection not found for '%s", connID)
	}

	params := parseParams(asset.Parameters)
	cfg, err := awsCfg.LoadDefaultConfig(
		ctx,
		awsCfg.WithRegion(params.Region),
		awsCfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				conn.AccessKey, conn.SecretKey, "",
			),
		),
	)

	if err != nil {
		return fmt.Errorf("error loading aws config: %w", err)
	}

	job := Job{
		logger: logger,
		client: emrserverless.NewFromConfig(cfg),
		params: params,
		asset:  asset,
	}

	return job.Run(ctx)
}

func NewBasicOperator(cfg *config.Config) (*BasicOperator, error) {
	op := &BasicOperator{
		connections: make(map[string]config.AwsConnection),
	}
	for _, conn := range cfg.SelectedEnvironment.Connections.AwsConnection {
		op.connections[conn.Name] = conn
	}
	return op, nil
}

type JobRunParams struct {
	ApplicationID string
	ExecutionRole string
	Entrypoint    string
	Args          []string
	Config        string
	MaxAttempts   int
	Timeout       time.Duration
	Region        string
}

func parseParams(m map[string]string) *JobRunParams {
	params := JobRunParams{
		ApplicationID: m["application_id"],
		ExecutionRole: m["execution_role"],
		Entrypoint:    m["entrypoint"],
		Config:        m["config"],
		MaxAttempts:   1,
		Region:        m["region"],
	}

	if m["timeout"] != "" {
		t, err := time.ParseDuration(m["timeout"])
		if err == nil {
			params.Timeout = t
		}
	}
	if m["args"] != "" {
		arglist := strings.Split(strings.TrimSpace(m["args"]), " ")
		for _, arg := range arglist {
			arg = strings.TrimSpace(arg)
			if arg != "" {
				params.Args = append(params.Args, arg)
			}
		}
	}
	if m["max_attempts"] != "" {
		max, err := strconv.ParseInt(m["max_attempts"], 10, 32)
		if err == nil && max > 0 {
			params.MaxAttempts = int(max)
		}
	}
	return &params
}

type Job struct {
	logger *log.Logger
	client *emrserverless.Client
	asset  *pipeline.Asset
	params *JobRunParams
}

func (job Job) Run(ctx context.Context) (err error) {

	result, err := job.client.StartJobRun(ctx, &emrserverless.StartJobRunInput{
		ApplicationId:           &job.params.ApplicationID,
		Name:                    &job.asset.Name,
		ExecutionRoleArn:        &job.params.ExecutionRole,
		ExecutionTimeoutMinutes: aws.Int64(int64(job.params.Timeout.Minutes())),
		RetryPolicy: &types.RetryPolicy{
			MaxAttempts: aws.Int32(int32(job.params.MaxAttempts)),
		},
		JobDriver: &types.JobDriverMemberSparkSubmit{
			Value: types.SparkSubmit{
				EntryPoint:            &job.params.Entrypoint,
				EntryPointArguments:   job.params.Args,
				SparkSubmitParameters: &job.params.Config,
			},
		},
	})

	if err != nil {
		return fmt.Errorf("error submitting job run: %w", err)
	}
	job.logger.Printf("created job run: %s", *result.JobRunId)
	defer func() {
		if err != nil {
			job.logger.Printf("error detected. cancelling job run.")
			job.client.CancelJobRun(context.Background(), &emrserverless.CancelJobRunInput{
				ApplicationId: &job.params.ApplicationID,
				JobRunId:      result.JobRunId,
			})
		}
	}()

	previousState := types.JobRunState("unknown")
	attempt := 1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			runs, err := job.client.ListJobRunAttempts(ctx, &emrserverless.ListJobRunAttemptsInput{
				ApplicationId: &job.params.ApplicationID,
				JobRunId:      result.JobRunId,
			})
			if err != nil {
				return fmt.Errorf("error checking job run status: %w", err)
			}
			if len(runs.JobRunAttempts) == 0 {
				return fmt.Errorf("job runs not found")
			}
			totalJobRuns := len(runs.JobRunAttempts)
			if attempt > totalJobRuns {
				// invariant. attempt _should_ never exceed total job runs.
				// this can happen when a job run is cancelled externally.
				return nil
			}
			latestRun := runs.JobRunAttempts[attempt-1]
			if previousState != latestRun.State {
				job.logger.Printf("%s | %d/%d | %s", *result.JobRunId, attempt, job.params.MaxAttempts, latestRun.State)
				previousState = latestRun.State
			}
			if slices.Contains(terminalJobRunStates, latestRun.State) {
				if attempt == job.params.MaxAttempts {
					return nil
				}
				attempt++
			}
			time.Sleep(time.Second)
		}
	}
}

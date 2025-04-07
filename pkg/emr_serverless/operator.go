package emr_serverless //nolint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

var terminalJobRunStates = []types.JobRunState{
	types.JobRunStateFailed,
	types.JobRunStateSuccess,
	types.JobRunStateCancelled,
}

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
		logger:    logger,
		s3Client:  s3.NewFromConfig(cfg),
		emrClient: emrserverless.NewFromConfig(cfg),
		params:    params,
		asset:     asset,
		poll: &PollTimer{
			BaseDuration: time.Second,

			// maximum backoff: 32 seconds
			MaxRetry: 5,
		},
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
	Timeout       time.Duration
	Region        string
	Logs          string
}

func parseParams(m map[string]string) *JobRunParams {
	params := JobRunParams{
		ApplicationID: m["application_id"],
		ExecutionRole: m["execution_role"],
		Entrypoint:    m["entrypoint"],
		Config:        m["config"],
		Region:        m["region"],
		Logs:          m["logs"],
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
	return &params
}

type Job struct {
	logger    *log.Logger
	emrClient *emrserverless.Client
	s3Client  *s3.Client
	asset     *pipeline.Asset
	params    *JobRunParams
	poll      *PollTimer
}

func (job Job) buildJobRunConfig() *emrserverless.StartJobRunInput {
	cfg := &emrserverless.StartJobRunInput{
		ApplicationId:           &job.params.ApplicationID,
		Name:                    &job.asset.Name,
		ExecutionRoleArn:        &job.params.ExecutionRole,
		ExecutionTimeoutMinutes: aws.Int64(int64(job.params.Timeout.Minutes())),
		JobDriver: &types.JobDriverMemberSparkSubmit{
			Value: types.SparkSubmit{
				EntryPoint:            &job.params.Entrypoint,
				EntryPointArguments:   job.params.Args,
				SparkSubmitParameters: &job.params.Config,
			},
		},
	}

	if job.params.Logs != "" {
		cfg.ConfigurationOverrides = &types.ConfigurationOverrides{
			MonitoringConfiguration: &types.MonitoringConfiguration{
				S3MonitoringConfiguration: &types.S3MonitoringConfiguration{
					LogUri: aws.String(job.params.Logs),
				},
			},
		}
	}

	return cfg
}

func (job Job) Run(ctx context.Context) (err error) { //nolint

	run, err := job.emrClient.StartJobRun(ctx, job.buildJobRunConfig())
	if err != nil {
		return fmt.Errorf("error submitting job run: %w", err)
	}
	job.logger.Printf("created job run: %s", *run.JobRunId)
	defer func() { //nolint
		if err != nil {
			job.logger.Printf("error detected. cancelling job run.")
			job.emrClient.CancelJobRun(context.Background(), &emrserverless.CancelJobRunInput{ //nolint
				ApplicationId: &job.params.ApplicationID,
				JobRunId:      run.JobRunId,
			})
		}
	}()

	var (
		previousState    = types.JobRunState("unknown")
		paginationToken  = ""
		maxAttemptsError = &retry.MaxAttemptsError{}
		jobLogs          = job.buildLogConsumer(ctx, run)
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(job.poll.Duration()):
			listJobArgs := &emrserverless.ListJobRunAttemptsInput{
				ApplicationId: &job.params.ApplicationID,
				JobRunId:      run.JobRunId,
			}
			if paginationToken != "" {
				listJobArgs.NextToken = &paginationToken
			}
			runs, err := job.emrClient.ListJobRunAttempts(ctx, listJobArgs)
			if errors.As(err, &maxAttemptsError) {
				job.poll.Increase()
				continue
			}
			job.poll.Reset()

			if err != nil {
				return fmt.Errorf("error checking job run status: %w", err)
			}
			if len(runs.JobRunAttempts) == 0 {
				return errors.New("job runs not found")
			}

			latestRun := runs.JobRunAttempts[len(runs.JobRunAttempts)-1]
			if previousState != latestRun.State {
				job.logger.Printf(
					"%s | %s | %s",
					*run.JobRunId,
					latestRun.State,
					*latestRun.StateDetails,
				)
				previousState = latestRun.State
			}
			for _, line := range jobLogs.Next() {
				job.logger.Printf("%s | %s | %s ", line.Source.Name, line.Source.Stream, line.Message)
			}
			if slices.Contains(terminalJobRunStates, latestRun.State) {
				return nil
			}
			if runs.NextToken != nil {
				paginationToken = *runs.NextToken
			}
		}
	}
}

type LogConsumer interface {
	Next() []LogLine
}

func (job Job) buildLogConsumer(ctx context.Context, run *emrserverless.StartJobRunOutput) LogConsumer {
	logURI := job.resolveLogURI(ctx, run)
	if logURI != "" {
		uri, err := url.Parse(logURI)
		if err == nil {
			return &S3LogConsumer{
				Ctx:   ctx,
				URI:   uri,
				S3cli: job.s3Client,
				RunID: *run.JobRunId,
				AppID: *run.ApplicationId,
			}
		}
	}

	return &NoOpLogConsumer{}
}

func (job Job) resolveLogURI(ctx context.Context, run *emrserverless.StartJobRunOutput) string {
	if job.params.Logs != "" {
		return job.params.Logs
	}

	app, err := job.emrClient.GetApplication(
		ctx,
		&emrserverless.GetApplicationInput{
			ApplicationId: run.ApplicationId,
		},
	)
	if err != nil {
		return ""
	}

	monitoringCfg := *app.Application.MonitoringConfiguration
	if monitoringCfg.S3MonitoringConfiguration != nil && monitoringCfg.S3MonitoringConfiguration.LogUri != nil {
		return *monitoringCfg.S3MonitoringConfiguration.LogUri
	}

	return ""
}

package dataprocserverless

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	dataproc "cloud.google.com/go/dataproc/v2/apiv1"
	"cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"cloud.google.com/go/logging/logadmin"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/storage"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/poll"
	"github.com/bruin-data/bruin/pkg/spark"
	"google.golang.org/protobuf/types/known/durationpb"
)

type batchError struct {
	BatchID string
	Details string
	State   dataprocpb.Batch_State
}

func (e batchError) Error() string {
	switch e.State { //nolint:exhaustive
	case dataprocpb.Batch_FAILED:
		return fmt.Sprintf("batch %s failed: %s", e.BatchID, e.Details)
	case dataprocpb.Batch_CANCELLED:
		return fmt.Sprintf("batch %s was cancelled", e.BatchID)
	default:
		return fmt.Sprintf("batch %s is in an unknown state: %s", e.BatchID, e.State)
	}
}

type JobRunParams struct {
	Project        string
	Region         string
	RuntimeVersion string
	Config         string
	Args           []string
	Timeout        time.Duration
	Workspace      string
	ExecutionRole  string
	SubnetworkURI  string
}

func parseParams(cfg *Client, params map[string]string) *JobRunParams {
	jobParams := JobRunParams{
		Project:        cfg.ProjectID,
		Region:         cfg.Region,
		RuntimeVersion: params["runtime_version"],
		Config:         params["config"],
		Workspace:      cfg.Workspace,
		ExecutionRole:  cfg.ExecutionRole,
		SubnetworkURI:  cfg.SubnetworkURI,
	}

	// default runtime version
	if jobParams.RuntimeVersion == "" {
		jobParams.RuntimeVersion = "2.2"
	}

	if params["timeout"] != "" {
		t, err := time.ParseDuration(params["timeout"])
		if err == nil {
			jobParams.Timeout = t
		}
	}
	if params["args"] != "" {
		arglist := strings.Split(strings.TrimSpace(params["args"]), " ")
		for _, arg := range arglist {
			arg = strings.TrimSpace(arg)
			if arg != "" {
				jobParams.Args = append(jobParams.Args, arg)
			}
		}
	}
	return &jobParams
}

type Job struct {
	logger        *log.Logger
	batchClient   *dataproc.BatchControllerClient
	storageClient *storage.Client
	logClient     *logadmin.Client
	asset         *pipeline.Asset
	pipeline      *pipeline.Pipeline
	params        *JobRunParams
	poll          *poll.Timer
	id            string
	env           map[string]string
}

type workspace struct {
	Root       *url.URL
	Entrypoint string
	Files      string
}

// prepareWorkspace sets up a GCS bucket location for a pyspark job run.
func (job Job) prepareWorkspace(ctx context.Context) (*workspace, error) {
	workspaceURI, err := url.Parse(job.params.Workspace)
	if err != nil {
		return nil, fmt.Errorf("error parsing workspace URL: %w", err)
	}
	jobURI := workspaceURI.JoinPath(job.pipeline.Name, job.id)

	bucket := job.storageClient.Bucket(workspaceURI.Host)

	// Upload the entrypoint script
	scriptPath := job.asset.ExecutableFile.Path
	fd, err := os.Open(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %q: %w", scriptPath, err)
	}
	defer fd.Close()

	scriptKey := strings.TrimPrefix(jobURI.JoinPath(job.asset.ExecutableFile.Name).Path, "/")
	scriptWriter := bucket.Object(scriptKey).NewWriter(ctx)
	if _, err := io.Copy(scriptWriter, fd); err != nil {
		scriptWriter.Close()
		return nil, fmt.Errorf("error uploading entrypoint %q: %w", scriptKey, err)
	}
	if err := scriptWriter.Close(); err != nil {
		return nil, fmt.Errorf("error closing script writer: %w", err)
	}

	// Create and upload context.zip
	tempFile, err := os.CreateTemp("", "bruin-spark-context-*.zip")
	if err != nil {
		return nil, fmt.Errorf("error creating temporary file: %w", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	zipper := zip.NewWriter(tempFile)
	defer zipper.Close()

	repo, err := git.FindRepoFromPath(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("error finding project root: %w", err)
	}

	err = spark.PackageContext(
		zipper,
		os.DirFS(repo.Path),
	)
	if err != nil {
		return nil, fmt.Errorf("error packaging files: %w", err)
	}
	if err := zipper.Close(); err != nil {
		return nil, fmt.Errorf("error closing zip writer: %w", err)
	}
	if _, err := tempFile.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("error rewinding file %q: %w", tempFile.Name(), err)
	}

	contextKey := strings.TrimPrefix(jobURI.JoinPath("context.zip").Path, "/")
	contextWriter := bucket.Object(contextKey).NewWriter(ctx)
	if _, err := io.Copy(contextWriter, tempFile); err != nil {
		contextWriter.Close()
		return nil, fmt.Errorf("error uploading context %q: %w", contextKey, err)
	}
	if err := contextWriter.Close(); err != nil {
		return nil, fmt.Errorf("error closing context writer: %w", err)
	}

	scriptURI := fmt.Sprintf("gs://%s/%s", workspaceURI.Host, scriptKey)
	contextURI := fmt.Sprintf("gs://%s/%s", workspaceURI.Host, contextKey)

	return &workspace{
		Root:       jobURI,
		Entrypoint: scriptURI,
		Files:      contextURI,
	}, nil
}

func (job Job) deleteWorkspace(ws *workspace) {
	if ws == nil || ws.Root == nil {
		return
	}

	ctx := context.Background()
	bucket := job.storageClient.Bucket(ws.Root.Host)
	prefix := strings.TrimPrefix(ws.Root.Path, "/")

	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, context.Canceled) || attrs == nil {
			break
		}
		if err != nil {
			break
		}
		bucket.Object(attrs.Name).Delete(ctx) //nolint
	}
}

func (job Job) buildBatchConfig(ws *workspace) *dataprocpb.CreateBatchRequest {
	pysparkBatch := &dataprocpb.PySparkBatch{
		MainPythonFileUri: ws.Entrypoint,
		PythonFileUris:    []string{ws.Files},
		Args:              job.params.Args,
	}

	// Add environment variables via Spark properties
	sparkProperties := make(map[string]string)
	for key, val := range job.env {
		sparkProperties["spark.executorEnv."+key] = val
		sparkProperties["spark.dataproc.driverEnv."+key] = val
	}

	// Parse additional config from params
	if job.params.Config != "" {
		// Config format: --conf key=value --conf key2=value2
		parts := strings.Split(job.params.Config, "--conf")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			kv := strings.SplitN(part, "=", 2)
			if len(kv) == 2 {
				sparkProperties[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
			}
		}
	}

	batch := &dataprocpb.Batch{
		BatchConfig: &dataprocpb.Batch_PysparkBatch{
			PysparkBatch: pysparkBatch,
		},
		RuntimeConfig: &dataprocpb.RuntimeConfig{
			Version:    job.params.RuntimeVersion,
			Properties: sparkProperties,
		},
		EnvironmentConfig: batchEnvironmentConfig(job.params.ExecutionRole, job.params.Timeout, job.params.SubnetworkURI),
	}

	return &dataprocpb.CreateBatchRequest{
		Parent:  fmt.Sprintf("projects/%s/locations/%s", job.params.Project, job.params.Region),
		BatchId: job.id,
		Batch:   batch,
	}
}

func (job Job) Run(ctx context.Context) (err error) {
	ws, err := job.prepareWorkspace(ctx)
	if err != nil {
		return fmt.Errorf("error preparing workspace: %w", err)
	}
	defer job.deleteWorkspace(ws) //nolint:contextcheck

	job.logger.Printf("uploading workspace to %s", ws.Root.String())

	req := job.buildBatchConfig(ws)
	job.logger.Printf("submitting batch job: %s", req.GetBatchId())

	operation, err := job.batchClient.CreateBatch(ctx, req)
	if err != nil {
		return fmt.Errorf("error submitting batch: %w", err)
	}

	defer func() { //nolint:contextcheck
		if err != nil && !errors.As(err, &batchError{}) {
			job.logger.Printf("error detected. attempting to delete batch.")
			//nolint:errcheck
			job.batchClient.CancelOperation(context.Background(), &longrunningpb.CancelOperationRequest{
				Name: operation.Name(),
			})
		}
	}()

	_, err = operation.Poll(ctx)
	if err != nil {
		return fmt.Errorf("error fetching batch state: %w", err)
	}

	meta, err := operation.Metadata()
	if err != nil {
		return fmt.Errorf("error fetching batch metadata: %w", err)
	}

	job.logger.Printf("created batch: %s", meta.GetBatch())

	var (
		previousState = dataprocpb.Batch_STATE_UNSPECIFIED
		jobLogs       = job.buildLogConsumer(ctx, req.GetBatchId())
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(job.poll.Duration()):
			batch, err := job.batchClient.GetBatch(ctx, &dataprocpb.GetBatchRequest{
				Name: meta.GetBatch(),
			})
			if err != nil {
				job.poll.Increase()
				continue
			}
			job.poll.Reset()

			if previousState != batch.GetState() {
				job.logger.Printf(
					"%s | %s | %s",
					req.GetBatchId(),
					batch.GetState().String(),
					batch.GetStateMessage(),
				)
				previousState = batch.GetState()
			}

			for _, line := range jobLogs.Next() {
				job.logger.Printf("%s | %s", line.Source, line.Message)
			}

			switch batch.GetState() { //nolint:exhaustive
			case dataprocpb.Batch_FAILED, dataprocpb.Batch_CANCELLED:
				return batchError{
					BatchID: req.GetBatchId(),
					State:   batch.GetState(),
					Details: batch.GetStateMessage(),
				}
			case dataprocpb.Batch_SUCCEEDED:
				// Drain remaining logs
				for _, line := range jobLogs.Next() {
					job.logger.Printf("%s | %s", line.Source, line.Message)
				}
				return nil
			}
		}
	}
}

type LogConsumer interface {
	Next() []LogLine
}

func (job Job) buildLogConsumer(ctx context.Context, batchID string) LogConsumer {
	return &CloudLoggingConsumer{
		ctx:       ctx,
		client:    job.logClient,
		project:   job.params.Project,
		region:    job.params.Region,
		batchID:   batchID,
		lastFetch: time.Now(),
	}
}

func batchEnvironmentConfig(role string, timeout time.Duration, subnetworkURI string) *dataprocpb.EnvironmentConfig {
	cfg := &dataprocpb.EnvironmentConfig{
		ExecutionConfig: &dataprocpb.ExecutionConfig{},
	}
	if strings.TrimSpace(role) != "" {
		cfg.ExecutionConfig.ServiceAccount = role
	}
	if timeout != 0 {
		cfg.ExecutionConfig.Ttl = durationpb.New(timeout)
	}
	if strings.TrimSpace(subnetworkURI) != "" {
		cfg.ExecutionConfig.Network = &dataprocpb.ExecutionConfig_SubnetworkUri{
			SubnetworkUri: subnetworkURI,
		}
	}
	return cfg
}

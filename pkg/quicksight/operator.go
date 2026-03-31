package quicksight

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

const (
	defaultRefreshTimeout = 60 * time.Minute
	pollInterval          = 5 * time.Second
)

type BasicOperator struct {
	connection config.ConnectionGetter
}

func NewBasicOperator(conn config.ConnectionGetter) *BasicOperator {
	return &BasicOperator{
		connection: conn,
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return fmt.Errorf("failed to get connection name for asset: %w", err)
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	client, ok := rawConn.(*Client)
	if !ok {
		return fmt.Errorf("connection '%s' is not a quicksight connection", connName)
	}

	if t.Parameters["refresh"] != "true" {
		return nil
	}

	switch t.Type {
	case pipeline.AssetTypeQuicksightDataset:
		return o.handleDatasetRefresh(ctx, client, t)
	case pipeline.AssetTypeQuicksightDashboard:
		return nil
	default:
		return fmt.Errorf("unsupported QuickSight asset type: %s", t.Type)
	}
}

func (o BasicOperator) handleDatasetRefresh(ctx context.Context, client *Client, t *pipeline.Asset) error {
	datasetID, ok := t.Parameters["dataset_id"]
	if !ok || datasetID == "" {
		return errors.New("quicksight.dataset asset requires 'dataset_id' parameter when 'refresh' is true")
	}

	writer := writerFromContext(ctx)

	incremental := resolveIncrementalRefresh(ctx, t.Parameters)
	timeout := resolveRefreshTimeout(t.Parameters)

	ingestionType := qstypes.IngestionTypeFullRefresh
	ingestionLabel := "full"
	if incremental {
		ingestionType = qstypes.IngestionTypeIncrementalRefresh
		ingestionLabel = "incremental"
	}

	ingestionID := fmt.Sprintf("bruin-%s-%d", datasetID, time.Now().Unix())

	fmt.Fprintf(writer, "Starting %s SPICE refresh for dataset '%s'...\n", ingestionLabel, datasetID)

	if err := client.CreateIngestion(ctx, datasetID, ingestionID, ingestionType); err != nil {
		if incremental && isIncrementalNotSupported(err) {
			fmt.Fprintf(writer, "Incremental refresh not supported, retrying with full refresh...\n")
			ingestionID = fmt.Sprintf("bruin-%s-%d-full", datasetID, time.Now().Unix())
			if err := client.CreateIngestion(ctx, datasetID, ingestionID, qstypes.IngestionTypeFullRefresh); err != nil {
				return fmt.Errorf("failed to create full refresh ingestion: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create %s ingestion: %w", ingestionLabel, err)
		}
	}

	fmt.Fprintf(writer, "Ingestion started (id: %s), polling for completion...\n", ingestionID)

	return pollIngestionStatus(ctx, client, datasetID, ingestionID, timeout, writer)
}

func pollIngestionStatus(ctx context.Context, client *Client, datasetID, ingestionID string, timeout time.Duration, writer io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for QuickSight ingestion to complete (dataset: %s, ingestion: %s)", datasetID, ingestionID)
		default:
			status, err := client.DescribeIngestion(ctx, datasetID, ingestionID)
			if err != nil {
				return err
			}

			switch status.Status {
			case "COMPLETED":
				fmt.Fprintf(writer, "SPICE refresh completed successfully.\n")
				return nil
			case "FAILED":
				if status.ErrorMessage != "" {
					return fmt.Errorf("SPICE refresh failed: %s", status.ErrorMessage)
				}
				return fmt.Errorf("SPICE refresh failed (dataset: %s)", datasetID)
			case "CANCELLED":
				return fmt.Errorf("SPICE refresh was cancelled (dataset: %s)", datasetID)
			default:
				// INITIALIZED, QUEUED, RUNNING — keep polling
				time.Sleep(pollInterval)
			}
		}
	}
}

func resolveIncrementalRefresh(ctx context.Context, params map[string]string) bool {
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if fullRefresh {
		return false
	}

	if incremental, ok := getBoolParam(params, "incremental"); ok {
		return incremental
	}

	return true
}

func resolveRefreshTimeout(params map[string]string) time.Duration {
	timeoutMinutes, ok := getIntParam(params, "refresh_timeout_minutes")
	if !ok || timeoutMinutes <= 0 {
		return defaultRefreshTimeout
	}
	return time.Duration(timeoutMinutes) * time.Minute
}

func writerFromContext(ctx context.Context) io.Writer {
	if w := ctx.Value(executor.KeyPrinter); w != nil {
		if wr, ok := w.(io.Writer); ok {
			return wr
		}
	}
	return os.Stdout
}

func isIncrementalNotSupported(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "incremental")
}

func getBoolParam(params map[string]string, key string) (bool, bool) {
	value, ok := params[key]
	if !ok {
		return false, false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return false, false
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, false
	}
	return parsed, true
}

func getIntParam(params map[string]string, key string) (int, bool) {
	value, ok := params[key]
	if !ok {
		return 0, false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

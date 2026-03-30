package quicksight

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
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

	ingestionID := fmt.Sprintf("bruin-%s-%d", datasetID, time.Now().Unix())
	if err := client.CreateIngestion(ctx, datasetID, ingestionID); err != nil {
		return fmt.Errorf("failed to refresh QuickSight dataset: %w", err)
	}

	return nil
}

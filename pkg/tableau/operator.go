package tableau

import (
	"context"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type connectionFetcher interface {
	GetTableauConnectionWithoutDefault(name string) (*Client, error)
	GetConnection(name string) any
}

type BasicOperator struct {
	connection connectionFetcher
}

func NewBasicOperator(conn connectionFetcher) *BasicOperator {
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
		return errors.Wrap(err, "failed to get connection name for asset")
	}

	client, err := o.connection.GetTableauConnectionWithoutDefault(connName)
	if err != nil {
		return errors.Wrap(err, "failed to get Tableau connection")
	}

	if t.Parameters["refresh"] == "" {
		return nil
	}

	switch {
	case t.Type == pipeline.AssetTypeTableauDatasource:
		return o.handleDatasourceRefresh(ctx, client, t)
	case t.Type == pipeline.AssetTypeTableauWorkbook:
		return o.handleWorkbookRefresh(ctx, client, t)
	case t.Type == pipeline.AssetTypeTableauWorksheet || t.Type == pipeline.AssetTypeTableauDashboard:
		return nil
	case t.Type == pipeline.AssetTypeTableau:
		return o.handleWorkbookRefresh(ctx, client, t)

	default:
		return errors.Errorf("unsupported Tableau asset type: %s", t.Type)
	}
}

func (o BasicOperator) handleDatasourceRefresh(ctx context.Context, client *Client, t *pipeline.Asset) error {
	refreshVal, ok := t.Parameters["refresh"]
	if !ok {
		return errors.New("tableau.datasource asset requires 'refresh' parameter (true/false)")
	}

	refresh := refreshVal == "true"

	if refresh {
		datasourceID, ok := t.Parameters["datasource_id"]
		if !ok {
			return errors.New("tableau.datasource asset requires 'datasource_id' parameter when 'refresh' is true")
		}
		if err := client.RefreshDataSource(ctx, datasourceID); err != nil {
			return errors.Wrap(err, "failed to refresh Tableau data source")
		}
	}

	return nil
}

func (o BasicOperator) handleWorkbookRefresh(ctx context.Context, client *Client, t *pipeline.Asset) error {
	refreshVal, ok := t.Parameters["refresh"]
	if !ok {
		return errors.New("tableau.workbook asset requires 'refresh' parameter (true/false)")
	}

	refresh := refreshVal == "true"

	if refresh {
		workbookID, ok := t.Parameters["workbook_id"]
		if !ok {
			return errors.New("tableau.workbook asset requires 'workbook_id' parameter when 'refresh' is true")
		}
		if err := client.RefreshWorksheet(ctx, workbookID); err != nil {
			return errors.Wrap(err, "failed to refresh Tableau workbook")
		}
	}

	return nil
}

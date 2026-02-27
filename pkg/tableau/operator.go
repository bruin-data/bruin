package tableau

import (
	"context"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
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
		return errors.Wrap(err, "failed to get connection name for asset")
	}

	rawConn := o.connection.GetConnection(connName)
	if rawConn == nil {
		return config.NewConnectionNotFoundError(ctx, "", connName)
	}

	client, ok := rawConn.(*Client)
	if !ok {
		return errors.Errorf("connection '%s' is not a tableau connection", connName)
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

// nolint
func (o BasicOperator) handleDatasourceRefresh(ctx context.Context, client *Client, t *pipeline.Asset) error {
	refreshVal, ok := t.Parameters["refresh"]
	refresh := refreshVal == "true"
	// if refresh is false, or if the parameter is not present, we treat
	if !ok || !refresh {
		return nil
	}

	datasourceID, ok := t.Parameters["datasource_id"]
	if !ok || datasourceID == "" {
		name, hasName := t.Parameters["datasource_name"]
		if !hasName || name == "" {
			return errors.New("tableau.datasource asset requires either 'datasource_id' or 'datasource_name' parameter when 'refresh' is true")
		}
		datasources, err := client.GetDatasource(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to list datasources for name lookup")
		}
		id, err := FindDatasourceIDByName(ctx, name, datasources)
		if err != nil {
			return errors.Wrapf(err, "failed to find datasource id for name '%s'", name)
		}
		if id == "" {
			return errors.Errorf("no datasource found with name '%s'", name)
		}
		datasourceID = id
	}
	incremental := resolveIncrementalRefresh(ctx, t.Parameters)
	if err := client.RefreshDataSource(ctx, datasourceID, incremental); err != nil {
		return errors.Wrap(err, "failed to refresh Tableau data source")
	}

	return nil
}

// nolint
func (o BasicOperator) handleWorkbookRefresh(ctx context.Context, client *Client, t *pipeline.Asset) error {
	refreshVal, ok := t.Parameters["refresh"]
	refresh := refreshVal == "true"
	// if refresh is false, or if the parameter is not present, we treat
	if !ok || !refresh {
		return nil
	}

	workbookID, ok := t.Parameters["workbook_id"]
	if !ok || workbookID == "" {
		name, hasName := t.Parameters["workbook_name"]
		if !hasName || name == "" {
			return errors.New("tableau.workbook asset requires either 'workbook_id' or 'workbook_name' parameter when 'refresh' is true")
		}
		workbooks, err := client.GetWorkbooks(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to list workbooks for name lookup")
		}
		id, err := FindWorkbookIDByName(ctx, name, workbooks)
		if err != nil {
			return errors.Wrapf(err, "failed to find workbook id for name '%s'", name)
		}
		if id == "" {
			return errors.Errorf("no workbook found with name '%s'", name)
		}
		workbookID = id
	}
	incremental := resolveIncrementalRefresh(ctx, t.Parameters)
	if err := client.RefreshWorksheet(ctx, workbookID, incremental); err != nil {
		return errors.Wrap(err, "failed to refresh Tableau workbook")
	}

	return nil
}

func resolveIncrementalRefresh(ctx context.Context, params map[string]string) bool {
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if fullRefresh {
		return false
	}

	if incremental, ok := getBoolParam(params, "incremental"); ok {
		return incremental
	}

	// Default to incremental refresh for extract updates.
	return true
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

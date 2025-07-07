package tableau

import (
	"context"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type connectionFetcher interface {
	GetTableauConnectionWithoutDefault(name string) (*Client, error)
	GetConnection(name string) (interface{}, error)
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

	datasourceID, ok := t.Parameters["datasource_id"]
	if !ok {
		return errors.New("tableau asset requires 'datasource_id' parameter")
	}

	err = client.RefreshDataSource(ctx, datasourceID)
	if err != nil {
		return errors.Wrap(err, "failed to refresh Tableau data source")
	}

	return nil
}

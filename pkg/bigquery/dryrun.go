package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type connectionGetter interface {
	GetConnection(name string) any
}

type queryExtractor interface {
	ExtractQueriesFromString(filepath string) ([]*query.Query, error)
}

type DryRunnerQuerier interface {
	QueryDryRun(ctx context.Context, queryObj *query.Query) (*bigquery.QueryStatistics, error)
}

type DryRunner struct {
	ConnectionGetter connectionGetter
	QueryExtractor   queryExtractor
}

func (r *DryRunner) DryRun(ctx context.Context, p pipeline.Pipeline, a pipeline.Asset, c *config.Config) (map[string]any, error) {
	if a.Type != pipeline.AssetTypeBigqueryQuery && a.Type != pipeline.AssetTypeBigqueryQuerySensor {
		return nil, errors.New("asset-metadata is only available for BigQuery SQL assets and BigQuery query sensors")
	}

	var queryString string
	var err error

	if a.Type == pipeline.AssetTypeBigqueryQuery {
		queryString = a.ExecutableFile.Content
	} else if a.Type == pipeline.AssetTypeBigqueryQuerySensor {
		queryParam, ok := a.Parameters["query"]
		if !ok {
			return nil, errors.New("query sensor requires a parameter named 'query'")
		}
		queryString = queryParam
	}

	queries, err := r.QueryExtractor.ExtractQueriesFromString(queryString)
	if err != nil || len(queries) == 0 {
		if err == nil {
			err = errors.New("no query found in asset")
		}
		return nil, err
	}
	q := queries[0]

	connName, err := p.GetConnectionNameForAsset(&a)
	if err != nil {
		return nil, err
	}

	bqClient, _ := r.ConnectionGetter.GetConnection(connName).(DryRunnerQuerier)
	if bqClient == nil {
		return nil, errors.New("resolved connection is not BigQuery")
	}

	meta, err := bqClient.QueryDryRun(ctx, q)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"bigquery": meta,
	}, nil
}

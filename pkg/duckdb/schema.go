//go:build !bruin_no_duckdb

package duck

import (
	"context"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type DuckDBSchemaCreator struct {
	schemaNameCache *sync.Map
}

func NewDuckDBSchemaCreator() *DuckDBSchemaCreator {
	return &DuckDBSchemaCreator{
		schemaNameCache: &sync.Map{},
	}
}

type queryRunner interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
}

func (sc *DuckDBSchemaCreator) CreateSchemaIfNotExist(ctx context.Context, qr queryRunner, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToLower(tableComponents[0])
	case 3:
		schemaName = strings.ToLower(tableComponents[1])
	default:
		return nil
	}
	// Check the cache for the schema
	if _, exists := sc.schemaNameCache.Load(schemaName); exists {
		return nil
	}
	createQuery := query.Query{
		Query: "CREATE SCHEMA IF NOT EXISTS " + schemaName,
	}
	if err := qr.RunQueryWithoutResult(ctx, &createQuery); err != nil {
		return errors.Wrapf(err, "failed to create or ensure schema: %s", schemaName)
	}
	sc.schemaNameCache.Store(schemaName, true)

	return nil
}

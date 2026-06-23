package ansisql

import (
	"context"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/tablename"
	"github.com/pkg/errors"
)

type SchemaCreator struct {
	schemaNameCache *sync.Map
}

func NewSchemaCreator() *SchemaCreator {
	return &SchemaCreator{
		schemaNameCache: &sync.Map{},
	}
}

type queryRunner interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
}

func (sc *SchemaCreator) CreateSchemaIfNotExist(ctx context.Context, qr queryRunner, asset *pipeline.Asset) error {
	// Three-part names are `database.schema.table` (Snowflake) or
	// `catalog.schema.table` (Databricks). SchemaToCreate qualifies the schema
	// with the first component so it is created in the database/catalog named in
	// the asset rather than the connection's default. The qualifier is also part
	// of the cache key, so the same schema name in two different databases is not
	// deduped into a single creation.
	schemaName, ok := tablename.SchemaToCreate(asset.Name, strings.ToUpper)
	if !ok {
		return nil
	}
	// Check the cache for the database
	if _, exists := sc.schemaNameCache.Load(schemaName); exists {
		return nil
	}
	createQuery := query.Query{
		Query: "CREATE SCHEMA IF NOT EXISTS " + schemaName,
	}
	if err := qr.RunQueryWithoutResult(ctx, &createQuery); err != nil {
		return errors.Wrapf(err, "failed to create or ensure database: %s", schemaName)
	}
	sc.schemaNameCache.Store(schemaName, true)

	return nil
}

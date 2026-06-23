package ansisql

import (
	"context"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
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
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToUpper(tableComponents[0])
	case 3:
		// Three-part names are `database.schema.table` (Snowflake) or
		// `catalog.schema.table` (Databricks). Qualify the schema with the
		// first component so it is created in the database/catalog named in the
		// asset rather than the connection's default database. Keeping the
		// qualifier in the cache key also stops the same schema name in two
		// different databases from being deduped into a single creation.
		schemaName = strings.ToUpper(tableComponents[0]) + "." + strings.ToUpper(tableComponents[1])
	default:
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

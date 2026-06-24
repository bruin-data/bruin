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
	containerCache  *sync.Map
	// containerKeyword, when set (e.g. "DATABASE" for Snowflake, "CATALOG" for
	// Databricks), makes the creator also ensure the parent database/catalog of a
	// three-part name exists via `CREATE <keyword> IF NOT EXISTS <name>` before
	// creating the schema. Empty disables parent creation (the default), for
	// platforms whose top-level container cannot be created with SQL.
	containerKeyword string
}

func NewSchemaCreator() *SchemaCreator {
	return &SchemaCreator{
		schemaNameCache: &sync.Map{},
		containerCache:  &sync.Map{},
	}
}

// NewSchemaCreatorWithContainer returns a SchemaCreator that, for three-part
// names, also auto-creates the parent database/catalog using the given DDL
// keyword (e.g. "DATABASE" or "CATALOG").
func NewSchemaCreatorWithContainer(containerKeyword string) *SchemaCreator {
	return &SchemaCreator{
		schemaNameCache:  &sync.Map{},
		containerCache:   &sync.Map{},
		containerKeyword: containerKeyword,
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

	// For three-part names, ensure the parent database/catalog exists first so
	// the qualified CREATE SCHEMA below has somewhere to land. Only done for
	// platforms that declared a container keyword.
	if sc.containerKeyword != "" {
		if container, hasContainer := tablename.ContainerToCreate(asset.Name, strings.ToUpper); hasContainer {
			if _, exists := sc.containerCache.Load(container); !exists {
				createContainer := query.Query{
					Query: "CREATE " + sc.containerKeyword + " IF NOT EXISTS " + container,
				}
				if err := qr.RunQueryWithoutResult(ctx, &createContainer); err != nil {
					return errors.Wrapf(err, "failed to create or ensure %s: %s", strings.ToLower(sc.containerKeyword), container)
				}
				sc.containerCache.Store(container, true)
			}
		}
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

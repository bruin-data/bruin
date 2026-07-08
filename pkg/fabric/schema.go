package fabric

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
)

type SchemaCreator struct {
	schemaNameCache *sync.Map
	currentDatabase string
}

type schemaIdentifier struct {
	catalog string
	schema  string
}

type schemaQueryRunner interface {
	RunQueryWithoutResult(ctx context.Context, q *query.Query) error
}

func NewSchemaCreator(currentDatabase ...string) *SchemaCreator {
	creator := &SchemaCreator{schemaNameCache: &sync.Map{}}
	if len(currentDatabase) > 0 {
		creator.currentDatabase = currentDatabase[0]
	}

	return creator
}

func (sc *SchemaCreator) CreateSchemaIfNotExist(ctx context.Context, qr schemaQueryRunner, asset *pipeline.Asset) error {
	schemaName, ok := schemaNameToCreate(asset.Name)
	if !ok {
		return nil
	}

	if schemaName.catalog != "" {
		if sc.currentDatabase == "" {
			return errors.Errorf("cannot create Fabric schema %s without a configured connection database", schemaName.cacheKey())
		}
		if !strings.EqualFold(schemaName.catalog, sc.currentDatabase) {
			return errors.Errorf(
				"cannot create Fabric schema %s while connected to warehouse %s",
				schemaName.cacheKey(),
				sc.currentDatabase,
			)
		}
	}

	cacheKey := schemaName.cacheKey()
	if _, exists := sc.schemaNameCache.Load(cacheKey); exists {
		return nil
	}

	createQuery := query.Query{Query: buildCreateSchemaQuery(schemaName)}
	if err := qr.RunQueryWithoutResult(ctx, &createQuery); err != nil {
		return errors.Wrapf(err, "failed to create or ensure schema: %s", cacheKey)
	}
	sc.schemaNameCache.Store(cacheKey, true)

	return nil
}

func schemaNameToCreate(assetName string) (schemaIdentifier, bool) {
	parts := strings.Split(assetName, ".")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return schemaIdentifier{}, false
		}
	}

	switch len(parts) {
	case 2:
		return schemaIdentifier{schema: parts[0]}, true
	case 3:
		return schemaIdentifier{catalog: parts[0], schema: parts[1]}, true
	default:
		return schemaIdentifier{}, false
	}
}

func (s schemaIdentifier) cacheKey() string {
	if s.catalog == "" {
		return s.schema
	}

	return s.catalog + "." + s.schema
}

func buildCreateSchemaQuery(schemaName schemaIdentifier) string {
	return fmt.Sprintf(
		"IF SCHEMA_ID(%s) IS NULL\n    EXEC(N'CREATE SCHEMA %s')",
		sqlStringLiteral(schemaName.schema),
		strings.ReplaceAll(QuoteIdentifier(schemaName.schema), "'", "''"),
	)
}

func sqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

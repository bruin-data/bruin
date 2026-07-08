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
}

type schemaQueryRunner interface {
	RunQueryWithoutResult(ctx context.Context, q *query.Query) error
}

func NewSchemaCreator() *SchemaCreator {
	return &SchemaCreator{schemaNameCache: &sync.Map{}}
}

func (sc *SchemaCreator) CreateSchemaIfNotExist(ctx context.Context, qr schemaQueryRunner, asset *pipeline.Asset) error {
	schemaName, ok := schemaNameToCreate(asset.Name)
	if !ok {
		return nil
	}

	if _, exists := sc.schemaNameCache.Load(schemaName); exists {
		return nil
	}

	createQuery := query.Query{Query: buildCreateSchemaQuery(schemaName)}
	if err := qr.RunQueryWithoutResult(ctx, &createQuery); err != nil {
		return errors.Wrapf(err, "failed to create or ensure schema: %s", schemaName)
	}
	sc.schemaNameCache.Store(schemaName, true)

	return nil
}

func schemaNameToCreate(assetName string) (string, bool) {
	parts := strings.Split(assetName, ".")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", false
		}
	}

	switch len(parts) {
	case 2:
		return parts[0], true
	case 3:
		return parts[1], true
	default:
		return "", false
	}
}

func buildCreateSchemaQuery(schemaName string) string {
	return fmt.Sprintf(
		"IF SCHEMA_ID(%s) IS NULL\n    EXEC(N'CREATE SCHEMA %s')",
		sqlStringLiteral(schemaName),
		strings.ReplaceAll(QuoteIdentifier(schemaName), "'", "''"),
	)
}

func sqlStringLiteral(value string) string {
	return "N'" + strings.ReplaceAll(value, "'", "''") + "'"
}

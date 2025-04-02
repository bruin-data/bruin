package devenv

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

type DevEnvQueryModifier struct {
	Dialect string
	Conn    connectionFetcher
	Parser  parser

	connSchemaCache     map[string]*ansisql.DBDatabase
	connSchemaCacheLock sync.Mutex
}

type connectionFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type parser interface {
	UsedTables(sql, dialect string) ([]string, error)
	RenameTables(sql, dialect string, tableMapping map[string]string) (string, error)
}

func (d *DevEnvQueryModifier) Modify(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) (*query.Query, error) {
	var err error

	env, ok := ctx.Value(config.EnvironmentContextKey).(*config.Environment)
	if !ok || env == nil {
		return q, nil
	}

	if env.SchemaPrefix == "" {
		return q, nil
	}

	assetName := a.Name
	assetNameParts := strings.Split(assetName, ".")
	if len(assetNameParts) != 2 {
		return q, nil
	}

	var wg sync.WaitGroup
	var usedTables []string

	connName, err := p.GetConnectionNameForAsset(a)
	if err != nil {
		return nil, err
	}

	conn, err := d.Conn.GetConnection(connName)
	if err != nil {
		return nil, err
	}

	dbFetcherConn, ok := conn.(interface {
		GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error)
	})
	if !ok {
		return nil, fmt.Errorf("the asset type '%s' does not support developer environments, please create an issue if you'd like that", a.Type)
	}

	var usedTablesError error
	wg.Add(1)
	go func() {
		defer wg.Done()
		usedTables, usedTablesError = d.Parser.UsedTables(q.Query, "postgres")
	}()

	var dbSummary *ansisql.DBDatabase
	var dbSummaryErr error

	d.connSchemaCacheLock.Lock()
	if d.connSchemaCache == nil {
		d.connSchemaCache = make(map[string]*ansisql.DBDatabase)

		wg.Add(1)
		go func() {
			defer wg.Done()
			dbSummary, dbSummaryErr = dbFetcherConn.GetDatabaseSummary(ctx)
			d.connSchemaCache[connName] = dbSummary
			d.connSchemaCacheLock.Unlock()
		}()
	} else {
		dbSummary = d.connSchemaCache[connName]
		d.connSchemaCacheLock.Unlock()
	}

	wg.Wait()
	if usedTablesError != nil {
		return nil, usedTablesError
	}

	if dbSummaryErr != nil {
		return nil, dbSummaryErr
	}

	renameMapping := map[string]string{}

	// we modify the asset names globally outside of this function.
	// this means if for some case the asset query queries itself, it would be cool if we handle that as well.
	// this conditional tries to do that.
	if strings.HasPrefix(assetNameParts[0], env.SchemaPrefix) {
		originalAssetName := strings.TrimPrefix(assetName, env.SchemaPrefix)
		renameMapping[originalAssetName] = assetName
	}

	for _, tableReference := range usedTables {
		parts := strings.Split(tableReference, ".")
		if len(parts) != 2 {
			continue
		}
		schema := parts[0]
		table := parts[1]
		devSchema := env.SchemaPrefix + schema
		devTable := fmt.Sprintf("%s.%s", devSchema, table)

		if dbSummary.TableExists(devSchema, table) {
			renameMapping[tableReference] = devTable
		}
	}

	q.Query, err = d.Parser.RenameTables(q.Query, d.Dialect, renameMapping)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (d *DevEnvQueryModifier) RegisterAssetForSchemaCache(ctx context.Context, p *pipeline.Pipeline, a *pipeline.Asset, q *query.Query) error {
	d.connSchemaCacheLock.Lock()
	defer d.connSchemaCacheLock.Unlock()

	if d.connSchemaCache == nil {
		d.connSchemaCache = make(map[string]*ansisql.DBDatabase)
	}

	connName, err := p.GetConnectionNameForAsset(a)
	if err != nil {
		return nil
	}

	summary, ok := d.connSchemaCache[connName]
	if !ok {
		return nil
	}

	assetNameParts := strings.Split(a.Name, ".")
	if len(assetNameParts) != 2 {
		return nil
	}

	assetSchema := assetNameParts[0]
	assetTable := assetNameParts[1]

	schemaIndex := -1
	for i, schema := range summary.Schemas {
		if !strings.EqualFold(schema.Name, assetSchema) {
			continue
		}

		schemaIndex = i
		for _, table := range schema.Tables {
			if strings.EqualFold(table.Name, assetTable) {
				return nil
			}
		}
	}

	if schemaIndex == -1 {
		summary.Schemas = append(summary.Schemas, &ansisql.DBSchema{
			Name:   assetSchema,
			Tables: []*ansisql.DBTable{},
		})
		schemaIndex = len(summary.Schemas) - 1
	}

	summary.Schemas[schemaIndex].Tables = append(summary.Schemas[schemaIndex].Tables, &ansisql.DBTable{
		Name: assetTable,
	})

	return nil
}

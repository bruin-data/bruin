package devenv

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

type DevEnvQueryModifier struct {
	Dialect string
	Conn    config.ConnectionGetter
	Parser  parser

	connSchemaCache     map[string]*ansisql.DBDatabase
	connSchemaCacheLock sync.Mutex
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
	if len(assetNameParts) != 2 && len(assetNameParts) != 3 {
		return q, nil
	}

	var wg sync.WaitGroup
	var usedTables []string

	connName, err := p.GetConnectionNameForAsset(a)
	if err != nil {
		return nil, err
	}

	conn := d.Conn.GetConnection(connName)
	if conn == nil {
		return nil, config.NewConnectionNotFoundError(ctx, "", connName)
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
		usedTables, usedTablesError = d.Parser.UsedTables(q.Query, d.Dialect)
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

	crossCatalogTables := make([]string, 0)
	seenCrossCatalogTables := make(map[string]bool)
	for _, tableReference := range usedTables {
		parts := strings.Split(tableReference, ".")
		if len(parts) != 3 || strings.EqualFold(parts[0], dbSummary.Name) {
			continue
		}
		devSchema := env.SchemaPrefix + parts[1]
		if d.databaseSummaryTableExists(dbSummary, parts[0], devSchema, parts[2]) {
			continue
		}
		devTable := fmt.Sprintf("%s.%s.%s", parts[0], devSchema, parts[2])
		if !seenCrossCatalogTables[devTable] {
			crossCatalogTables = append(crossCatalogTables, devTable)
			seenCrossCatalogTables[devTable] = true
		}
	}

	var crossCatalogTableExists map[string]bool
	batchChecker, batchSupported := conn.(interface {
		TablesExist(ctx context.Context, tableNames []string) (map[string]bool, error)
	})
	if batchSupported && len(crossCatalogTables) > 0 {
		crossCatalogTableExists, err = batchChecker.TablesExist(ctx, crossCatalogTables)
		if err != nil {
			return nil, err
		}
	}

	renameMapping := map[string]string{}

	// we modify the asset names globally outside of this function.
	// this means if for some case the asset query queries itself, it would be cool if we handle that as well.
	// this conditional tries to do that.
	assetSchemaIndex := len(assetNameParts) - 2
	if strings.HasPrefix(assetNameParts[assetSchemaIndex], env.SchemaPrefix) {
		originalAssetNameParts := append([]string(nil), assetNameParts...)
		originalAssetNameParts[assetSchemaIndex] = strings.TrimPrefix(
			originalAssetNameParts[assetSchemaIndex],
			env.SchemaPrefix,
		)
		renameMapping[strings.Join(originalAssetNameParts, ".")] = assetName
	}

	for _, tableReference := range usedTables {
		parts := strings.Split(tableReference, ".")

		switch len(parts) {
		case 2:
			// schema.table -> dev_schema.table
			schema := parts[0]
			table := parts[1]
			devSchema := env.SchemaPrefix + schema
			devTable := fmt.Sprintf("%s.%s", devSchema, table)

			if d.databaseSummaryTableExists(dbSummary, dbSummary.Name, devSchema, table) {
				renameMapping[tableReference] = devTable
			}
		case 3:
			// database.schema.table -> database.dev_schema.table
			database := parts[0]
			schema := parts[1]
			table := parts[2]

			devSchema := env.SchemaPrefix + schema
			devTable := fmt.Sprintf("%s.%s.%s", database, devSchema, table)

			tableExists := d.databaseSummaryTableExists(dbSummary, database, devSchema, table)
			if !strings.EqualFold(database, dbSummary.Name) {
				if batchSupported {
					if batchResult, checked := crossCatalogTableExists[devTable]; checked {
						tableExists = batchResult
					}
				} else {
					tableExists, err = tableExistsInDatabase(ctx, conn, devTable)
					if err != nil {
						return nil, err
					}
				}
			}

			if tableExists {
				renameMapping[tableReference] = devTable
			}
		default:
			continue
		}
	}

	q.Query, err = d.Parser.RenameTables(q.Query, d.Dialect, renameMapping)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (d *DevEnvQueryModifier) databaseSummaryTableExists(
	database *ansisql.DBDatabase,
	catalog,
	schema,
	table string,
) bool {
	useUnqualifiedSchema := catalog == "" || strings.EqualFold(catalog, database.Name)
	if useUnqualifiedSchema && database.TableExists(schema, table) {
		return true
	}
	if catalog != "" && database.TableExists(catalog+"."+schema, table) {
		return true
	}
	if !strings.EqualFold(d.Dialect, "spark") {
		return false
	}

	schemaNames := make([]string, 0, 2)
	if useUnqualifiedSchema {
		schemaNames = append(schemaNames, schema)
	}
	if catalog != "" {
		schemaNames = append(schemaNames, catalog+"."+schema)
	}
	for _, existingSchema := range database.Schemas {
		schemaMatches := false
		for _, schemaName := range schemaNames {
			if strings.EqualFold(existingSchema.Name, schemaName) {
				schemaMatches = true
				break
			}
		}
		if !schemaMatches {
			continue
		}
		for _, existingTable := range existingSchema.Tables {
			if strings.EqualFold(existingTable.Name, table) {
				return true
			}
		}
	}
	return false
}

func tableExistsInDatabase(ctx context.Context, conn any, tableName string) (bool, error) {
	if metadataChecker, ok := conn.(interface {
		TableExists(ctx context.Context, tableName string) (bool, error)
	}); ok {
		return metadataChecker.TableExists(ctx, tableName)
	}

	tableChecker, ok := conn.(ansisql.TableExistsChecker)
	if !ok {
		return false, nil
	}

	tableExistsQuery, err := tableChecker.BuildTableExistsQuery(tableName)
	if err != nil {
		return false, fmt.Errorf("failed to build existence check for developer environment table '%s': %w", tableName, err)
	}

	result, err := tableChecker.Select(ctx, &query.Query{Query: tableExistsQuery})
	if err != nil {
		return false, fmt.Errorf("failed to check developer environment table '%s': %w", tableName, err)
	}

	count, err := helpers.CastResultToInteger(result, true)
	if err != nil {
		return false, fmt.Errorf("failed to parse existence check for developer environment table '%s': %w", tableName, err)
	}

	return count > 0, nil
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
	if len(assetNameParts) != 2 && len(assetNameParts) != 3 {
		return nil
	}

	assetCatalog := summary.Name
	if len(assetNameParts) == 3 {
		assetCatalog = assetNameParts[0]
	}
	assetSchemaName := assetNameParts[len(assetNameParts)-2]
	assetSchema := assetCatalog + "." + assetSchemaName
	if len(assetNameParts) == 2 || strings.EqualFold(assetCatalog, summary.Name) {
		assetSchema = databaseSummarySchemaName(summary, assetCatalog, assetSchemaName)
	}
	assetTable := assetNameParts[len(assetNameParts)-1]

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

func databaseSummarySchemaName(database *ansisql.DBDatabase, catalog, schema string) string {
	qualifiedSchema := catalog + "." + schema
	for _, existingSchema := range database.Schemas {
		if strings.EqualFold(existingSchema.Name, schema) ||
			(catalog != "" && strings.EqualFold(existingSchema.Name, qualifiedSchema)) {
			return existingSchema.Name
		}
	}
	if catalog != "" {
		catalogPrefix := strings.ToLower(catalog) + "."
		for _, existingSchema := range database.Schemas {
			if strings.HasPrefix(strings.ToLower(existingSchema.Name), catalogPrefix) {
				return qualifiedSchema
			}
		}
	}
	return schema
}

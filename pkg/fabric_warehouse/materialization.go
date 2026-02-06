package fabricwarehouse

import (
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var matMap = pipeline.AssetMaterializationMap{
	pipeline.MaterializationTypeView: {
		pipeline.MaterializationStrategyNone:          viewMaterializer,
		pipeline.MaterializationStrategyAppend:        errorMaterializer,
		pipeline.MaterializationStrategyCreateReplace: viewMaterializer,
		pipeline.MaterializationStrategyDeleteInsert:  errorMaterializer,
	},
	pipeline.MaterializationTypeTable: {
		pipeline.MaterializationStrategyNone:           buildCreateReplaceQuery,
		pipeline.MaterializationStrategyAppend:         buildAppendQuery,
		pipeline.MaterializationStrategyCreateReplace:  buildCreateReplaceQuery,
		pipeline.MaterializationStrategyDeleteInsert:   buildDeleteInsertQuery,
		pipeline.MaterializationStrategyMerge:          buildMergeQuery,
		pipeline.MaterializationStrategyTruncateInsert: buildTruncateInsertQuery,
	},
}

func NewMaterializer(fullRefresh bool) *pipeline.Materializer {
	return &pipeline.Materializer{
		MaterializationMap: matMap,
		FullRefresh:        fullRefresh,
	}
}

func errorMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "", fmt.Errorf("materialization strategy %s is not supported for materialization type %s and asset type %s", asset.Materialization.Strategy, asset.Materialization.Type, asset.Type)
}

func viewMaterializer(asset *pipeline.Asset, query string) (string, error) {
	return "CREATE OR ALTER VIEW " + QuoteIdentifier(asset.Name) + " AS\n" + strings.TrimSuffix(query, ";"), nil
}

// buildCreateReplaceQuery uses a temp table swap pattern because Fabric doesn't support CREATE OR REPLACE TABLE.
func buildCreateReplaceQuery(asset *pipeline.Asset, query string) (string, error) {
	_, baseName := splitSchemaName(asset.Name)
	targetFullName := asset.Name
	tempFullName := asset.Name + "__bruin_tmp"
	backupFullName := asset.Name + "__bruin_backup"

	backupBase := baseName + "__bruin_backup"
	targetBase := baseName

	tempName := QuoteIdentifier(tempFullName)
	backupName := QuoteIdentifier(backupFullName)

	query = strings.TrimSuffix(query, ";")

	queries := []string{
		"DROP TABLE IF EXISTS " + tempName,
		"DROP TABLE IF EXISTS " + backupName,
		"SELECT * INTO " + tempName + " FROM (\n" + query + "\n) AS __bruin_src",
		"IF OBJECT_ID('" + targetFullName + "', 'U') IS NOT NULL BEGIN EXEC sp_rename '" + targetFullName + "', '" + backupBase + "' END",
		"EXEC sp_rename '" + tempFullName + "', '" + targetBase + "'",
		"DROP TABLE IF EXISTS " + backupName,
	}

	return strings.Join(queries, ";\n") + ";", nil
}

func buildAppendQuery(asset *pipeline.Asset, query string) (string, error) {
	return "INSERT INTO " + QuoteIdentifier(asset.Name) + "\n" + strings.TrimSuffix(query, ";"), nil
}

func buildDeleteInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	if len(asset.Columns) == 0 {
		return "", errors.New("delete+insert strategy requires columns to be defined")
	}

	pkCols := getPrimaryKeyColumns(asset)
	if len(pkCols) == 0 {
		return "", errors.New("delete+insert strategy requires primary key columns")
	}

	tableName := QuoteIdentifier(asset.Name)
	tempName := QuoteIdentifier(asset.Name + "__bruin_tmp")

	deleteCondition := buildDeleteCondition(pkCols, tableName, tempName)
	query = strings.TrimSuffix(query, ";")

	queries := []string{
		"DROP TABLE IF EXISTS " + tempName,
		"SELECT * INTO " + tempName + " FROM (\n" + query + "\n) AS __bruin_src",
		"DELETE FROM " + tableName + " WHERE EXISTS (\n  SELECT 1 FROM " + tempName + " WHERE " + deleteCondition + "\n)",
		"INSERT INTO " + tableName + " SELECT * FROM " + tempName,
		"DROP TABLE IF EXISTS " + tempName,
	}

	return strings.Join(queries, ";\n") + ";", nil
}

// buildMergeQuery falls back to delete+insert because Fabric MERGE is limited.
func buildMergeQuery(asset *pipeline.Asset, query string) (string, error) {
	return buildDeleteInsertQuery(asset, query)
}

func buildTruncateInsertQuery(asset *pipeline.Asset, query string) (string, error) {
	queries := []string{
		"TRUNCATE TABLE " + QuoteIdentifier(asset.Name),
		"INSERT INTO " + QuoteIdentifier(asset.Name) + "\n" + strings.TrimSuffix(query, ";"),
	}
	return strings.Join(queries, ";\n") + ";", nil
}

func getPrimaryKeyColumns(asset *pipeline.Asset) []string {
	var pkCols []string
	for _, col := range asset.Columns {
		if col.PrimaryKey {
			pkCols = append(pkCols, col.Name)
		}
	}
	return pkCols
}

func buildDeleteCondition(pkCols []string, targetAlias, sourceAlias string) string {
	conditions := make([]string, len(pkCols))
	for i, col := range pkCols {
		conditions[i] = fmt.Sprintf("%s.%s = %s.%s", targetAlias, QuoteIdentifier(col), sourceAlias, QuoteIdentifier(col))
	}
	return strings.Join(conditions, " AND ")
}

func splitSchemaName(name string) (string, string) {
	if idx := strings.LastIndex(name, "."); idx != -1 {
		return name[:idx], name[idx+1:]
	}

	return "", name
}

package executor

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type Config map[scheduler.TaskInstanceType]Operator

var DefaultExecutorsV2 = map[pipeline.AssetType]Config{
	pipeline.AssetTypeBigqueryQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeBigqueryTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeBigqueryQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeBigquerySource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypePostgresSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeSynapseSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeAthenaSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeClickHouseSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeBigquerySeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"gcs.sensor.object_sensor_with_prefix": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"gcs.sensor.object": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"dbt": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"dbt.test": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeEmpty: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePostgresQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMySQLQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMySQLSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMySQLQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMySQLTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouse: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouseSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouseQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouseTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePostgresSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePostgresQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeFabricWarehouseQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeFabricWarehouseSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeFabricWarehouseQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeFabricWarehouseTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaSQLSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePython: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeR: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"python.beta": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"python.legacy": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeS3KeySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeSeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"appsflyer.export.bq": {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeIngestr: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTableau: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeEMRServerlessSpark: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeEMRServerlessPyspark: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDataprocServerlessPyspark: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeLooker: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeLookerStudio: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePowerBI: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeQlikSense: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeQlikView: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSisense: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeDomo: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeQuicksight: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeMetabase: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeGrafana: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeSuperset: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeModeBI: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeRedash: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeGoodData: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTableauDatasource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTableauWorkbook: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTableauWorksheet: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTableauDashboard: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypeTrinoQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeTrinoQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeOracleQuery: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeOracleSource: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypePostgresTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeAthenaTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftTableSensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeAgentClaudeCode: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
}

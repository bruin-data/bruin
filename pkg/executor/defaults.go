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
	pipeline.AssetTypeBigquerySeed: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	"gcs.sensor.object_sensor_with_prefix": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"gcs.sensor.object": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"dbt": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"dbt.test": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeEmpty: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
	},
	pipeline.AssetTypePostgresQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouse: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeClickHouseSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypePostgresSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypePostgresQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftQuerySensor: {
		scheduler.TaskInstanceTypeMain:         NoOpOperator{},
		scheduler.TaskInstanceTypeMetadataPush: NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck:  NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck:  NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeDatabricksSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeAthenaSQLSensor: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeDuckDBSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypePython: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
	},
	"python.beta": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"python.legacy": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"s3.sensor.key_sensor": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeQuery: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeQuerySensor: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeSnowflakeSeed: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	"appsflyer.export.bq": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeIngestr: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeTableau: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeEMRServerlessSpark: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeEMRServerlessPyspark: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
}

package executor

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

type Config map[scheduler.TaskInstanceType]Operator

var DefaultExecutorsV2 = map[pipeline.AssetType]Config{
	pipeline.AssetTypeBigqueryQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
	},
	pipeline.AssetTypeBigqueryTableSensor: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"bq.sensor.query": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"bq.cost_tracker": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"bq.transfer": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"bq.sensor.partition": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"gcs.from.s3": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"gcs.delete": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"gcs.sensor.object_sensor_with_prefix": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"gcs.sensor.object": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeEmpty: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypePostgresQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeRedshiftQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeMsSQLQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
		scheduler.TaskInstanceTypeCustomCheck: NoOpOperator{},
	},
	pipeline.AssetTypeSynapseQuery: {
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
	"appsflyer.export.bq": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	pipeline.AssetTypeIngestr: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
}

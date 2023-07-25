package executor

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

const (
	TaskTypePython         = pipeline.AssetType("python")
	TaskTypeSnowflakeQuery = pipeline.AssetType("sf.sql")
	TaskTypeBigqueryQuery  = pipeline.AssetType("bq.sql")
	TaskTypeEmpty          = pipeline.AssetType("empty")
)

type Config map[scheduler.TaskInstanceType]Operator

var DefaultExecutorsV2 = map[pipeline.AssetType]Config{
	TaskTypeBigqueryQuery: {
		scheduler.TaskInstanceTypeMain:        NoOpOperator{},
		scheduler.TaskInstanceTypeColumnCheck: NoOpOperator{},
	},
	"bq.sensor.table": {
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
	TaskTypeEmpty: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"athena.sql": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"athena.sensor.query": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	TaskTypePython: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
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
	TaskTypeSnowflakeQuery: {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"adjust.export.bq": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
	"appsflyer.export.bq": {
		scheduler.TaskInstanceTypeMain: NoOpOperator{},
	},
}

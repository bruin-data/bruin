package cmd

import (
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/ingestr"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Not parallel: SetupExecutors mutates the shared executor.DefaultExecutorsV2.
func TestSetupExecutorsRoutesInferenceChecksToTheWarehouse(t *testing.T) { //nolint:paralleltest
	asset := &pipeline.Asset{
		Name:       "analytics.enriched",
		Type:       pipeline.AssetTypeInference,
		Connection: "warehouse",
		Parameters: pipeline.ParameterMap{"destination": "postgres"},
		Columns: []pipeline.Column{
			{Name: "id", PrimaryKey: true, Checks: []pipeline.ColumnCheck{{Name: "not_null"}}},
		},
		CustomChecks: []pipeline.CustomCheck{{Name: "has rows", Query: "select 1", Value: 1}},
	}
	p := &pipeline.Pipeline{Name: "test", Assets: []*pipeline.Asset{asset}}
	python.InferIngestrDestinations(p, staticConnectionTypes{"warehouse": "duckdb"})

	conn, errs := connection.NewManagerFromConfig(&config.Config{SelectedEnvironment: &config.Environment{Connections: &config.Connections{}}})
	require.Empty(t, errs)

	s := scheduler.NewScheduler(zap.NewNop().Sugar(), p, "test")
	now := time.Now()
	executors, err := SetupExecutors(s, conn, now, now, now, p.Name, "run", false, "", nil, nil, nil, "", "default")
	require.NoError(t, err)

	inferenceExecutors := executors[pipeline.AssetTypeInference]
	assert.IsType(t, &ingestr.IngestrCheckOperator{}, inferenceExecutors[scheduler.TaskInstanceTypeColumnCheck])
	assert.IsType(t, &ingestr.IngestrCustomCheckOperator{}, inferenceExecutors[scheduler.TaskInstanceTypeCustomCheck])
	assert.IsType(t, &ingestr.IngestrMetadataPushOperator{}, inferenceExecutors[scheduler.TaskInstanceTypeMetadataPush])

	// The user-set postgres destination is replaced by the connection's DuckDB, whose check runners are real.
	duckExecutors := executors[pipeline.AssetTypeDuckDBQuery]
	assert.IsType(t, &ansisql.ColumnCheckOperator{}, duckExecutors[scheduler.TaskInstanceTypeColumnCheck])
	assert.IsType(t, &ansisql.CustomCheckOperator{}, duckExecutors[scheduler.TaskInstanceTypeCustomCheck])
}

// Not parallel: SetupExecutors mutates the shared executor.DefaultExecutorsV2.
func TestSetupExecutorsRunsChecksOnSourceAssets(t *testing.T) { //nolint:paralleltest
	// Source assets don't execute, but their checks must query the warehouse instead of passing as no-ops.
	// mongo.source is excluded: there is no SQL check runner for MongoDB.
	sourceTypes := []pipeline.AssetType{
		pipeline.AssetTypeAthenaSource,
		pipeline.AssetTypeBigquerySource,
		pipeline.AssetTypeClickHouseSource,
		pipeline.AssetTypeDatabricksSource,
		pipeline.AssetTypeDorisSource,
		pipeline.AssetTypeDremioSource,
		pipeline.AssetTypeDuckDBSource,
		pipeline.AssetTypeFabricSource,
		pipeline.AssetTypeMsSQLSource,
		pipeline.AssetTypeOracleSource,
		pipeline.AssetTypePostgresSource,
		pipeline.AssetTypeRedshiftSource,
		pipeline.AssetTypeSnowflakeSource,
		pipeline.AssetTypeSparkSource,
		pipeline.AssetTypeStarRocksSource,
		pipeline.AssetTypeSynapseSource,
		pipeline.AssetTypeVerticaSource,
	}

	var assets []*pipeline.Asset
	for _, assetType := range sourceTypes {
		assets = append(assets, &pipeline.Asset{
			Name: "schema." + strings.ReplaceAll(string(assetType), ".", "_"),
			Type: assetType,
			Columns: []pipeline.Column{
				{Name: "id", Checks: []pipeline.ColumnCheck{{Name: "not_null"}}},
			},
			CustomChecks: []pipeline.CustomCheck{{Name: "always fails", Query: "select 1", Value: 0}},
		})
	}
	p := &pipeline.Pipeline{Name: "test", Assets: assets}

	conn, errs := connection.NewManagerFromConfig(&config.Config{SelectedEnvironment: &config.Environment{Connections: &config.Connections{}}})
	require.Empty(t, errs)

	s := scheduler.NewScheduler(zap.NewNop().Sugar(), p, "test")
	now := time.Now()
	executors, err := SetupExecutors(s, conn, now, now, now, p.Name, "run", false, "", nil, nil, nil, "", "default")
	require.NoError(t, err)

	for _, assetType := range sourceTypes {
		t.Run(string(assetType), func(t *testing.T) {
			assert.NotEqual(t, executor.NoOpOperator{}, executors[assetType][scheduler.TaskInstanceTypeColumnCheck])
			assert.IsType(t, &ansisql.CustomCheckOperator{}, executors[assetType][scheduler.TaskInstanceTypeCustomCheck])
		})
	}
}

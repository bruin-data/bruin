package dataprocserverless

import (
	"testing"

	"cloud.google.com/go/dataproc/v2/apiv1/dataprocpb"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestParseParams_Autotuning(t *testing.T) {
	t.Parallel()

	cfg := &Client{Config: Config{ProjectID: "proj", Region: "region"}}

	params := parseParams(cfg, pipeline.ParameterMap{
		"cohort":               "daily-etl",
		"autotuning_scenarios": "auto",
	})
	assert.Equal(t, "daily-etl", params.Cohort)
	assert.Equal(t, []dataprocpb.AutotuningConfig_Scenario{dataprocpb.AutotuningConfig_AUTO}, params.AutotuningScenarios)

	params = parseParams(cfg, pipeline.ParameterMap{
		"autotuning_scenarios": "scaling, broadcast-hash-join,MEMORY,bogus",
	})
	assert.Empty(t, params.Cohort)
	assert.Equal(t, []dataprocpb.AutotuningConfig_Scenario{
		dataprocpb.AutotuningConfig_SCALING,
		dataprocpb.AutotuningConfig_BROADCAST_HASH_JOIN,
		dataprocpb.AutotuningConfig_MEMORY,
	}, params.AutotuningScenarios)

	params = parseParams(cfg, pipeline.ParameterMap{})
	assert.Empty(t, params.Cohort)
	assert.Empty(t, params.AutotuningScenarios)
}

func TestBuildBatchConfig_Autotuning(t *testing.T) {
	t.Parallel()

	newJob := func(params *JobRunParams) Job {
		return Job{
			asset:    &pipeline.Asset{Name: "my_asset.staging"},
			pipeline: &pipeline.Pipeline{Name: "My Pipeline"},
			params:   params,
		}
	}
	ws := &workspace{Entrypoint: "gs://bucket/main.py", Files: "gs://bucket/context.zip"}

	// no autotuning params: nothing set
	req := newJob(&JobRunParams{}).buildBatchConfig(ws)
	assert.Empty(t, req.Batch.RuntimeConfig.Cohort)
	assert.Nil(t, req.Batch.RuntimeConfig.AutotuningConfig)

	// autotuning without explicit cohort: cohort derived from pipeline + asset
	req = newJob(&JobRunParams{
		AutotuningScenarios: []dataprocpb.AutotuningConfig_Scenario{dataprocpb.AutotuningConfig_AUTO},
	}).buildBatchConfig(ws)
	assert.Equal(t, "my-pipeline-my-asset-staging", req.Batch.RuntimeConfig.Cohort)
	assert.Equal(t,
		[]dataprocpb.AutotuningConfig_Scenario{dataprocpb.AutotuningConfig_AUTO},
		req.Batch.RuntimeConfig.AutotuningConfig.Scenarios,
	)

	// explicit cohort wins
	req = newJob(&JobRunParams{
		Cohort:              "custom-cohort",
		AutotuningScenarios: []dataprocpb.AutotuningConfig_Scenario{dataprocpb.AutotuningConfig_SCALING},
	}).buildBatchConfig(ws)
	assert.Equal(t, "custom-cohort", req.Batch.RuntimeConfig.Cohort)

	// cohort without autotuning is allowed
	req = newJob(&JobRunParams{Cohort: "just-cohort"}).buildBatchConfig(ws)
	assert.Equal(t, "just-cohort", req.Batch.RuntimeConfig.Cohort)
	assert.Nil(t, req.Batch.RuntimeConfig.AutotuningConfig)
}

func TestDefaultCohort(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "uk-data-gcp-healthcare-joining", defaultCohort("uk_data_gcp", "healthcare_joining"))
	assert.Equal(t, "p-a-b", defaultCohort("P", "a.b"))

	long := defaultCohort("pipeline", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	assert.LessOrEqual(t, len(long), 63)
}

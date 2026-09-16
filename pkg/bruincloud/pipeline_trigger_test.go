package bruincloud

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduledAgentPipelineTriggerMissing(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		writeJSON(t, w, map[string]any{"pipeline_trigger": nil})
	})
	result, err := client.GetScheduledAgentPipelineTrigger(t.Context(), 42)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.PipelineTrigger)
}

func TestScheduledAgentIncludesPipelineTrigger(t *testing.T) {
	t.Parallel()
	client := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"id": 42, "pipeline_trigger": map[string]string{"id": "daily-etl", "project_id": "analytics"}})
	})
	agent, err := client.GetScheduledAgent(t.Context(), 42)
	require.NoError(t, err)
	require.NotNil(t, agent.PipelineTrigger)
	assert.Equal(t, "daily-etl", agent.PipelineTrigger.ID)
	assert.Equal(t, "analytics", agent.PipelineTrigger.ProjectID)
	data, err := json.Marshal(agent)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"pipeline_trigger":{"id":"daily-etl","project_id":"analytics"}`)
}

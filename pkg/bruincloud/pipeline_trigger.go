package bruincloud

import (
	"context"
	"fmt"
	"net/http"
)

func (c *APIClient) GetScheduledAgentPipelineTrigger(ctx context.Context, scheduledAgentID int) (*ScheduledAgentPipelineTriggerResponse, error) {
	var result ScheduledAgentPipelineTriggerResponse
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/scheduled-agents/%d/pipeline-trigger", scheduledAgentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) SetScheduledAgentPipelineTrigger(ctx context.Context, scheduledAgentID int, selection ScheduledAgentPipelineTrigger) (*ScheduledAgentPipelineTriggerResponse, error) {
	var result ScheduledAgentPipelineTriggerResponse
	err := c.doRequest(ctx, http.MethodPut, fmt.Sprintf("/scheduled-agents/%d/pipeline-trigger", scheduledAgentID), selection, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) DeleteScheduledAgentPipelineTrigger(ctx context.Context, scheduledAgentID int) (*ScheduledAgentPipelineTriggerResponse, error) {
	var result ScheduledAgentPipelineTriggerResponse
	err := c.doRequest(ctx, http.MethodDelete, fmt.Sprintf("/scheduled-agents/%d/pipeline-trigger", scheduledAgentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

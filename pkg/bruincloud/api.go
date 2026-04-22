package bruincloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	defaultBaseURL      = "https://cloud.getbruin.com/api/v1"
	defaultRetryMax     = 3
	defaultRetryWaitMin = 1 * time.Second
	defaultRetryWaitMax = 10 * time.Second
)

// APIClient is the HTTP client for the Bruin Cloud API.
type APIClient struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewAPIClient creates a new API client with the given API key.
func NewAPIClient(apiKey string) *APIClient {
	rc := retryablehttp.NewClient()
	rc.Logger = nil
	rc.RetryMax = defaultRetryMax
	rc.RetryWaitMin = defaultRetryWaitMin
	rc.RetryWaitMax = defaultRetryWaitMax
	rc.CheckRetry = retryOn429
	// Preserve the final response (body + status) when retries are exhausted so
	// doRequest can surface the upstream APIError instead of an opaque "giving
	// up after N attempts" message.
	rc.ErrorHandler = retryablehttp.PassthroughErrorHandler
	return &APIClient{
		baseURL:    defaultBaseURL,
		apiKey:     apiKey,
		httpClient: rc.StandardClient(),
	}
}

// retryOn429 retries only on HTTP 429 Too Many Requests. Other error statuses
// (including 5xx) are returned to the caller unchanged — 429 is the only
// status the server signals as explicitly retryable, and the default backoff
// respects the Retry-After header automatically.
func retryOn429(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		return true, nil
	}
	return false, err
}

// doRequest performs an HTTP request and unmarshals the response.
func (c *APIClient) doRequest(ctx context.Context, method, path string, body any, result any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		apiErr := &APIError{StatusCode: resp.StatusCode}
		if err := json.Unmarshal(respBody, apiErr); err != nil {
			apiErr.Message = fmt.Sprintf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
		}
		if apiErr.Message == "" {
			apiErr.Message = fmt.Sprintf("API error (HTTP %d)", resp.StatusCode)
		}
		return apiErr
	}

	if result != nil {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
	}

	return nil
}

// --- Projects ---

func (c *APIClient) ListProjects(ctx context.Context) ([]Project, error) {
	var projects []Project
	err := c.doRequest(ctx, http.MethodGet, "/projects", nil, &projects)
	return projects, err
}

// --- Pipelines ---

func (c *APIClient) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	var pipelines []Pipeline
	err := c.doRequest(ctx, http.MethodGet, "/pipelines", nil, &pipelines)
	return pipelines, err
}

func (c *APIClient) GetPipeline(ctx context.Context, project, name string) (*Pipeline, error) {
	params := url.Values{}
	params.Set("project", project)
	params.Set("name", name)
	var resp struct {
		Data Pipeline `json:"data"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/pipeline?"+params.Encode(), nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp.Data, nil
}

func (c *APIClient) DeletePipeline(ctx context.Context, project, pipeline string) error {
	params := url.Values{}
	params.Set("project", project)
	params.Set("pipeline", pipeline)
	return c.doRequest(ctx, http.MethodDelete, "/pipeline?"+params.Encode(), nil, nil)
}

func (c *APIClient) EnablePipeline(ctx context.Context, project, pipeline string) error {
	body := map[string]any{
		"pipelines": []map[string]string{
			{"project": project, "pipeline": pipeline},
		},
	}
	return c.doRequest(ctx, http.MethodPost, "/enable-pipelines", body, nil)
}

func (c *APIClient) DisablePipeline(ctx context.Context, project, pipeline string) error {
	body := map[string]any{
		"pipelines": []map[string]string{
			{"project": project, "pipeline": pipeline},
		},
	}
	return c.doRequest(ctx, http.MethodPost, "/disable-pipelines", body, nil)
}

func (c *APIClient) GetPipelineErrors(ctx context.Context) ([]json.RawMessage, error) {
	var errors []json.RawMessage
	err := c.doRequest(ctx, http.MethodGet, "/pipeline-validation-errors", nil, &errors)
	return errors, err
}

// --- Runs ---

func (c *APIClient) ListRuns(ctx context.Context, project, pipeline string, limit, offset int) ([]PipelineRun, error) {
	params := url.Values{}
	params.Set("project", project)
	params.Set("name", pipeline)
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	var runs []PipelineRun
	err := c.doRequest(ctx, http.MethodGet, "/pipeline-runs?"+params.Encode(), nil, &runs)
	return runs, err
}

func (c *APIClient) GetRun(ctx context.Context, project, pipeline, runID string) (*PipelineRun, error) {
	params := url.Values{}
	params.Set("project", project)
	params.Set("name", pipeline)
	params.Set("run_id", runID)
	var resp struct {
		Data PipelineRun `json:"data"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/pipeline-run?"+params.Encode(), nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp.Data, nil
}

func (c *APIClient) TriggerRun(ctx context.Context, project, pipeline, startDate, endDate string) error {
	body := map[string]any{
		"pipelines": []map[string]string{
			{
				"project":    project,
				"pipeline":   pipeline,
				"start_date": startDate,
				"end_date":   endDate,
			},
		},
	}
	return c.doRequest(ctx, http.MethodPost, "/trigger-pipeline-runs", body, nil)
}

func (c *APIClient) RerunRun(ctx context.Context, project, pipeline, runID string, onlyFailed bool) error {
	body := map[string]any{
		"only_failed": onlyFailed,
		"pipeline_runs": []map[string]string{
			{
				"project":  project,
				"pipeline": pipeline,
				"run_id":   runID,
			},
		},
	}
	return c.doRequest(ctx, http.MethodPost, "/rerun-pipeline-runs", body, nil)
}

func (c *APIClient) MarkRunStatus(ctx context.Context, project, pipeline, runID, status string) error {
	body := map[string]any{
		"status": status,
		"pipeline_runs": []map[string]string{
			{
				"project":  project,
				"pipeline": pipeline,
				"run_id":   runID,
			},
		},
	}
	return c.doRequest(ctx, http.MethodPost, "/mark-pipeline-runs-status", body, nil)
}

func (c *APIClient) GetLatestRun(ctx context.Context, project, pipeline string) (*PipelineRun, error) {
	runs, err := c.ListRuns(ctx, project, pipeline, 1, 0)
	if err != nil {
		return nil, err
	}
	if len(runs) == 0 {
		return nil, fmt.Errorf("no runs found for pipeline '%s' in project '%s'", pipeline, project)
	}
	return &runs[0], nil
}

// --- Assets ---

func (c *APIClient) ListAssets(ctx context.Context, project, pipeline string) ([]Asset, error) {
	params := url.Values{}
	params.Set("project", project)
	params.Set("name", pipeline)
	var assets []Asset
	err := c.doRequest(ctx, http.MethodGet, "/pipeline-assets?"+params.Encode(), nil, &assets)
	return assets, err
}

func (c *APIClient) GetAsset(ctx context.Context, project, pipeline, asset string) (*Asset, error) {
	params := url.Values{}
	params.Set("project", project)
	params.Set("pipeline", pipeline)
	params.Set("asset", asset)
	var a Asset
	err := c.doRequest(ctx, http.MethodGet, "/asset?"+params.Encode(), nil, &a)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// --- Instances ---

func (c *APIClient) ListInstances(ctx context.Context, project, pipeline, runID string) (json.RawMessage, error) {
	body := map[string]string{
		"project":  project,
		"pipeline": pipeline,
		"run_id":   runID,
	}
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodPost, "/asset-instances-for-run", body, &result)
	return result, err
}

func (c *APIClient) ListInstancesParsed(ctx context.Context, project, pipeline, runID string) (*AssetInstanceResponse, error) {
	body := map[string]string{
		"project":  project,
		"pipeline": pipeline,
		"run_id":   runID,
	}
	var result AssetInstanceResponse
	err := c.doRequest(ctx, http.MethodPost, "/asset-instances-for-run", body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) GetInstance(ctx context.Context, project, pipeline, runID, assetName string) (json.RawMessage, error) {
	body := map[string]string{
		"project":    project,
		"pipeline":   pipeline,
		"run_id":     runID,
		"asset_name": assetName,
	}
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodPost, "/asset-instance-details", body, &result)
	return result, err
}

func (c *APIClient) GetInstanceLogs(ctx context.Context, project, pipeline, runID, stepID string, tryNumber int) (json.RawMessage, error) {
	body := map[string]any{
		"project":    project,
		"pipeline":   pipeline,
		"run_id":     runID,
		"step_id":    stepID,
		"try_number": tryNumber,
	}
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodPost, "/asset-instance-logs", body, &result)
	return result, err
}

// --- Glossary ---

func (c *APIClient) ListGlossaryEntities(ctx context.Context) (json.RawMessage, error) {
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodGet, "/glossary-entities", nil, &result)
	return result, err
}

func (c *APIClient) GetGlossaryEntity(ctx context.Context, project, entityName string) (json.RawMessage, error) {
	body := map[string]string{
		"project":     project,
		"entity_name": entityName,
	}
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodPost, "/glossary-entity-details", body, &result)
	return result, err
}

// --- Agents ---

func (c *APIClient) ListAgents(ctx context.Context) ([]Agent, error) {
	var resp struct {
		Agents []Agent `json:"agents"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/agents", nil, &resp)
	return resp.Agents, err
}

func (c *APIClient) SendAgentMessage(ctx context.Context, agentID int, message string, threadID *int) (json.RawMessage, error) {
	body := map[string]any{
		"message": message,
	}
	if threadID != nil {
		body["thread_id"] = *threadID
	}
	var result json.RawMessage
	err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/agents/%d/messages", agentID), body, &result)
	return result, err
}

func (c *APIClient) GetAgentMessageStatus(ctx context.Context, agentID, threadID, messageID int) (*AgentMessage, error) {
	var resp struct {
		Data AgentMessage `json:"data"`
	}
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/agents/%d/threads/%d/messages/%d", agentID, threadID, messageID), nil, &resp)
	if err != nil {
		return nil, err
	}
	return &resp.Data, nil
}

func (c *APIClient) ListAgentThreads(ctx context.Context, agentID int, limit, offset int) ([]AgentThread, error) {
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	path := fmt.Sprintf("/agents/%d/threads", agentID)
	if len(params) > 0 {
		path += "?" + params.Encode()
	}
	var resp struct {
		Threads []AgentThread `json:"threads"`
	}
	err := c.doRequest(ctx, http.MethodGet, path, nil, &resp)
	return resp.Threads, err
}

func (c *APIClient) ListAgentMessages(ctx context.Context, agentID, threadID int, limit, offset int) ([]AgentMessage, error) {
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	path := fmt.Sprintf("/agents/%d/threads/%d/messages", agentID, threadID)
	if len(params) > 0 {
		path += "?" + params.Encode()
	}
	var resp struct {
		Messages []AgentMessage `json:"messages"`
	}
	err := c.doRequest(ctx, http.MethodGet, path, nil, &resp)
	return resp.Messages, err
}

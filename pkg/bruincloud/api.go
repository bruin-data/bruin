package bruincloud

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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
	team       string
	httpClient *http.Client
}

// SetTeam makes requests act on the given team (company prefix) via the
// X-Bruin-Team header, instead of the token owner's current team.
func (c *APIClient) SetTeam(team string) {
	c.team = team
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
	baseURL := defaultBaseURL
	if v := os.Getenv("BRUIN_CLOUD_BASE_URL"); v != "" {
		baseURL = v
	}
	return &APIClient{
		baseURL:    baseURL,
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
	if c.team != "" {
		req.Header.Set("X-Bruin-Team", c.team)
	}
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

// --- Teams ---

// ListTeams returns the teams the token can act on. Unlike the other endpoints
// it needs no --team: it's how you discover the company_prefixes to pass there.
func (c *APIClient) ListTeams(ctx context.Context) ([]Team, error) {
	var resp struct {
		Teams []Team `json:"teams"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/teams", nil, &resp)
	return resp.Teams, err
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

// TriggerRunOptions holds the optional parameters the trigger endpoint accepts.
type TriggerRunOptions struct {
	Assets      []string
	Downstream  bool
	FullRefresh bool
	Split       string
	ChunkSize   int
	Variables   map[string]any
	Note        string
	Tags        []string
}

// encodeRunNote serializes the run note and tags into the single note field the Cloud expects.
func encodeRunNote(note string, tags []string) string {
	if note == "" && len(tags) == 0 {
		return ""
	}
	if tags == nil {
		tags = []string{}
	}
	b, err := json.Marshal(struct {
		Note string   `json:"note"`
		Tags []string `json:"tags"`
	}{Note: note, Tags: tags})
	if err != nil {
		return ""
	}
	return string(b)
}

// TriggerRunResult holds the response from the trigger endpoint. A normal run
// returns RunID; a backfill (triggered with a split) returns MultipleActionID
// and a URL to track the backfill in the dashboard.
type TriggerRunResult struct {
	Message          string `json:"message"`
	Project          string `json:"project"`
	Pipeline         string `json:"pipeline"`
	RunID            string `json:"run_id"`
	MultipleActionID string `json:"multiple_action_id"`
	Split            string `json:"split"`
	ChunkSize        int    `json:"chunk_size"`
	URL              string `json:"url"`
}

// TriggerRun triggers a pipeline run for the given date range.
func (c *APIClient) TriggerRun(ctx context.Context, project, pipeline, startDate, endDate string, opts TriggerRunOptions) (*TriggerRunResult, error) {
	body := map[string]any{
		"project":    project,
		"pipeline":   pipeline,
		"start_date": startDate,
		"end_date":   endDate,
	}
	if len(opts.Assets) > 0 {
		body["assets"] = opts.Assets
	}
	if opts.Downstream {
		body["downstream"] = true
	}
	if opts.FullRefresh {
		body["full_refresh"] = true
	}
	if opts.Split != "" {
		body["split"] = opts.Split
		chunkSize := opts.ChunkSize
		if chunkSize < 1 {
			chunkSize = 1
		}
		body["chunk_size"] = chunkSize
	}
	if len(opts.Variables) > 0 {
		body["variables"] = opts.Variables
	}
	if note := encodeRunNote(opts.Note, opts.Tags); note != "" {
		body["note"] = note
	}
	var result TriggerRunResult
	if err := c.doRequest(ctx, http.MethodPost, "/trigger-pipeline-run", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
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

// --- Backfills ---

// ListBackfills returns the most recent backfills, optionally filtered by project and pipeline.
func (c *APIClient) ListBackfills(ctx context.Context, project, pipeline string, limit int) ([]Backfill, error) {
	params := url.Values{}
	if project != "" {
		params.Set("project", project)
	}
	if pipeline != "" {
		params.Set("pipeline", pipeline)
	}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	path := "/backfills"
	if enc := params.Encode(); enc != "" {
		path += "?" + enc
	}
	var backfills []Backfill
	err := c.doRequest(ctx, http.MethodGet, path, nil, &backfills)
	return backfills, err
}

// GetBackfillRuns returns the individual runs that make up a single backfill.
func (c *APIClient) GetBackfillRuns(ctx context.Context, multipleActionID string, limit, offset int) ([]BackfillRun, error) {
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	if offset > 0 {
		params.Set("offset", strconv.Itoa(offset))
	}
	path := "/backfills/" + url.PathEscape(multipleActionID) + "/runs"
	if enc := params.Encode(); enc != "" {
		path += "?" + enc
	}
	var runs []BackfillRun
	err := c.doRequest(ctx, http.MethodGet, path, nil, &runs)
	return runs, err
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

// ListAgentConnections returns the connection names and types available to the
// agent (from its dev-env secret) — names/types only, never credentials. Use it
// to pick a connection the agent can actually query.
func (c *APIClient) ListAgentConnections(ctx context.Context, agentID int) ([]AgentConnection, error) {
	var resp struct {
		Connections []AgentConnection `json:"connections"`
	}
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/agents/%d/connections", agentID), nil, &resp)
	return resp.Connections, err
}

// AddAgentConnection adds a connection (type + name + config) to the agent's
// connection set and returns the resulting connections — names/types only,
// never credentials. The config carries the credential values and is sent to
// the server, not logged.
func (c *APIClient) AddAgentConnection(ctx context.Context, agentID int, connType, name string, config map[string]any) ([]AgentConnection, error) {
	body := map[string]any{"type": connType, "name": name, "config": config}
	var resp struct {
		Connections []AgentConnection `json:"connections"`
	}
	err := c.doRequest(ctx, http.MethodPost, fmt.Sprintf("/agents/%d/connections", agentID), body, &resp)
	return resp.Connections, err
}

// CreateAgent creates a new agent. Optional fields are omitted when empty so the
// server applies its defaults (null description/prompt, "team" visibility).
func (c *APIClient) CreateAgent(ctx context.Context, name, description, systemPrompt, visibility string) (*Agent, error) {
	body := map[string]any{"name": name}
	if description != "" {
		body["description"] = description
	}
	if systemPrompt != "" {
		body["system_prompt"] = systemPrompt
	}
	if visibility != "" {
		body["visibility"] = visibility
	}
	var result Agent
	err := c.doRequest(ctx, http.MethodPost, "/agents", body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetAgent returns a single agent's details.
func (c *APIClient) GetAgent(ctx context.Context, agentID int) (*Agent, error) {
	var result Agent
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/agents/%d", agentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateAgent patches an agent with the given fields. The caller decides which
// fields to include (based on which flags were set), so an explicit empty value
// is sent and can clear a field.
func (c *APIClient) UpdateAgent(ctx context.Context, agentID int, fields map[string]any) (*Agent, error) {
	var result Agent
	err := c.doRequest(ctx, http.MethodPatch, fmt.Sprintf("/agents/%d", agentID), fields, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ListAgentMcpServers returns the agent's current external MCP server picks
// along with the supported kinds and the connections eligible for each.
func (c *APIClient) ListAgentMcpServers(ctx context.Context, agentID int) (*AgentMcpServersResponse, error) {
	var result AgentMcpServersResponse
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/agents/%d/mcp-servers", agentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// SetAgentMcpServers replaces the agent's external MCP picks with the given set.
// This is a full replace: any kind not in servers is detached.
func (c *APIClient) SetAgentMcpServers(ctx context.Context, agentID int, servers []AgentMcpServer) (*Agent, error) {
	return c.UpdateAgent(ctx, agentID, map[string]any{"mcp_integrations": servers})
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

func (c *APIClient) GetAgentPrompt(ctx context.Context, agentID int) (*AgentPrompt, error) {
	var result AgentPrompt
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/agents/%d/prompt", agentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) SetAgentPrompt(ctx context.Context, agentID int, systemPrompt string) (*AgentPrompt, error) {
	body := map[string]any{"system_prompt": systemPrompt}
	var result AgentPrompt
	err := c.doRequest(ctx, http.MethodPut, fmt.Sprintf("/agents/%d/prompt", agentID), body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
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

// --- Connections ---

func (c *APIClient) ListConnections(ctx context.Context) ([]Connection, error) {
	var connections []Connection
	err := c.doRequest(ctx, http.MethodGet, "/connections", nil, &connections)
	return connections, err
}

func (c *APIClient) CreateConnection(ctx context.Context, name, connType string, credentials map[string]any) error {
	body := map[string]any{
		"name":        name,
		"type":        connType,
		"credentials": credentials,
	}
	return c.doRequest(ctx, http.MethodPost, "/connections", body, nil)
}

func (c *APIClient) DeleteConnection(ctx context.Context, name string) error {
	return c.doRequest(ctx, http.MethodDelete, "/connections/"+url.PathEscape(name), nil, nil)
}

// --- Dashboards ---

func (c *APIClient) ListDashboards(ctx context.Context) ([]Dashboard, error) {
	var resp struct {
		Dashboards []Dashboard `json:"dashboards"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/dashboards", nil, &resp)
	return resp.Dashboards, err
}

func (c *APIClient) GetDashboard(ctx context.Context, dashboardID int) (*Dashboard, error) {
	var result Dashboard
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/dashboards/%d", dashboardID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateDashboard creates a dashboard from a definition. The server writes the
// definition to the draft only (never published). Empty optional fields are
// omitted so the server applies its defaults.
func (c *APIClient) CreateDashboard(ctx context.Context, title, visibility string, agentID int, state map[string]any) (*Dashboard, error) {
	body := map[string]any{"title": title}
	if visibility != "" {
		body["visibility"] = visibility
	}
	// Bind the dashboard to an agent so canvas chat and refresh work; omit to let
	// the server fall back to the agent encoded in a Cloud-CLI token.
	if agentID > 0 {
		body["agent_id"] = agentID
	}
	if state != nil {
		body["state"] = state
	}
	var result Dashboard
	err := c.doRequest(ctx, http.MethodPost, "/dashboards", body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateDashboard applies a partial update to a dashboard. Only the fields
// present are changed; the definition (state) is written to the draft only
// (never published), same as create. Changing visibility requires manage-access
// on the server side.
func (c *APIClient) UpdateDashboard(ctx context.Context, dashboardID int, fields map[string]any) (*Dashboard, error) {
	var result Dashboard
	err := c.doRequest(ctx, http.MethodPatch, fmt.Sprintf("/dashboards/%d", dashboardID), fields, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *APIClient) ListScheduledAgents(ctx context.Context) ([]ScheduledAgent, error) {
	var resp struct {
		ScheduledAgents []ScheduledAgent `json:"scheduled_agents"`
	}
	err := c.doRequest(ctx, http.MethodGet, "/scheduled-agents", nil, &resp)
	return resp.ScheduledAgents, err
}

func (c *APIClient) GetScheduledAgent(ctx context.Context, scheduledAgentID int) (*ScheduledAgent, error) {
	var result ScheduledAgent
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/scheduled-agents/%d", scheduledAgentID), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// CreateScheduledAgent creates a scheduled agent from a plan. The server stores it as
// an inactive draft (a human activates it from the UI); fields is the request
// body and must include agent_id.
func (c *APIClient) CreateScheduledAgent(ctx context.Context, fields map[string]any) (*ScheduledAgent, error) {
	var result ScheduledAgent
	err := c.doRequest(ctx, http.MethodPost, "/scheduled-agents", fields, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateScheduledAgent applies a partial update to a scheduled agent's plan. Only the
// fields present are changed; activation is not updatable via the API.
func (c *APIClient) UpdateScheduledAgent(ctx context.Context, scheduledAgentID int, fields map[string]any) (*ScheduledAgent, error) {
	var result ScheduledAgent
	err := c.doRequest(ctx, http.MethodPatch, fmt.Sprintf("/scheduled-agents/%d", scheduledAgentID), fields, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// --- Scheduled agent run state ---

// ListRunStates returns the run-state ("memory") files persisted on a scheduled
// agent. Each is a markdown file the agent carries across runs, keyed by name.
func (c *APIClient) ListRunStates(ctx context.Context, scheduledAgentID int) ([]RunState, error) {
	var resp struct {
		RunStates []RunState `json:"run_states"`
	}
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/scheduled-agents/%d/run-states", scheduledAgentID), nil, &resp)
	return resp.RunStates, err
}

// GetRunState returns a single run-state file by name.
func (c *APIClient) GetRunState(ctx context.Context, scheduledAgentID int, name string) (*RunState, error) {
	var result RunState
	err := c.doRequest(ctx, http.MethodGet, fmt.Sprintf("/scheduled-agents/%d/run-states/%s", scheduledAgentID, url.PathEscape(name)), nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// SetRunState creates or replaces a run-state file (the server upserts on name).
func (c *APIClient) SetRunState(ctx context.Context, scheduledAgentID int, name, content string) (*RunState, error) {
	var result RunState
	body := map[string]any{"content": content}
	err := c.doRequest(ctx, http.MethodPut, fmt.Sprintf("/scheduled-agents/%d/run-states/%s", scheduledAgentID, url.PathEscape(name)), body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteRunState removes a run-state file by name.
func (c *APIClient) DeleteRunState(ctx context.Context, scheduledAgentID int, name string) error {
	return c.doRequest(ctx, http.MethodDelete, fmt.Sprintf("/scheduled-agents/%d/run-states/%s", scheduledAgentID, url.PathEscape(name)), nil, nil)
}

// --- Audit logs ---

// ListAuditLogs returns a page of the team's audit trail, most recent first.
// The endpoint is read-only and accepts only a personal (user-scoped) token.
func (c *APIClient) ListAuditLogs(ctx context.Context, opts AuditLogListOptions) ([]AuditLog, error) {
	params := url.Values{}
	for _, t := range opts.Types {
		params.Add("types[]", t)
	}
	for _, id := range opts.UserIDs {
		params.Add("userIds[]", id)
	}
	if opts.StartDate != "" {
		params.Set("startDate", opts.StartDate)
	}
	if opts.EndDate != "" {
		params.Set("endDate", opts.EndDate)
	}
	if opts.Limit > 0 {
		params.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		params.Set("offset", strconv.Itoa(opts.Offset))
	}

	path := "/audit-logs"
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var resp struct {
		AuditLogs []AuditLog `json:"auditLogs"`
	}
	err := c.doRequest(ctx, http.MethodGet, path, nil, &resp)
	return resp.AuditLogs, err
}

// GetCostExplorerSchema lists the dimensions, filter fields, and time buckets the cost explorer supports.
func (c *APIClient) GetCostExplorerSchema(ctx context.Context, platform string) (*CostExplorerSchema, error) {
	params := url.Values{}
	if platform != "" {
		params.Set("platform", platform)
	}
	path := "/cost-explorer/schema"
	if enc := params.Encode(); enc != "" {
		path += "?" + enc
	}
	var schema CostExplorerSchema
	err := c.doRequest(ctx, http.MethodGet, path, nil, &schema)
	return &schema, err
}

// GetCostExplorer returns a page of warehouse cost breakdown rows.
func (c *APIClient) GetCostExplorer(ctx context.Context, req CostExplorerRequest) (*CostExplorerResponse, error) {
	var resp CostExplorerResponse
	err := c.doRequest(ctx, http.MethodPost, "/cost-explorer", req, &resp)
	return &resp, err
}

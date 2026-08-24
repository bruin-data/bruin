package bruincloud

import (
	"encoding/json"
	"strings"
)

// Team is a team the caller's token can act on. CompanyPrefix is what you pass
// to --team (the X-Bruin-Team header) to target this team on other commands.
type Team struct {
	ID            int    `json:"id"`
	Name          string `json:"name"`
	CompanyPrefix string `json:"company_prefix"`
}

// Project represents a Bruin Cloud project.
type Project struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Repo     Repo            `json:"repo"`
	Rollouts json.RawMessage `json:"rollouts"`
}

// Repo represents a git repository configuration.
type Repo struct {
	URL    string `json:"url"`
	Branch string `json:"branch"`
}

// Pipeline represents a Bruin Cloud pipeline.
type Pipeline struct {
	Name               string          `json:"name"`
	Description        *string         `json:"description"`
	Schedule           *string         `json:"schedule"`
	Assets             json.RawMessage `json:"assets"`
	Owner              json.RawMessage `json:"owner"`
	StartDate          string          `json:"start_date"`
	Project            string          `json:"project"`
	DefaultConnections json.RawMessage `json:"default_connections"`
	Commit             *string         `json:"commit"`
	Variables          json.RawMessage `json:"variables"`
	OxrScheduling      *bool           `json:"oxrScheduling"`
	Status             *string         `json:"status"`
}

// PipelineRun represents a pipeline run.
type PipelineRun struct {
	Project                   string          `json:"project"`
	Pipeline                  string          `json:"pipeline"`
	RunID                     string          `json:"run_id"`
	DataIntervalStart         json.RawMessage `json:"data_interval_start"`
	DataIntervalEnd           json.RawMessage `json:"data_interval_end"`
	StartDate                 json.RawMessage `json:"start_date"`
	EndDate                   json.RawMessage `json:"end_date"`
	WallTimeDuration          *float64        `json:"wall_time_duration"`
	WallTimeDurationHumanized *string         `json:"wall_time_duration_humanized"`
	TotalExecutionDuration    *float64        `json:"total_execution_duration"`
	Status                    string          `json:"status"`
	UnknownInstanceCount      int             `json:"unknown_instance_count"`
	Note                      *string         `json:"note"`
}

// Backfill represents a group of runs created by a single split trigger,
// identified by its multiple_action_id.
type Backfill struct {
	ID            string        `json:"id"`
	Project       string        `json:"project"`
	Pipeline      string        `json:"pipeline"`
	IntervalStart string        `json:"interval_start"`
	IntervalEnd   string        `json:"interval_end"`
	CreatedAt     string        `json:"created_at"`
	Runs          []BackfillRun `json:"runs"`
}

// BackfillRun represents a single run within a backfill.
type BackfillRun struct {
	Project       string  `json:"project"`
	Pipeline      string  `json:"pipeline"`
	RunID         string  `json:"run_id"`
	IntervalStart string  `json:"interval_start"`
	IntervalEnd   string  `json:"interval_end"`
	CreatedAt     string  `json:"created_at"`
	Note          *string `json:"note"`
}

// Asset represents a pipeline asset.
type Asset struct {
	Project                 string          `json:"project"`
	Pipeline                string          `json:"pipeline"`
	ID                      string          `json:"id"`
	Name                    string          `json:"name"`
	Type                    string          `json:"type"`
	URI                     string          `json:"uri"`
	Description             *string         `json:"description"`
	Content                 *string         `json:"content"`
	Upstreams               json.RawMessage `json:"upstreams"`
	Downstream              json.RawMessage `json:"downstream"`
	Columns                 json.RawMessage `json:"columns"`
	CustomChecks            json.RawMessage `json:"custom_checks"`
	Owner                   json.RawMessage `json:"owner"`
	Materialization         json.RawMessage `json:"materialization"`
	Instance                *string         `json:"instance"`
	Tags                    json.RawMessage `json:"tags"`
	Connection              *string         `json:"connection"`
	Image                   *string         `json:"image"`
	Parameters              json.RawMessage `json:"parameters"`
	Metadata                json.RawMessage `json:"metadata"`
	MarkdownDescription     *string         `json:"markdown_description"`
	QualityScore            int             `json:"quality_score"`
	MaxPossibleQualityScore int             `json:"max_possible_quality_score"`
	QualityScorePercentage  int             `json:"quality_score_percentage"`
}

// AssetInstance represents an asset instance within a pipeline run.
// The API returns dynamic JSON, so we use json.RawMessage for the full response.
type AssetInstance struct {
	json.RawMessage
}

// LogEntry represents a log entry for an asset instance.
type LogEntry struct {
	json.RawMessage
}

// GlossaryEntity represents a glossary entity.
// The API returns dynamic JSON structures, so we use json.RawMessage.
type GlossaryEntity struct {
	json.RawMessage
}

// Agent represents a Bruin Cloud agent.
type Agent struct {
	ID              int              `json:"id"`
	Name            string           `json:"name"`
	Description     *string          `json:"description"`
	Visibility      string           `json:"visibility"`
	MCPIntegrations []AgentMcpServer `json:"mcp_integrations,omitempty"`
}

// AgentMcpServer is one external MCP server attached to an agent — a supported
// kind (linear, github, …) backed by a bruin.yml connection in the agent's
// dev-env set.
type AgentMcpServer struct {
	Kind           string  `json:"kind"`
	ConnectionName string  `json:"connection_name"`
	DisplayName    *string `json:"display_name"`
}

// AgentMcpServersResponse is the payload of the agent MCP-servers endpoint: the
// current picks plus the options for configuring them.
type AgentMcpServersResponse struct {
	MCPIntegrations      []AgentMcpServer    `json:"mcp_integrations"`
	MCPKinds             map[string]string   `json:"mcp_kinds"`
	ConnectionsByMcpKind map[string][]string `json:"connections_by_mcp_kind"`
}

// AgentConnection is one connection available to an agent — name and type only
// (no credential values).
type AgentConnection struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// AgentThread represents a thread for an agent.
type AgentThread struct {
	ID         int     `json:"id"`
	AgentID    int     `json:"agent_id"`
	Title      *string `json:"title"`
	CreatedAt  string  `json:"created_at"`
	UpdatedAt  string  `json:"updated_at"`
	ArchivedAt *string `json:"archived_at"`
}

// AgentPrompt represents an agent's system prompt.
type AgentPrompt struct {
	ID           int     `json:"id"`
	Name         string  `json:"name"`
	SystemPrompt *string `json:"system_prompt"`
}

// AgentMemory represents an agent's long-term memory blob.
type AgentMemory struct {
	ID     int     `json:"id"`
	Name   string  `json:"name"`
	Memory *string `json:"memory"`
}

// AgentMessage represents a message in an agent thread.
type AgentMessage struct {
	ID                int             `json:"id"`
	Status            string          `json:"status"`
	OutputMessage     *string         `json:"output_message"`
	AgentLogs         json.RawMessage `json:"agent_logs"`
	QueryLogs         json.RawMessage `json:"query_logs"`
	OutputAttachments json.RawMessage `json:"output_attachments"`
	CreatedAt         string          `json:"created_at"`
	UpdatedAt         string          `json:"updated_at"`
}

// PipelineValidationError represents a pipeline validation error.
// The API returns dynamic JSON structures.
type PipelineValidationError struct {
	json.RawMessage
}

// ExtractDateString extracts a date string from the API's date JSON format.
// API dates come as {"date": "2026-03-06 20:22:29.753319", "timezone_type": 1, "timezone": "+00:00"}.
func ExtractDateString(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var dv struct {
		Date string `json:"date"`
	}
	if err := json.Unmarshal(raw, &dv); err != nil {
		return ""
	}
	if idx := strings.Index(dv.Date, "."); idx > 0 {
		return dv.Date[:idx]
	}
	return dv.Date
}

// AssetInstanceResponse represents the parsed response from the asset instances endpoint.
type AssetInstanceResponse struct {
	Message        string                       `json:"message"`
	AssetInstances map[string]AssetInstanceInfo `json:"asset_instances"`
	RunID          string                       `json:"run_id"`
}

// AssetInstanceInfo represents detailed information about an asset instance.
type AssetInstanceInfo struct {
	Asset                  string             `json:"asset"`
	Type                   string             `json:"type"`
	StartDate              string             `json:"start_date"`
	EndDate                string             `json:"end_date"`
	WallTimeDuration       float64            `json:"wall_time_duration"`
	TotalExecutionDuration float64            `json:"total_execution_duration"`
	Status                 string             `json:"status"`
	IsFinished             bool               `json:"is_finished"`
	Steps                  AssetInstanceSteps `json:"steps"`
	StepIDs                []string           `json:"step_ids"`
}

// AssetInstanceSteps represents the steps of an asset instance.
type AssetInstanceSteps struct {
	Main   []StepInstance      `json:"main"`
	Checks AssetInstanceChecks `json:"checks"`
}

// StepInstance represents a step or check instance.
type StepInstance struct {
	Name       string  `json:"name"`
	StepID     string  `json:"stepId"`
	StartDate  string  `json:"startDate"`
	EndDate    string  `json:"endDate"`
	Duration   float64 `json:"duration"`
	TryNumber  int     `json:"tryNumber"`
	Status     string  `json:"status"`
	IsFinished bool    `json:"isFinished"`
}

// AssetInstanceChecks represents checks for an asset instance.
type AssetInstanceChecks struct {
	Column []AssetInstanceCheck `json:"column"`
	Custom []AssetInstanceCheck `json:"custom"`
}

// AssetInstanceCheck represents a named check with its step instance.
type AssetInstanceCheck struct {
	Name     string       `json:"name"`
	Instance StepInstance `json:"instance"`
}

// APIError represents an error response from the API.
type APIError struct {
	Message string `json:"message"`
	// Code is the API's machine-readable error identifier (e.g. "team_required",
	// "team_not_in_scope"), when present, used to render actionable CLI hints.
	Code       string              `json:"error,omitempty"`
	Errors     map[string][]string `json:"errors,omitempty"`
	StatusCode int                 `json:"-"`
}

func (e *APIError) Error() string {
	if len(e.Errors) > 0 {
		var b strings.Builder
		b.WriteString(e.Message)
		b.WriteString(":")
		for field, errs := range e.Errors {
			for _, err := range errs {
				b.WriteString(" ")
				b.WriteString(field)
				b.WriteString(" ")
				b.WriteString(err)
				b.WriteString(";")
			}
		}
		return b.String()
	}
	return e.Message
}

// SendAgentMessageResponse represents the response from sending a message to an agent.
type SendAgentMessageResponse struct {
	ThreadID  int `json:"thread_id"`
	MessageID int `json:"message_id"`
}

// Connection represents a Bruin Cloud connection (name and type only).
type Connection struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ConnectionSet represents a Bruin Cloud connection set (dev-env secret): a named
// bundle of connections an agent runs against via its connection_set_id.
type ConnectionSet struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// Skill is a team skill-library entry — a named instruction snippet attached to
// agents. AgentIDs lists the agents it's attached to.
type Skill struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Body        string `json:"body"`
	AllAgents   bool   `json:"all_agents"`
	AgentIDs    []int  `json:"agent_ids"`
}

// Dashboard represents a Bruin Cloud dashboard. State is only populated by the
// single-dashboard endpoint and carries the published definition as raw JSON.
type Dashboard struct {
	ID         int             `json:"id"`
	Title      *string         `json:"title"`
	Visibility string          `json:"visibility"`
	UpdatedAt  *string         `json:"updated_at"`
	URL        string          `json:"url"`
	AgentID    *int            `json:"agent_id"`
	State      json.RawMessage `json:"state,omitempty"`
}

// DashboardVersion is one version-history snapshot's metadata (no state blob).
type DashboardVersion struct {
	ID         int    `json:"id"`
	Kind       string `json:"kind"`        // "draft" | "published"
	CreatedAt  string `json:"created_at"`  // ISO 8601; empty if unknown
	Author     string `json:"author"`      // user / team / agent name; empty if unknown
	AuthorKind string `json:"author_kind"` // "user" | "team" | "agent" | ""
	ViaAPI     bool   `json:"via_api"`     // written through an API token (CLI) vs the UI
}

// DashboardVersionDetail is a snapshot including its full parsed state.
type DashboardVersionDetail struct {
	DashboardVersion
	State json.RawMessage `json:"state"`
}

// ScheduledAgent represents a Bruin Cloud scheduled agent — a cron-based recurring
// agent task. The nested plan fields (verified SQLs, memory, ...) are kept as raw
// JSON so `--output json` round-trips the full server response faithfully.
// RunState is a single markdown "memory" file persisted across runs of a
// scheduled agent, keyed by name. Content may be empty.
type RunState struct {
	Name      string  `json:"name"`
	Content   string  `json:"content"`
	CreatedAt *string `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
}

// ScheduledAgent represents a Bruin Cloud scheduled agent — a cron-based
// recurring agent task. The nested plan fields (verified SQLs, memory, ...) are
// kept as raw JSON so `--output json` round-trips the full server response.
type ScheduledAgent struct {
	ID                int             `json:"id"`
	Title             *string         `json:"title"`
	IsActive          bool            `json:"is_active"`
	ScheduleCron      *string         `json:"schedule_cron"`
	ScheduleTimezone  *string         `json:"schedule_timezone"`
	NextRunAt         *string         `json:"next_run_at"`
	LastRunAt         *string         `json:"last_run_at"`
	Instructions      *string         `json:"instructions"`
	OutputFormatting  *string         `json:"output_formatting"`
	VerifiedSqls      json.RawMessage `json:"verified_sqls,omitempty"`
	Memory            json.RawMessage `json:"memory,omitempty"`
	MonitorsDashboard json.RawMessage `json:"monitors_dashboard,omitempty"`
	RecentExecutions  json.RawMessage `json:"recent_executions,omitempty"`
}

// ScheduledAgentExecution is the execution a trigger stands up: the run that was
// just kicked off, and the thread it runs in.
type ScheduledAgentExecution struct {
	ExecutionID int `json:"execution_id"`
	ThreadID    int `json:"thread_id"`
}

// AuditLog is one entry in a team's audit trail as returned by GET /audit-logs.
// Metadata is event-specific and left raw.
type AuditLog struct {
	Type           string          `json:"type"`
	UserIdentifier string          `json:"user_identifier"`
	IPAddress      *string         `json:"ip_address"`
	CreatedAt      string          `json:"created_at"`
	URL            *string         `json:"url"`
	Source         *string         `json:"source"`
	Metadata       json.RawMessage `json:"metadata"`
}

// AuditLogListOptions are the optional filters for ListAuditLogs. Zero values
// are omitted from the request.
type AuditLogListOptions struct {
	Types     []string
	UserIDs   []string
	StartDate string
	EndDate   string
	Limit     int
	Offset    int
}

// CostDimension is a group-by dimension the cost explorer supports.
type CostDimension struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

// CostFilterField is a filter field the cost explorer supports.
type CostFilterField struct {
	Field    string `json:"field"`
	Op       string `json:"op"`
	Multiple bool   `json:"multiple"`
}

// CostExplorerSchema lists the dimensions, filter fields, and time buckets the cost explorer supports.
type CostExplorerSchema struct {
	Platform           string            `json:"platform"`
	AvailablePlatforms []string          `json:"available_platforms"`
	Dimensions         []CostDimension   `json:"dimensions"`
	Filters            []CostFilterField `json:"filters"`
	TimeDimensions     []string          `json:"time_dimensions"`
}

// CostFilter is a single {field, op, value} cost explorer filter.
type CostFilter struct {
	Field string `json:"field"`
	Op    string `json:"op"`
	Value any    `json:"value"`
}

// CostExplorerRequest is the body sent to the cost explorer endpoint.
type CostExplorerRequest struct {
	StartDate     string       `json:"start_date"`
	EndDate       string       `json:"end_date"`
	Platform      string       `json:"platform,omitempty"`
	Dimension     string       `json:"dimension,omitempty"`
	TimeDimension string       `json:"time_dimension,omitempty"`
	Filters       []CostFilter `json:"filters,omitempty"`
	Limit         int          `json:"limit,omitempty"`
	Offset        int          `json:"offset,omitempty"`
}

// CostExplorerResponse is a page of cost breakdown rows.
type CostExplorerResponse struct {
	Platform      string           `json:"platform"`
	StartDate     string           `json:"start_date"`
	EndDate       string           `json:"end_date"`
	Dimension     *string          `json:"dimension"`
	TimeDimension *string          `json:"time_dimension"`
	TotalRows     int              `json:"total_rows"`
	ReturnedRows  int              `json:"returned_rows"`
	Offset        int              `json:"offset"`
	Truncated     bool             `json:"truncated"`
	NextOffset    *int             `json:"next_offset,omitempty"`
	Rows          []map[string]any `json:"rows"`
}
